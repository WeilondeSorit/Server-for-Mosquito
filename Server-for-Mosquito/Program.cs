using System;
using System.Collections.Generic;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

app.UseWebSockets(new WebSocketOptions
{
    KeepAliveInterval = TimeSpan.FromSeconds(30)
});

// Строка подключения (без вывода в лог)
var connectionString = Environment.GetEnvironmentVariable("DATABASE_URL")
    ?? "Host=postgr;Port=5432;Database=main_db;Username=main_usesr;Password=MainTestAppPass";

// Хранилище активных WebSocket-соединений
var clients = new Dictionary<string, WebSocket>();

app.Run(async (context) =>
{
    if (context.WebSockets.IsWebSocketRequest)
    {
        var webSocket = await context.WebSockets.AcceptWebSocketAsync();
        await HandleWebSocket(context, webSocket, clients);
    }
    else
    {
        context.Response.StatusCode = 400;
        await context.Response.WriteAsync("WebSocket expected");
    }
});

async Task HandleWebSocket(HttpContext context, WebSocket webSocket, Dictionary<string, WebSocket> clients)
{
    var username = context.Request.Query["username"].ToString();
    if (string.IsNullOrEmpty(username))
    {
        await webSocket.CloseAsync(WebSocketCloseStatus.PolicyViolation, "Username required", CancellationToken.None);
        return;
    }

    try
    {
        await EnsureUserExists(username);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error registering user: {ex.Message}");
        await webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError, ex.Message, CancellationToken.None);
        return;
    }

    lock (clients)
    {
        if (clients.TryGetValue(username, out var oldSocket))
        {
            oldSocket.Abort();
            clients.Remove(username);
        }
        clients[username] = webSocket;
    }

    var buffer = new byte[1024 * 4];
    try
    {
        while (webSocket.State == WebSocketState.Open)
        {
            var result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
            if (result.MessageType == WebSocketMessageType.Text)
            {
                var messageJson = Encoding.UTF8.GetString(buffer, 0, result.Count);
                await ProcessMessage(messageJson, clients, webSocket, username);
            }
            else if (result.MessageType == WebSocketMessageType.Close)
            {
                await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closed by client", CancellationToken.None);
            }
        }
    }
    finally
    {
        lock (clients)
        {
            clients.Remove(username);
        }
    }
}

async Task ProcessMessage(string messageJson, Dictionary<string, WebSocket> clients, WebSocket sender, string currentUser)
{
    try
    {
        using var doc = JsonDocument.Parse(messageJson);
        var root = doc.RootElement;

        string type = root.TryGetProperty("type", out var typeProp) ? typeProp.GetString() : "chat";

        switch (type)
        {
            case "get_conversations":
                await SendConversations(sender, currentUser);
                break;

            case "get_messages":
                if (root.TryGetProperty("with", out var withProp))
                {
                    string otherUser = withProp.GetString();
                    await SendMessageHistory(sender, currentUser, otherUser);
                }
                break;

            default: // "chat" или без type
                if (root.TryGetProperty("to", out var toProp) &&
                    root.TryGetProperty("from", out var fromProp) &&
                    root.TryGetProperty("text", out var textProp))
                {
                    string toUser = toProp.GetString();
                    string fromUser = fromProp.GetString();
                    string text = textProp.GetString();
                    long timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                    if (fromUser == toUser)
                    {
                        await SendError(sender, "Cannot send message to yourself");
                        return;
                    }

                    if (!await SaveMessage(fromUser, toUser, text, timestamp))
                    {
                        await SendError(sender, "Failed to save message (recipient not found)");
                        return;
                    }

                    if (clients.TryGetValue(toUser, out var receiverSocket) && receiverSocket.State == WebSocketState.Open)
                    {
                        var outMsg = new { from = fromUser, to = toUser, text, timestamp };
                        var outJson = JsonSerializer.Serialize(outMsg);
                        var bytes = Encoding.UTF8.GetBytes(outJson);
                        await receiverSocket.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, CancellationToken.None);
                    }
                }
                break;
        }
    }
    catch (JsonException)
    {
        // игнорируем
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error processing message: {ex.Message}");
        await SendError(sender, "Internal server error");
    }
}

async Task SendError(WebSocket socket, string message)
{
    if (socket.State == WebSocketState.Open)
    {
        var error = new { error = message };
        var json = JsonSerializer.Serialize(error);
        var bytes = Encoding.UTF8.GetBytes(json);
        await socket.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, CancellationToken.None);
    }
}

// --- Работа с БД ---

async Task EnsureUserExists(string username)
{
    await using var conn = new NpgsqlConnection(connectionString);
    await conn.OpenAsync();

    await using var cmd = new NpgsqlCommand(
        "INSERT INTO users (username, last_seen) VALUES (@username, CURRENT_TIMESTAMP) " +
        "ON CONFLICT (username) DO UPDATE SET last_seen = CURRENT_TIMESTAMP",
        conn);
    cmd.Parameters.AddWithValue("username", username);
    await cmd.ExecuteNonQueryAsync();
}

async Task<bool> SaveMessage(string fromUser, string toUser, string text, long timestamp)
{
    if (fromUser == toUser) return false;

    await using var conn = new NpgsqlConnection(connectionString);
    await conn.OpenAsync();

    // Получаем ID обоих пользователей (если кого-то нет — создаём)
    int fromId, toId;

    // Первая попытка
    (fromId, toId) = await GetUserIds(conn, fromUser, toUser);
    if (fromId == 0 || toId == 0)
    {
        await EnsureUserExists(fromUser);
        await EnsureUserExists(toUser);
        (fromId, toId) = await GetUserIds(conn, fromUser, toUser);
        if (fromId == 0 || toId == 0) return false;
    }

    await using var transaction = await conn.BeginTransactionAsync();

    try
    {
        // Вставка сообщения
        await using (var cmdMsg = new NpgsqlCommand(
            "INSERT INTO messages (sender_id, receiver_id, text, timestamp) VALUES (@fromId, @toId, @text, to_timestamp(@timestamp))",
            conn, transaction))
        {
            cmdMsg.Parameters.AddWithValue("fromId", fromId);
            cmdMsg.Parameters.AddWithValue("toId", toId);
            cmdMsg.Parameters.AddWithValue("text", text);
            cmdMsg.Parameters.AddWithValue("timestamp", timestamp);
            await cmdMsg.ExecuteNonQueryAsync();
        }

        // Определяем упорядоченную пару для conversations
        string user1, user2;
        if (string.Compare(fromUser, toUser, StringComparison.Ordinal) < 0)
        {
            user1 = fromUser; user2 = toUser;
        }
        else
        {
            user1 = toUser; user2 = fromUser;
        }

        // Получаем ID для упорядоченной пары
        int id1, id2;
        (id1, id2) = await GetUserIds(conn, user1, user2);
        if (id1 == 0 || id2 == 0) return false;

        // Обновляем диалог
        await using (var cmdConv = new NpgsqlCommand(
            "INSERT INTO conversations (user1_id, user2_id, last_message_text, last_message_timestamp) " +
            "VALUES (@id1, @id2, @text, to_timestamp(@timestamp)) " +
            "ON CONFLICT (user1_id, user2_id) DO UPDATE SET " +
            "    last_message_text = EXCLUDED.last_message_text, " +
            "    last_message_timestamp = EXCLUDED.last_message_timestamp",
            conn, transaction))
        {
            cmdConv.Parameters.AddWithValue("id1", id1);
            cmdConv.Parameters.AddWithValue("id2", id2);
            cmdConv.Parameters.AddWithValue("text", text);
            cmdConv.Parameters.AddWithValue("timestamp", timestamp);
            await cmdConv.ExecuteNonQueryAsync();
        }

        await transaction.CommitAsync();
        return true;
    }
    catch
    {
        await transaction.RollbackAsync();
        throw;
    }
}

// Вспомогательный метод: возвращает (id1, id2) для двух имён, либо (0,0) если кто-то отсутствует
async Task<(int, int)> GetUserIds(NpgsqlConnection conn, string user1, string user2)
{
    await using var cmd = new NpgsqlCommand(
        "SELECT id, username FROM users WHERE username = @u1 OR username = @u2",
        conn);
    cmd.Parameters.AddWithValue("u1", user1);
    cmd.Parameters.AddWithValue("u2", user2);

    var dict = new Dictionary<string, int>();
    await using var idReader = await cmd.ExecuteReaderAsync();
    while (await idReader.ReadAsync())
    {
        dict[idReader.GetString(1)] = idReader.GetInt32(0);
    }
    await idReader.CloseAsync();

    dict.TryGetValue(user1, out int id1);
    dict.TryGetValue(user2, out int id2);
    return (id1, id2);
}

async Task SendConversations(WebSocket webSocket, string currentUser)
{
    await using var conn = new NpgsqlConnection(connectionString);
    await conn.OpenAsync();

    int currentId;
    await using (var cmd = new NpgsqlCommand("SELECT id FROM users WHERE username = @current", conn))
    {
        cmd.Parameters.AddWithValue("current", currentUser);
        var res = await cmd.ExecuteScalarAsync();
        if (res == null) return;
        currentId = (int)res;
    }

    await using var cmdConv = new NpgsqlCommand(
        @"SELECT 
            CASE 
                WHEN user1_id = @currentId THEN (SELECT username FROM users WHERE id = user2_id)
                ELSE (SELECT username FROM users WHERE id = user1_id)
            END AS other_user,
            last_message_text,
            EXTRACT(epoch FROM last_message_timestamp)::bigint AS last_message_timestamp
        FROM conversations
        WHERE user1_id = @currentId OR user2_id = @currentId
        ORDER BY last_message_timestamp DESC",
        conn);
    cmdConv.Parameters.AddWithValue("currentId", currentId);

    var conversations = new List<object>();
    await using var convReader = await cmdConv.ExecuteReaderAsync();
    while (await convReader.ReadAsync())
    {
        conversations.Add(new
        {
            with = convReader.GetString(0),
            last_message = convReader.GetString(1),
            timestamp = convReader.GetInt64(2)
        });
    }
    await convReader.CloseAsync();

    var response = new { type = "conversations", list = conversations };
    var json = JsonSerializer.Serialize(response);
    var bytes = Encoding.UTF8.GetBytes(json);
    await webSocket.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, CancellationToken.None);
}

async Task SendMessageHistory(WebSocket webSocket, string currentUser, string otherUser)
{
    await using var conn = new NpgsqlConnection(connectionString);
    await conn.OpenAsync();

    // Получаем ID обоих
    int currentId, otherId;
    (currentId, otherId) = await GetUserIds(conn, currentUser, otherUser);
    if (currentId == 0 || otherId == 0)
    {
        await EnsureUserExists(currentUser);
        await EnsureUserExists(otherUser);
        (currentId, otherId) = await GetUserIds(conn, currentUser, otherUser);
        if (currentId == 0 || otherId == 0) return;
    }

    await using var cmdMsg = new NpgsqlCommand(
        @"SELECT 
            sender_id,
            receiver_id,
            text,
            EXTRACT(epoch FROM timestamp)::bigint AS timestamp
        FROM messages
        WHERE (sender_id = @cId AND receiver_id = @oId)
           OR (sender_id = @oId AND receiver_id = @cId)
        ORDER BY timestamp DESC
        LIMIT 50",
        conn);
    cmdMsg.Parameters.AddWithValue("cId", currentId);
    cmdMsg.Parameters.AddWithValue("oId", otherId);

    var messages = new List<object>();
    await using var msgReader = await cmdMsg.ExecuteReaderAsync();
    while (await msgReader.ReadAsync())
    {
        int senderId = msgReader.GetInt32(0);
        int receiverId = msgReader.GetInt32(1);
        string text = msgReader.GetString(2);
        long timestamp = msgReader.GetInt64(3);

        string from = senderId == currentId ? currentUser : otherUser;
        string to = senderId == currentId ? otherUser : currentUser;

        messages.Add(new { from, to, text, timestamp });
    }
    await msgReader.CloseAsync();

    messages.Reverse(); // от старых к новым
    var response = new { type = "messages", with = otherUser, list = messages };
    var json = JsonSerializer.Serialize(response);
    var bytes = Encoding.UTF8.GetBytes(json);
    await webSocket.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, CancellationToken.None);
}

app.Run();