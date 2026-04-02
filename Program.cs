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

app.UseWebSockets();

// Строка подключения к PostgreSQL (можно задать через переменную окружения)
var connectionString = Environment.GetEnvironmentVariable("DATABASE_URL")
    ?? "Host=localhost;Port=5432;Database=main_db;Username=main_usesr;Password=MainTestAppPass";

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

    // Регистрация/обновление пользователя в БД
    await EnsureUserExists(username);

    // Добавление в словарь активных клиентов
    lock (clients)
    {
        if (clients.ContainsKey(username))
        {
            clients[username].Abort();
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
        var doc = JsonDocument.Parse(messageJson);
        var root = doc.RootElement;

        // Определяем тип сообщения
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

            default: // "chat" или отсутствие type
                // Обычное сообщение
                if (root.TryGetProperty("to", out var toProp) &&
                    root.TryGetProperty("from", out var fromProp) &&
                    root.TryGetProperty("text", out var textProp))
                {
                    string toUser = toProp.GetString();
                    string fromUser = fromProp.GetString();
                    string text = textProp.GetString();
                    long timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                    // Сохраняем в БД
                    await SaveMessage(fromUser, toUser, text, timestamp);

                    // Пересылаем, если получатель онлайн
                    if (clients.TryGetValue(toUser, out var receiverSocket) && receiverSocket.State == WebSocketState.Open)
                    {
                        var outMsg = new
                        {
                            from = fromUser,
                            to = toUser,
                            text = text,
                            timestamp = timestamp
                        };
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
        // Игнорируем некорректные сообщения
    }
}

// --- Работа с базой данных ---

async Task EnsureUserExists(string username)
{
    await using var conn = new NpgsqlConnection(connectionString);
    await conn.OpenAsync();

    // Проверяем существование пользователя
    await using var cmd = new NpgsqlCommand(
        "INSERT INTO users (username, last_seen) VALUES (@username, CURRENT_TIMESTAMP) " +
        "ON CONFLICT (username) DO UPDATE SET last_seen = CURRENT_TIMESTAMP",
        conn);
    cmd.Parameters.AddWithValue("username", username);
    await cmd.ExecuteNonQueryAsync();
}

async Task SaveMessage(string fromUser, string toUser, string text, long timestamp)
{
    await using var conn = new NpgsqlConnection(connectionString);
    await conn.OpenAsync();

    // Вставляем сообщение
    await using var cmdMsg = new NpgsqlCommand(
        "INSERT INTO messages (sender_id, receiver_id, text, timestamp) " +
        "VALUES ((SELECT id FROM users WHERE username = @from), " +
        "        (SELECT id FROM users WHERE username = @to), @text, to_timestamp(@timestamp))",
        conn);
    cmdMsg.Parameters.AddWithValue("from", fromUser);
    cmdMsg.Parameters.AddWithValue("to", toUser);
    cmdMsg.Parameters.AddWithValue("text", text);
    cmdMsg.Parameters.AddWithValue("timestamp", timestamp);
    await cmdMsg.ExecuteNonQueryAsync();

    // Обновляем диалог (последнее сообщение)
    await using var cmdConv = new NpgsqlCommand(
        "INSERT INTO conversations (user1_id, user2_id, last_message_text, last_message_timestamp) " +
        "VALUES ((SELECT id FROM users WHERE username = @user1), " +
        "        (SELECT id FROM users WHERE username = @user2), @text, to_timestamp(@timestamp)) " +
        "ON CONFLICT (user1_id, user2_id) DO UPDATE SET " +
        "    last_message_text = EXCLUDED.last_message_text, " +
        "    last_message_timestamp = EXCLUDED.last_message_timestamp",
        conn);
    // Обеспечиваем уникальную пару: user1_id < user2_id
    string user1 = string.Compare(fromUser, toUser, StringComparison.Ordinal) < 0 ? fromUser : toUser;
    string user2 = string.Compare(fromUser, toUser, StringComparison.Ordinal) < 0 ? toUser : fromUser;
    cmdConv.Parameters.AddWithValue("user1", user1);
    cmdConv.Parameters.AddWithValue("user2", user2);
    cmdConv.Parameters.AddWithValue("text", text);
    cmdConv.Parameters.AddWithValue("timestamp", timestamp);
    await cmdConv.ExecuteNonQueryAsync();
}

async Task SendConversations(WebSocket webSocket, string currentUser)
{
    await using var conn = new NpgsqlConnection(connectionString);
    await conn.OpenAsync();

    // Получаем список диалогов для currentUser
    await using var cmd = new NpgsqlCommand(
        @"SELECT 
            CASE 
                WHEN user1_id = (SELECT id FROM users WHERE username = @current) THEN 
                    (SELECT username FROM users WHERE id = user2_id)
                ELSE 
                    (SELECT username FROM users WHERE id = user1_id)
            END AS other_user,
            last_message_text,
            EXTRACT(epoch FROM last_message_timestamp)::bigint AS last_message_timestamp
        FROM conversations
        WHERE user1_id = (SELECT id FROM users WHERE username = @current) 
           OR user2_id = (SELECT id FROM users WHERE username = @current)
        ORDER BY last_message_timestamp DESC",
        conn);
    cmd.Parameters.AddWithValue("current", currentUser);

    var conversations = new List<object>();
    await using var reader = await cmd.ExecuteReaderAsync();
    while (await reader.ReadAsync())
    {
        conversations.Add(new
        {
            with = reader.GetString(0),
            last_message = reader.GetString(1),
            timestamp = reader.GetInt64(2)
        });
    }

    var response = new { type = "conversations", list = conversations };
    var json = JsonSerializer.Serialize(response);
    var bytes = Encoding.UTF8.GetBytes(json);
    await webSocket.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, CancellationToken.None);
}

async Task SendMessageHistory(WebSocket webSocket, string currentUser, string otherUser)
{
    await using var conn = new NpgsqlConnection(connectionString);
    await conn.OpenAsync();

    // Получаем последние 50 сообщений между двумя пользователями
    await using var cmd = new NpgsqlCommand(
        @"SELECT 
            sender_id,
            receiver_id,
            text,
            EXTRACT(epoch FROM timestamp)::bigint AS timestamp
        FROM messages
        WHERE (sender_id = (SELECT id FROM users WHERE username = @user1) AND receiver_id = (SELECT id FROM users WHERE username = @user2))
           OR (sender_id = (SELECT id FROM users WHERE username = @user2) AND receiver_id = (SELECT id FROM users WHERE username = @user1))
        ORDER BY timestamp DESC
        LIMIT 50",
        conn);
    cmd.Parameters.AddWithValue("user1", currentUser);
    cmd.Parameters.AddWithValue("user2", otherUser);

    var messages = new List<object>();
    await using var reader = await cmd.ExecuteReaderAsync();
    while (await reader.ReadAsync())
    {
        int senderId = reader.GetInt32(0);
        int receiverId = reader.GetInt32(1);
        string text = reader.GetString(2);
        long timestamp = reader.GetInt64(3);

        // Определяем, кто отправитель (по id, потом получим username)
        string fromUser;
        if (senderId == (await GetUserId(currentUser)))
            fromUser = currentUser;
        else
            fromUser = otherUser;

        messages.Add(new
        {
            from = fromUser,
            to = fromUser == currentUser ? otherUser : currentUser,
            text = text,
            timestamp = timestamp
        });
    }

    // Возвращаем в обратном порядке (от старых к новым)
    messages.Reverse();
    var response = new { type = "messages", with = otherUser, list = messages };
    var json = JsonSerializer.Serialize(response);
    var bytes = Encoding.UTF8.GetBytes(json);
    await webSocket.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, CancellationToken.None);
}

async Task<int> GetUserId(string username)
{
    await using var conn = new NpgsqlConnection(connectionString);
    await conn.OpenAsync();
    await using var cmd = new NpgsqlCommand("SELECT id FROM users WHERE username = @username", conn);
    cmd.Parameters.AddWithValue("username", username);
    var result = await cmd.ExecuteScalarAsync();
    return result != null ? (int)result : 0;
}

app.Run();