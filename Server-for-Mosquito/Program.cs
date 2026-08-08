using System;
using System.Collections.Concurrent;
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

var connectionString = Environment.GetEnvironmentVariable("DATABASE_URL")
    ?? "Host=postgr;Port=5432;Database=main_db;Username=main_usesr;Password=MainTestAppPass";

var clients = new ConcurrentDictionary<string, WebSocket>();
var callSessions = new ConcurrentDictionary<string, CallSession>();

app.Run(async (context) =>
{
    if (context.WebSockets.IsWebSocketRequest)
    {
        var webSocket = await context.WebSockets.AcceptWebSocketAsync();
        await HandleWebSocket(context, webSocket);
    }
    else
    {
        context.Response.StatusCode = 400;
        await context.Response.WriteAsync("WebSocket expected");
    }
});

async Task HandleWebSocket(HttpContext context, WebSocket webSocket)
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
    Console.WriteLine($"Error registering user: {ex.GetType().Name} - {ex.Message}");
    Console.WriteLine(ex.StackTrace);
    // вместо закрытия сокета можно отправить сообщение об ошибке клиенту
    await SendTo(webSocket, new { error = "DB error: " + ex.Message });
    await webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError, "DB error", CancellationToken.None);
}

    clients[username] = webSocket;

    var buffer = new byte[1024 * 4];
    try
    {
        while (webSocket.State == WebSocketState.Open)
        {
            var result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
            if (result.MessageType == WebSocketMessageType.Text)
            {
                var messageJson = Encoding.UTF8.GetString(buffer, 0, result.Count);
                await ProcessMessage(messageJson, username);
            }
            else if (result.MessageType == WebSocketMessageType.Close)
            {
                await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closed by client", CancellationToken.None);
            }
        }
    }
    finally
    {
        clients.TryRemove(username, out _);
        // Очистка звонков при отключении
        foreach (var key in callSessions.Keys)
        {
            if (key.StartsWith(username + ":") || key.EndsWith(":" + username))
            {
                if (callSessions.TryRemove(key, out var session))
                {
                    var other = session.User1 == username ? session.User2 : session.User1;
                    if (clients.TryGetValue(other, out var otherSocket) && otherSocket.State == WebSocketState.Open)
                    {
                        await SendTo(otherSocket, new { type = "call_ended", reason = "user_disconnected" });
                    }
                }
            }
        }
    }
}

async Task ProcessMessage(string messageJson, string currentUser)
{
    try
    {
        using var doc = JsonDocument.Parse(messageJson);
        var root = doc.RootElement;
        string type = root.TryGetProperty("type", out var typeProp) ? typeProp.GetString() : "chat";

        switch (type)
        {
            case "get_conversations":
                await SendConversations(currentUser);
                break;

            case "get_messages":
                if (root.TryGetProperty("with", out var withProp))
                    await SendMessageHistory(currentUser, withProp.GetString());
                break;

            case "call_join":
                if (root.TryGetProperty("to", out var toJoin))
                    await HandleCallJoin(currentUser, toJoin.GetString());
                break;

            case "call_leave":
                if (root.TryGetProperty("to", out var toLeave))
                    await HandleCallLeave(currentUser, toLeave.GetString());
                break;

            case "call_offer":
            case "call_answer":
            case "ice_candidate":
                if (root.TryGetProperty("to", out var toSignal))
                    await ForwardCallSignal(currentUser, toSignal.GetString(), type, messageJson);
                break;

            default:
                if (root.TryGetProperty("to", out var toMsg) &&
                    root.TryGetProperty("from", out var fromMsg) &&
                    root.TryGetProperty("text", out var textProp))
                {
                    string toUser = toMsg.GetString();
                    string fromUser = fromMsg.GetString();
                    string text = textProp.GetString();
                    long timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                    if (fromUser == toUser) return;

                    if (!await SaveMessage(fromUser, toUser, text, timestamp))
                    {
                        await SendError(currentUser, "Failed to save message (recipient not found)");
                        return;
                    }

                    var outMsg = new { from = fromUser, to = toUser, text, timestamp };

                    if (clients.TryGetValue(toUser, out var receiverSocket) && receiverSocket.State == WebSocketState.Open)
                    {
                        await SendTo(receiverSocket, outMsg);
                    }

                    if (clients.TryGetValue(currentUser, out var senderSocket) && senderSocket.State == WebSocketState.Open)
                    {
                        await SendTo(senderSocket, outMsg);
                    }
                }
                break;
        }
    }
    catch (JsonException) { /* игнорируем */ }
    catch (Exception ex)
    {
        Console.WriteLine($"Error processing message: {ex.Message}");
        await SendError(currentUser, "Internal server error");
    }
}

// ===== Логика звонков =====
async Task HandleCallJoin(string user, string target)
{
    if (user == target) return;

    string sessionKey = GetOrderedPair(user, target);
    var session = callSessions.GetOrAdd(sessionKey, _ => new CallSession { User1 = user, User2 = target });

    lock (session)
    {
        if (session.User1 == user && !session.User1Joined)
        {
            session.User1Joined = true;
            _ = SendTo(clients[user], new { type = "call_waiting", message = "Ожидание подключения собеседника..." });
        }
        else if (session.User2 == user && !session.User2Joined)
        {
            session.User2Joined = true;
            _ = SendTo(clients[user], new { type = "call_waiting", message = "Ожидание подключения собеседника..." });
        }

        if (session.User1Joined && session.User2Joined)
        {
            _ = SendTo(clients[session.User1], new { type = "call_started", with = session.User2 });
            _ = SendTo(clients[session.User2], new { type = "call_started", with = session.User1 });
        }
    }
}

async Task HandleCallLeave(string user, string target)
{
    string sessionKey = GetOrderedPair(user, target);
    if (callSessions.TryGetValue(sessionKey, out var session))
    {
        lock (session)
        {
            var other = session.User1 == user ? session.User2 : session.User1;
            if (clients.TryGetValue(other, out var otherSocket) && otherSocket.State == WebSocketState.Open)
            {
                _ = SendTo(otherSocket, new { type = "call_ended", reason = "user_left" });
            }
        }
        callSessions.TryRemove(sessionKey, out _);
    }
}

async Task ForwardCallSignal(string from, string to, string signalType, string originalJson)
{
    if (clients.TryGetValue(to, out var socket) && socket.State == WebSocketState.Open)
    {
        var doc = JsonDocument.Parse(originalJson);
        var newMsg = new Dictionary<string, object>();
        foreach (var prop in doc.RootElement.EnumerateObject())
        {
            if (prop.Name == "from") continue;
            newMsg[prop.Name] = prop.Value;
        }
        newMsg["from"] = from;
        await SendTo(socket, newMsg);
    }
}

string GetOrderedPair(string u1, string u2) =>
    string.Compare(u1, u2, StringComparison.Ordinal) < 0 ? $"{u1}:{u2}" : $"{u2}:{u1}";

async Task SendTo(WebSocket socket, object data)
{
    var json = JsonSerializer.Serialize(data);
    var bytes = Encoding.UTF8.GetBytes(json);
    await socket.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, CancellationToken.None);
}

// ===== БД (без изменений) =====
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

    int fromId, toId;
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
        // 1. Сохраняем сообщение
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

        // 2. Обновляем диалог – гарантируем, что user1_id < user2_id
        int user1Id = Math.Min(fromId, toId);
        int user2Id = Math.Max(fromId, toId);

        await using (var cmdConv = new NpgsqlCommand(
            "INSERT INTO conversations (user1_id, user2_id, last_message_text, last_message_timestamp) " +
            "VALUES (@id1, @id2, @text, to_timestamp(@timestamp)) " +
            "ON CONFLICT (user1_id, user2_id) DO UPDATE SET " +
            "    last_message_text = EXCLUDED.last_message_text, " +
            "    last_message_timestamp = EXCLUDED.last_message_timestamp",
            conn, transaction))
        {
            cmdConv.Parameters.AddWithValue("id1", user1Id);
            cmdConv.Parameters.AddWithValue("id2", user2Id);
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
async Task<(int, int)> GetUserIds(NpgsqlConnection conn, string user1, string user2)
{
    await using var cmd = new NpgsqlCommand(
        "SELECT id, username FROM users WHERE username = @u1 OR username = @u2", conn);
    cmd.Parameters.AddWithValue("u1", user1);
    cmd.Parameters.AddWithValue("u2", user2);
    var dict = new Dictionary<string, int>();
    await using var reader = await cmd.ExecuteReaderAsync();
    while (await reader.ReadAsync()) dict[reader.GetString(1)] = reader.GetInt32(0);
    await reader.CloseAsync();
    dict.TryGetValue(user1, out int id1);
    dict.TryGetValue(user2, out int id2);
    return (id1, id2);
}

async Task SendConversations(string currentUser)
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
        ORDER BY last_message_timestamp DESC", conn);
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
    await SendTo(clients[currentUser], new { type = "conversations", list = conversations });
}

async Task SendMessageHistory(string currentUser, string otherUser)
{
    await using var conn = new NpgsqlConnection(connectionString);
    await conn.OpenAsync();
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
        @"SELECT sender_id, receiver_id, text, EXTRACT(epoch FROM timestamp)::bigint AS timestamp
          FROM messages
          WHERE (sender_id = @cId AND receiver_id = @oId)
             OR (sender_id = @oId AND receiver_id = @cId)
          ORDER BY timestamp DESC LIMIT 50", conn);
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
    messages.Reverse();
    await SendTo(clients[currentUser], new { type = "messages", with = otherUser, list = messages });
}

async Task SendError(string user, string message)
{
    if (clients.TryGetValue(user, out var socket) && socket.State == WebSocketState.Open)
    {
        await SendTo(socket, new { error = message });
    }
}



app.Run();
class CallSession
{
    public string User1 { get; set; }
    public string User2 { get; set; }
    public bool User1Joined { get; set; }
    public bool User2Joined { get; set; }
}