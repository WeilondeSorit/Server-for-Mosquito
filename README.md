
# Server-for-Mosquito

WebSocket chat server using ASP.NET Core 8 + PostgreSQL.

## Structure

- `Program.cs` – main logic
- `ChatServer.csproj` – .NET 8 project
- `Dockerfile`
- `appsettings.json`

## Database

Run these tables in PostgreSQL:

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) UNIQUE NOT NULL,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE messages (
    id SERIAL PRIMARY KEY,
    sender_id INT REFERENCES users(id),
    receiver_id INT REFERENCES users(id),
    text TEXT NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE conversations (
    user1_id INT, user2_id INT,
    last_message_text TEXT NOT NULL,
    last_message_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user1_id, user2_id), CHECK (user1_id < user2_id)
);
```

## Environment

`DATABASE_URL` – PostgreSQL connection string.  
Default: `Host=postgres;Port=5432;Database=main_db;Username=main_usesr;Password=MainTestAppPass`

## Run

### Local

```bash
dotnet restore
dotnet run
```

### Docker

```bash
docker build -t mosquito-chat-server .
docker run -d -p 8090:8090 -e DATABASE_URL="..." mosquito-chat-server
```

## Connect

WebSocket URL: `ws://localhost:5000/?username=YourName`

### JSON messages

- `chat` – `{"type":"chat","from":"A","to":"B","text":"Hi"}`
- `get_conversations` – `{"type":"get_conversations"}`
- `get_messages` – `{"type":"get_messages","with":"B"}`
