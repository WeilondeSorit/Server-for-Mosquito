-- init.sql для PostgreSQL
-- Создаёт таблицы, необходимые для работы приложения чата

-- Таблица пользователей
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Таблица сообщений
CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY,
    sender_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    receiver_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    text TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL   -- имя столбца совпадает с кодом
);

-- Таблица диалогов (последнее сообщение для каждой пары)
CREATE TABLE IF NOT EXISTS conversations (
    user1_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    user2_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    last_message_text TEXT NOT NULL,
    last_message_timestamp TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (user1_id, user2_id),
    CONSTRAINT check_user1_less_than_user2 CHECK (user1_id < user2_id)
);

-- Индексы для ускорения запросов
CREATE INDEX idx_messages_sender_receiver_timestamp ON messages(sender_id, receiver_id, timestamp DESC);
CREATE INDEX idx_messages_receiver_sender_timestamp ON messages(receiver_id, sender_id, timestamp DESC);
CREATE INDEX idx_conversations_user1 ON conversations(user1_id);
CREATE INDEX idx_conversations_user2 ON conversations(user2_id);