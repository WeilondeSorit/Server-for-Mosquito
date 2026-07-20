# Используем образ с SDK для сборки
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Копируем файл проекта и восстанавливаем зависимости
COPY ChatServer.csproj .
RUN dotnet restore

# Копируем весь код
COPY . .

# Собираем приложение
RUN dotnet publish ChatServer.csproj -c Release -o /app/publish

# Финальный образ
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app/publish .

EXPOSE 8080

# Запускаем приложение
ENTRYPOINT ["dotnet", "ChatServer.dll"]