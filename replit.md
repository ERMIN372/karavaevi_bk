# Telegram Bot - Караваевы

## Обзор проекта
Telegram бот для управления подработками в сети "Братья Караваевы". Бот позволяет директорам публиковать заявки на смены, а сотрудникам откликаться на них.

## Технологии
- Python 3.11
- aiogram 2.25.1 (Telegram Bot Framework)
- Google Sheets для хранения данных
- Webhook режим для деплоя на Reserved VM

## Последние изменения (07.10.2025)
- Конвертирован с polling на webhook режим
- Настроен для деплоя на Replit Reserved VM
- Добавлены переменные окружения для webhook
- Создан requirements.txt с зависимостями
- Настроен workflow и deployment конфигурация

## Структура проекта
- `main.py` - основной файл бота с обработчиками
- `storage.py` - модуль для работы с Google Sheets
- `requirements.txt` - зависимости Python
- `.env.example` - пример переменных окружения

## Переменные окружения (Secrets)
Необходимо добавить в Replit Secrets:
- `BOT_TOKEN` - токен Telegram бота
- `CHANNEL_ID` - ID канала для публикаций
- `TECH_CHAT_ID` - ID чата для технических уведомлений
- `ADMINS` - список ID директоров через запятую
- `TIMEZONE` - часовой пояс (по умолчанию Europe/Moscow)
- `RATE_LIMIT_PER_DAY` - лимит заявок в день (по умолчанию 3)
- `GOOGLE_SERVICE_ACCOUNT_JSON_BASE64` - Base64 JSON сервисного аккаунта Google
- `GOOGLE_SPREADSHEET_ID` - ID таблицы Google Sheets
- `WEBHOOK_URL` - URL для webhook (например, https://your-app.replit.app)

## Deployment на Reserved VM
Проект настроен для деплоя на Replit Reserved VM с webhook:

### Шаг 1: Добавьте секреты
В Replit Secrets добавьте следующие переменные:
- `BOT_TOKEN` - токен от @BotFather
- `GOOGLE_SERVICE_ACCOUNT_JSON_BASE64` - Base64 JSON сервисного аккаунта Google
- `GOOGLE_SPREADSHEET_ID` - ID таблицы Google Sheets
- `CHANNEL_ID` - ID канала для публикаций
- `TECH_CHAT_ID` - ID чата для технических уведомлений
- `ADMINS` - список ID директоров через запятую
- `WEBHOOK_URL` - будет автоматически установлен как URL вашего деплоя (например, `https://your-app.replit.app`)

### Шаг 2: Настройте Google Sheets
1. Создайте Google Sheets таблицу
2. Создайте Service Account в Google Cloud Console
3. Дайте доступ Service Account к таблице
4. Закодируйте JSON ключ в Base64 и добавьте в GOOGLE_SERVICE_ACCOUNT_JSON_BASE64

### Шаг 3: Опубликуйте
1. Нажмите "Publish" в Replit
2. Выберите "Reserved VM" как тип деплоя
3. После публикации, скопируйте URL деплоя и добавьте его в секрет `WEBHOOK_URL`
4. Перезапустите деплой

## Режимы работы
- **Webhook** (production): используется при наличии WEBHOOK_URL для Reserved VM
- **Polling** (development): используется если WEBHOOK_URL не установлен (для локальной разработки)
