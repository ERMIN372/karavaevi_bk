# Караваевы бот (Google Sheets)

Прототип телеграм-бота подработок сети «Братья Караваевы», переписанный на
Python 3.11 с хранением данных в Google Sheets.

## Требования

- Python 3.11
- aiogram 2.25.1
- aiohttp < 3.9
- gspread
- google-auth
- python-dotenv

## Переменные окружения

| Переменная | Описание |
| --- | --- |
| `BOT_TOKEN` | Токен Telegram-бота |
| `CHANNEL_ID` | ID канала для публикаций |
| `TECH_CHAT_ID` | ID тех. чата для уведомлений об ошибках |
| `ADMINS` | Список ID директоров через запятую |
| `TIMEZONE` | Часовой пояс для валидации дат (например, `Europe/Moscow`) |
| `GOOGLE_SERVICE_ACCOUNT_JSON_BASE64` | Base64 JSON сервисного аккаунта Google |
| `GOOGLE_SPREADSHEET_ID` | ID таблицы Google Sheets |

## Запуск

1. Установите зависимости `pip install -r requirements.txt` (создайте файл при
   необходимости).
2. Создайте `.env` с перечисленными переменными.
3. Запустите бота: `python main.py`.

Таблица должна содержать листы `Requests`, `Users` и `Shops` — при запуске бот
создаст их и заголовки автоматически. Лист `Shops` используется для выдачи
справочника лавок в меню.
