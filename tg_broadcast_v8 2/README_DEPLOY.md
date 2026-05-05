# tg-broadcast — Web UI + Telegram bot (FastAPI)

Проект: веб‑панель рассылки в Telegram + хранение данных в SQLite (app/app.db) + бот на aiogram v3.

## Настройки (.env)
Скопируй пример и укажи токен бота:

```bash
cp .env.example .env
nano .env
```

Ключевые переменные:
- BOT_TOKEN — токен Telegram бота
- ADMIN_LOGIN / ADMIN_PASSWORD — логин/пароль администратора (создаётся при первом запуске)
- PORT — порт, по умолчанию 8127
- SECRET_KEY — длинная случайная строка

## Лого
Замени файл app/static/maxma_logo.svg (имя файла должно быть тем же).

## Локальный запуск

 bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8127


Открой: http://127.0.0.1:8127/login

## Прод (systemd)
