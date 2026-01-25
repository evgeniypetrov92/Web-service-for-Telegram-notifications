

## 1) Подключиться к серверу (с Mac)

```bash
ssh root@<SERVER_IP>
```

---

## 2) Установить системные пакеты (на сервере)

```bash
apt update
apt install -y python3 python3-venv python3-pip unzip
```

---

## 3) Залить архив проекта на сервер (с Mac)

На **Mac**:

```bash
scp ~/Downloads/<PROJECT_ZIP>.zip root@<SERVER_IP>:/opt/
```

---

## 4) Распаковать проект в `/opt` (на сервере)

> Папка у тебя “opt” (ты писал “otp” — я использую **/opt**)

```bash
cd /opt
rm -rf <PROJECT_DIR>
mkdir -p <PROJECT_DIR>
unzip <PROJECT_ZIP>.zip -d <PROJECT_DIR>
cd /opt/<PROJECT_DIR>
```

 **Важно:** дальше команды выполняй **в корне проекта**, где лежит `requirements.txt`.

---

## 5) Создать venv и установить зависимости

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 6) Создать `.env` (оригинальные данные как у тебя)

```bash
nano .env
```

Вставь (токен/секрет можешь заменить на свои значения):

```env
BOT_TOKEN=123456789:AAAbbbCCCdddEEEfffGGGhhhIIIjjjkkk

ADMIN_LOGIN=admin
ADMIN_PASSWORD=maxmarketing

HOST=0.0.0.0
PORT=8127
BASE_URL=http://<SERVER_IP>:8127

SECRET_KEY=change_me_to_a_long_random_secret_key

UPLOAD_DIR=app/uploads
DB_PATH=app/app.db
```

Сохранить: `Ctrl+O` → Enter, выйти: `Ctrl+X`

---

## 7) Создать systemd-сервис (на сервере)

```bash
nano /etc/systemd/system/<SERVICE_NAME>.service
```

Вставь:

```ini
[Unit]
Description=<SERVICE_NAME>
After=network.target

[Service]
WorkingDirectory=/opt/<PROJECT_DIR>
EnvironmentFile=/opt/<PROJECT_DIR>/.env
ExecStart=/opt/<PROJECT_DIR>/.venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8127
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
```

Запуск:

```bash
systemctl daemon-reload
systemctl enable <SERVICE_NAME>
systemctl restart <SERVICE_NAME>
systemctl status <SERVICE_NAME> --no-pager
```

Логи:

```bash
journalctl -u <SERVICE_NAME> -n 80 --no-pager
```

---

## 8) Открыть порт 8127 (если UFW включён)

```bash
ufw allow 8127/tcp
ufw status
```

---

## 9) Проверка

Открывай:

```
http://<SERVER_IP>:8127/
```

---


