# Planka Telegram Bot

A robust Telegram bot that sends notifications about new tasks, assignments, and deadlines from [Planka](https://planka.app/) project management tool.

## ✨ Features
- **Real-time notifications** for new tasks, deadline alerts (24h, 3h, 1h, overdue), assignee changes
- **Multi-board support** with separate Telegram chats
- **Automatic recovery** from network/API errors
- **SQLite database** for state persistence
- **Health monitoring** and statistics
- **Configurable timezone** support
- **Automatic cleanup** of deleted/archived tasks

## Quick Start

### 1. Installation

```bash
git clone https://github.com/yourusername/planka-telegram-bot.git
cd planka-telegram-bot
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configuration

```bash 
cp .env.example .env
# Edit .env with your settings:
# - Planka credentials
# - Telegram bot token
# - Board to chat mapping
```

See .env.example for all available options. Key variables:

    PLANKA_URL, PLANKA_USERNAME, PLANKA_PASSWORD - Planka API access

    TELEGRAM_BOT_TOKEN - Bot token from @BotFather

    BOARD_CHAT_MAP - JSON mapping of board IDs to Telegram chat IDs

    TIMEZONE - Timezone for notifications (default: Europe/Moscow)

### 3. Running 

```bash
python3 main.py
```
