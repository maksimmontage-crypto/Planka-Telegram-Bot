#!/usr/bin/env python3
"""
Planka Telegram Bot – Public Version
Sends notifications about new tasks, assignments, and deadlines from Planka to Telegram.
"""

import os
import sys
import time
import json
import sqlite3
import requests
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
import pytz
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import threading

# Load environment variables
load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('planka-bot.log')
    ]
)
logger = logging.getLogger(__name__)


class PlankaBotService:
    def __init__(self):
        # Load config from .env
        self.config = {
            'planka_url': os.getenv('PLANKA_URL'),
            'telegram_token': os.getenv('TELEGRAM_BOT_TOKEN'),
            'planka_username': os.getenv('PLANKA_USERNAME'),
            'planka_password': os.getenv('PLANKA_PASSWORD'),
            'poll_interval': int(os.getenv('POLL_INTERVAL', 30)),
            'deadline_check_interval': int(os.getenv('DEADLINE_CHECK_INTERVAL', 30)),
            'max_retries': int(os.getenv('MAX_RETRIES', 3)),
            'timezone': os.getenv('TIMEZONE', 'Europe/Moscow'),
            'api_timeout': int(os.getenv('API_TIMEOUT', 30)),
        }

        # Validate required env vars
        required = ['planka_url', 'telegram_token', 'planka_username', 'planka_password']
        for key in required:
            if not self.config.get(key):
                logger.error(f"❌ Missing required environment variable: {key.upper()}")
                sys.exit(1)

        # Load board → chat mapping
        try:
            board_chat_str = os.getenv('BOARD_CHAT_MAP', '{}')
            self.boards_config = json.loads(board_chat_str)
            if not isinstance(self.boards_config, dict):
                raise ValueError("BOARD_CHAT_MAP must be a JSON object")
            logger.info(f"📋 Loaded mapping for {len(self.boards_config)} boards")
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in BOARD_CHAT_MAP: {e}")
            sys.exit(1)

        self.working_dir = Path.cwd()
        os.chdir(self.working_dir)

        # Caches with TTL
        self.boards_info = {}
        self.lists_cache = {}  # list_id -> (list_name, last_updated)
        self.users_cache = {}  # user_id -> (user_name, last_updated)
        self.cache_ttl = 3600  # 1 hour
        
        # Timezone
        try:
            self.tz = pytz.timezone(self.config['timezone'])
            logger.info(f"⏰ Using timezone: {self.config['timezone']}")
        except pytz.exceptions.UnknownTimeZoneError:
            logger.warning(f"⚠️ Unknown timezone: {self.config['timezone']}, falling back to UTC")
            self.tz = pytz.UTC

        # Database lock for thread safety
        self.db_lock = threading.RLock()
        
        # API session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
        self.session.timeout = self.config['api_timeout']
        
        self.init_db()
        self.auth()
        self.load_boards_info()

        # Health check
        self.last_successful_check = time.time()
        self.error_count = 0
        self.max_errors_before_restart = 10

        logger.info(f"🚀 Planka Bot started")
        logger.info(f"📊 Monitoring {len(self.boards_info)} boards")

    def init_db(self):
        """Initialize SQLite database with schema migration support."""
        with self.db_lock:
            self.db_path = self.working_dir / 'planka-bot.db'
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            cursor = self.conn.cursor()

            # Enable WAL mode for better concurrency
            cursor.execute('PRAGMA journal_mode=WAL')
            cursor.execute('PRAGMA synchronous=NORMAL')
            cursor.execute('PRAGMA busy_timeout=5000')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS processed_cards (
                    card_id TEXT PRIMARY KEY,
                    board_id TEXT NOT NULL,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tracked_tasks (
                    card_id TEXT PRIMARY KEY,
                    board_id TEXT NOT NULL,
                    board_name TEXT NOT NULL,
                    telegram_chat TEXT NOT NULL,
                    card_name TEXT NOT NULL,
                    list_id TEXT NOT NULL,
                    list_name TEXT NOT NULL,
                    due_date TIMESTAMP,
                    assigned_user_id TEXT,
                    assigned_user_name TEXT,
                    is_completed BOOLEAN DEFAULT FALSE,
                    notified_assigned BOOLEAN DEFAULT FALSE,
                    notified_24h BOOLEAN DEFAULT FALSE,
                    notified_3h BOOLEAN DEFAULT FALSE,
                    notified_1h BOOLEAN DEFAULT FALSE,
                    notified_assignee BOOLEAN DEFAULT FALSE,
                    notified_overdue BOOLEAN DEFAULT FALSE,
                    last_check TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS assignee_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    card_id TEXT NOT NULL,
                    board_id TEXT NOT NULL,
                    old_user_id TEXT,
                    old_user_name TEXT,
                    new_user_id TEXT,
                    new_user_name TEXT,
                    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    notified BOOLEAN DEFAULT FALSE
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS deleted_cards (
                    card_id TEXT PRIMARY KEY,
                    board_id TEXT NOT NULL,
                    card_name TEXT,
                    deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_board_card ON tracked_tasks(board_id, card_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_completed ON tracked_tasks(is_completed)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_due_date ON tracked_tasks(due_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_history_card ON assignee_history(card_id, board_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_deleted_cards ON deleted_cards(deleted_at)')

            # Check if we need migration from old structure
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tracked_tasks_new'")
            if cursor.fetchone():
                cursor.execute('DROP TABLE tracked_tasks_new')

            self.conn.commit()
            logger.info("✅ Database ready")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((requests.exceptions.RequestException,)),
        reraise=True
    )
    def make_request(self, method: str, url: str, **kwargs) -> Optional[requests.Response]:
        """Make HTTP request with retry logic."""
        try:
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            self.error_count = max(0, self.error_count - 1)  # Reduce error count on success
            self.last_successful_check = time.time()
            return response
        except requests.exceptions.RequestException as e:
            self.error_count += 1
            logger.warning(f"⚠️ Request failed: {e}, attempt {self.error_count}/{self.max_errors_before_restart}")
            
            if self.error_count >= self.max_errors_before_restart:
                logger.error(f"💥 Too many consecutive errors ({self.error_count}), attempting reauthentication...")
                self.auth()  # Try to reauthenticate
                self.error_count = 0
            
            raise

    def auth(self):
        """Authenticate with Planka API."""
        max_retries = self.config['max_retries']
        
        for attempt in range(max_retries):
            try:
                logger.info(f"🔐 Authenticating with Planka (attempt {attempt + 1}/{max_retries})...")
                
                # Create new session to avoid stale connections
                self.session = requests.Session()
                self.session.headers.update({'Content-Type': 'application/json'})
                self.session.timeout = self.config['api_timeout']
                
                auth = self.session.post(
                    f"{self.config['planka_url']}/api/access-tokens",
                    json={
                        "emailOrUsername": self.config['planka_username'],
                        "password": self.config['planka_password']
                    },
                    timeout=10
                )
                
                if auth.status_code == 200:
                    self.token = auth.json()['item']
                    self.session.headers.update({'Authorization': f'Bearer {self.token}'})
                    logger.info("✅ Authentication successful")
                    return True
                else:
                    logger.error(f"❌ Authentication failed: {auth.status_code} - {auth.text}")
                    
            except Exception as e:
                logger.error(f"❌ Authentication error on attempt {attempt + 1}: {e}")
                
            if attempt < max_retries - 1:
                wait_time = 5 * (attempt + 1)
                logger.info(f"⏳ Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
        
        logger.error(f"❌ Authentication failed after {max_retries} attempts")
        sys.exit(1)

    def validate_telegram_chat(self, chat_id: str) -> bool:
        """Validate that bot can send messages to chat."""
        try:
            url = f"https://api.telegram.org/bot{self.config['telegram_token']}/getChat"
            response = requests.get(url, params={'chat_id': chat_id}, timeout=10)
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"⚠️ Cannot validate chat {chat_id}: {e}")
            return True  # Assume valid for now, will fail on send

    def load_boards_info(self):
        """Fetch board metadata including 'completed' list detection."""
        logger.info("📊 Loading board info...")
        
        valid_boards = {}
        
        for board_id, chat_id in self.boards_config.items():
            if not board_id or not chat_id:
                logger.warning(f"⚠️ Skipping invalid board/chat mapping: {board_id} -> {chat_id}")
                continue
                
            # Validate chat ID
            if not self.validate_telegram_chat(chat_id):
                logger.error(f"❌ Cannot access Telegram chat {chat_id} for board {board_id}")
                continue
                
            completed_list_id = None
            try:
                url = f"{self.config['planka_url']}/api/boards/{board_id}"
                response = self.make_request('GET', url)
                
                if not response:
                    continue
                    
                board_data = response.json()
                board_name = board_data.get('item', {}).get('name', f'Board {board_id}')

                # Load lists to detect "completed"
                lists_url = f"{url}?include=lists"
                lists_res = self.make_request('GET', lists_url)
                
                if lists_res:
                    lists_data = lists_res.json()
                    if 'included' in lists_data and 'lists' in lists_data['included']:
                        completed_keywords = ['done', 'completed', 'finished', 'готово', 'выполнено', 'закрыт', 'closed']
                        for lst in lists_data['included']['lists']:
                            if isinstance(lst, dict):
                                list_id = lst.get('id')
                                list_name = lst.get('name', '').lower()
                                self.lists_cache[list_id] = {'name': lst.get('name', 'Unnamed'), 'updated': time.time()}
                                if any(kw in list_name for kw in completed_keywords):
                                    completed_list_id = list_id
                                    logger.debug(f"📌 Found completed list '{lst.get('name')}' for board '{board_name}'")

                valid_boards[board_id] = {
                    'name': board_name,
                    'chat_id': chat_id,
                    'completed_list_id': completed_list_id,
                    'last_checked': 0
                }
                logger.info(f"✅ Board: '{board_name}' (ID: {board_id}) → Chat: {chat_id}")
                
            except Exception as e:
                logger.error(f"❌ Error loading board {board_id}: {e}")
                # Don't fail completely, skip this board
                continue
        
        self.boards_info = valid_boards
        
        # Clean old cache entries
        current_time = time.time()
        self.lists_cache = {k: v for k, v in self.lists_cache.items() 
                           if current_time - v['updated'] < self.cache_ttl}
        
        logger.info(f"📝 Loaded {len(self.lists_cache)} lists for {len(self.boards_info)} boards")

    def get_board_cards(self, board_id: str, page: int = 1, per_page: int = 100) -> List[Dict]:
        """Fetch cards from board with pagination."""
        try:
            url = f"{self.config['planka_url']}/api/boards/{board_id}/cards"
            params = {'page': page, 'perPage': per_page}
            
            response = self.make_request('GET', url, params=params)
            if not response:
                return []
                
            data = response.json()
            cards = data.get('items', [])
            
            # Check if there are more pages
            total_pages = data.get('totalPages', 1)
            if page < total_pages:
                next_cards = self.get_board_cards(board_id, page + 1, per_page)
                cards.extend(next_cards)
                
            return cards
            
        except Exception as e:
            logger.error(f"❌ Error fetching cards for board {board_id}: {e}")
            return []

    def check_and_update_tasks(self, cards: List[Dict]):
        """Check for new/updated cards and send notifications."""
        with self.db_lock:
            cursor = self.conn.cursor()
            
            for card in cards:
                try:
                    card_id = card.get('id')
                    board_id = card.get('boardId')
                    
                    if not card_id or not board_id:
                        continue
                        
                    # Check if card exists in processed table
                    cursor.execute(
                        "SELECT 1 FROM processed_cards WHERE card_id = ? AND board_id = ?",
                        (card_id, board_id)
                    )
                    
                    if not cursor.fetchone():
                        # New card found
                        self.process_new_card(card)
                        cursor.execute(
                            "INSERT OR REPLACE INTO processed_cards (card_id, board_id) VALUES (?, ?)",
                            (card_id, board_id)
                        )
                    else:
                        # Existing card, check for updates
                        self.update_existing_card(card)
                        
                except Exception as e:
                    logger.error(f"❌ Error processing card {card.get('id')}: {e}")
            
            # Clean up deleted cards
            self.cleanup_deleted_cards(cards)
            
            self.conn.commit()

    def process_new_card(self, card: Dict):
        """Process a newly discovered card."""
        board_id = card.get('boardId')
        board_info = self.boards_info.get(board_id)
        
        if not board_info:
            return
            
        try:
            # Check if it's in completed list
            list_id = card.get('listId')
            is_completed = (list_id == board_info.get('completed_list_id'))
            
            assigned_user_id = card.get('assignedUserId')
            assigned_user_name = card.get('assignedUserName', '')
            
            # Store in tracked tasks
            with self.db_lock:
                cursor = self.conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO tracked_tasks 
                    (card_id, board_id, board_name, telegram_chat, card_name, 
                     list_id, list_name, due_date, assigned_user_id, assigned_user_name,
                     is_completed, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    card['id'],
                    board_id,
                    board_info['name'],
                    board_info['chat_id'],
                    card.get('name', 'Untitled'),
                    list_id,
                    self.lists_cache.get(list_id, {}).get('name', 'Unknown'),
                    card.get('dueDate'),
                    assigned_user_id,
                    assigned_user_name,
                    is_completed,
                    datetime.now(tz=timezone.utc)
                ))
            
            # Send notification only if not already completed
            if not is_completed:
                self.send_new_card_notification(card, board_info)
                
        except Exception as e:
            logger.error(f"❌ Error processing new card {card.get('id')}: {e}")

    def update_existing_card(self, card: Dict):
        """Check for updates in existing card."""
        with self.db_lock:
            cursor = self.conn.cursor()
            
            cursor.execute(
                "SELECT * FROM tracked_tasks WHERE card_id = ? AND board_id = ?",
                (card['id'], card['boardId'])
            )
            existing = cursor.fetchone()
            
            if not existing:
                return
                
            # Check for assignee change
            new_assignee_id = card.get('assignedUserId')
            old_assignee_id = existing['assigned_user_id']
            
            if new_assignee_id != old_assignee_id:
                self.log_assignee_change(card, existing)
                
            # Check for due date change
            new_due_date = card.get('dueDate')
            old_due_date = existing['due_date']
            
            if new_due_date != old_due_date and new_due_date:
                self.send_deadline_notification(card, existing)
                
            # Check if moved to completed list
            board_info = self.boards_info.get(card['boardId'])
            if board_info and card.get('listId') == board_info.get('completed_list_id'):
                cursor.execute(
                    "UPDATE tracked_tasks SET is_completed = TRUE WHERE card_id = ?",
                    (card['id'],)
                )
                self.send_completion_notification(card, existing)
            
            # Update last checked timestamp
            cursor.execute(
                "UPDATE tracked_tasks SET last_check = ? WHERE card_id = ?",
                (datetime.now(tz=timezone.utc), card['id'])
            )

    def cleanup_deleted_cards(self, current_cards: List[Dict]):
        """Remove cards that no longer exist in Planka."""
        with self.db_lock:
            cursor = self.conn.cursor()
            
            # Get all card IDs from current cards
            current_card_ids = {card['id'] for card in current_cards if 'id' in card}
            
            if not current_card_ids:
                return
                
            # Find tracked cards not in current cards
            placeholders = ','.join(['?'] * len(current_card_ids))
            cursor.execute(f'''
                SELECT card_id, board_id, card_name 
                FROM tracked_tasks 
                WHERE card_id NOT IN ({placeholders}) AND is_completed = FALSE
            ''', list(current_card_ids))
            
            deleted_cards = cursor.fetchall()
            
            for card in deleted_cards:
                # Move to deleted table
                cursor.execute('''
                    INSERT OR REPLACE INTO deleted_cards (card_id, board_id, card_name)
                    VALUES (?, ?, ?)
                ''', (card['card_id'], card['board_id'], card['card_name']))
                
                # Remove from tracked tasks
                cursor.execute(
                    "DELETE FROM tracked_tasks WHERE card_id = ?",
                    (card['card_id'],)
                )
                
                logger.info(f"🗑️ Card '{card['card_name']}' ({card['card_id']}) was deleted from board")
                
            # Clean old deleted records (older than 7 days)
            week_ago = datetime.now(tz=timezone.utc) - timedelta(days=7)
            cursor.execute(
                "DELETE FROM deleted_cards WHERE deleted_at < ?",
                (week_ago,)
            )

    def log_assignee_change(self, card: Dict, existing_task: sqlite3.Row):
        """Log assignee change for later notification."""
        with self.db_lock:
            cursor = self.conn.cursor()
            
            cursor.execute('''
                INSERT INTO assignee_history 
                (card_id, board_id, old_user_id, old_user_name, new_user_id, new_user_name)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                card['id'],
                card['boardId'],
                existing_task['assigned_user_id'],
                existing_task['assigned_user_name'],
                card.get('assignedUserId'),
                card.get('assignedUserName', '')
            ))

    def send_to_telegram(self, message: str, chat_id: str, parse_mode: str = 'HTML') -> bool:
        """Send message to Telegram with retry logic."""
        try:
            url = f"https://api.telegram.org/bot{self.config['telegram_token']}/sendMessage"
            payload = {
                'chat_id': chat_id,
                'text': message,
                'parse_mode': parse_mode
            }
            
            response = requests.post(url, json=payload, timeout=10)
            
            if response.status_code == 200:
                return True
            elif response.status_code == 400:
                # Bad request, likely invalid chat_id
                data = response.json()
                if 'description' in data and 'chat not found' in data['description']:
                    logger.error(f"❌ Chat {chat_id} not found or bot removed")
                    # Remove this board from monitoring
                    for board_id, info in list(self.boards_info.items()):
                        if info['chat_id'] == chat_id:
                            del self.boards_info[board_id]
                            logger.warning(f"🗑️ Removed board {board_id} due to invalid chat")
                return False
            else:
                logger.error(f"❌ Telegram API error {response.status_code}: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Failed to send Telegram message: {e}")
            return False

    def send_new_card_notification(self, card: Dict, board_info: Dict):
        """Send notification about new card."""
        try:
            list_name = self.lists_cache.get(card.get('listId', ''), {}).get('name', 'Unknown list')
            board_name = board_info.get('name', 'Unknown board')
            chat_id = board_info.get('chat_id', '')

            message = (
                f"❗️<b>New Task</b>\n\n"
                f"🆕 <b>Title:</b> {card.get('name', 'Untitled')}\n"
                f"📊 <b>Category:</b> {list_name}\n"
                f"💻 <b>Board:</b> {board_name}"
            )

            created_at = card.get('createdAt')
            if created_at:
                try:
                    dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    dt_local = dt.astimezone(self.tz)
                    message += f"\n🕠 <b>Created ({self.tz.zone}):</b> {dt_local.strftime('%H:%M %d.%m.%Y')}"
                except:
                    pass

            if card.get('dueDate'):
                message += f"\n🗓 <b>Deadline:</b> {self.format_local_time(card['dueDate'])}"
            if card.get('assignedUserName'):
                message += f"\n👤 <b>Assignee:</b> {card['assignedUserName']}"

            if self.send_to_telegram(message, chat_id):
                logger.info(f"📨 Sent new task notification: {card.get('name')}")
                return True
            return False
            
        except Exception as e:
            logger.error(f"❌ Failed to send new task notification: {e}")
            return False

    def format_local_time(self, timestamp: str) -> str:
        """Format timestamp to local timezone."""
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            dt_local = dt.astimezone(self.tz)
            return dt_local.strftime('%H:%M %d.%m.%Y')
        except:
            return timestamp

    def check_deadlines(self):
        """Check for approaching deadlines."""
        with self.db_lock:
            cursor = self.conn.cursor()
            now = datetime.now(tz=timezone.utc)
            
            # Tasks due in 24 hours
            day_from_now = now + timedelta(hours=24)
            cursor.execute('''
                SELECT * FROM tracked_tasks 
                WHERE due_date IS NOT NULL 
                AND due_date BETWEEN ? AND ?
                AND is_completed = FALSE
                AND notified_24h = FALSE
            ''', (now, day_from_now))
            
            for task in cursor.fetchall():
                self.send_deadline_warning(task, '24h')
                cursor.execute(
                    "UPDATE tracked_tasks SET notified_24h = TRUE WHERE card_id = ?",
                    (task['card_id'],)
                )
            
            # Tasks due in 3 hours
            three_hours_from_now = now + timedelta(hours=3)
            cursor.execute('''
                SELECT * FROM tracked_tasks 
                WHERE due_date IS NOT NULL 
                AND due_date BETWEEN ? AND ?
                AND is_completed = FALSE
                AND notified_3h = FALSE
            ''', (now, three_hours_from_now))
            
            for task in cursor.fetchall():
                self.send_deadline_warning(task, '3h')
                cursor.execute(
                    "UPDATE tracked_tasks SET notified_3h = TRUE WHERE card_id = ?",
                    (task['card_id'],)
                )
            
            # Tasks due in 1 hour
            hour_from_now = now + timedelta(hours=1)
            cursor.execute('''
                SELECT * FROM tracked_tasks 
                WHERE due_date IS NOT NULL 
                AND due_date BETWEEN ? AND ?
                AND is_completed = FALSE
                AND notified_1h = FALSE
            ''', (now, hour_from_now))
            
            for task in cursor.fetchall():
                self.send_deadline_warning(task, '1h')
                cursor.execute(
                    "UPDATE tracked_tasks SET notified_1h = TRUE WHERE card_id = ?",
                    (task['card_id'],)
                )
            
            # Overdue tasks
            cursor.execute('''
                SELECT * FROM tracked_tasks 
                WHERE due_date IS NOT NULL 
                AND due_date < ?
                AND is_completed = FALSE
                AND notified_overdue = FALSE
            ''', (now,))
            
            for task in cursor.fetchall():
                self.send_overdue_notification(task)
                cursor.execute(
                    "UPDATE tracked_tasks SET notified_overdue = TRUE WHERE card_id = ?",
                    (task['card_id'],)
                )
            
            self.conn.commit()

    def send_deadline_warning(self, task: sqlite3.Row, timeframe: str):
        """Send deadline warning notification."""
        try:
            time_text = {
                '24h': '24 hours',
                '3h': '3 hours',
                '1h': '1 hour'
            }.get(timeframe, timeframe)
            
            message = (
                f"⏰ <b>Deadline Approaching</b>\n\n"
                f"📝 <b>Task:</b> {task['card_name']}\n"
                f"📊 <b>Board:</b> {task['board_name']}\n"
                f"🗓 <b>Deadline ({self.tz.zone}):</b> {self.format_local_time(task['due_date'])}\n"
                f"⚠️ <b>Time left:</b> {time_text}"
            )
            
            if task['assigned_user_name']:
                message += f"\n👤 <b>Assignee:</b> {task['assigned_user_name']}"
            
            if self.send_to_telegram(message, task['telegram_chat']):
                logger.info(f"⏰ Sent {timeframe} deadline warning for: {task['card_name']}")
                
        except Exception as e:
            logger.error(f"❌ Failed to send deadline warning: {e}")

    def send_overdue_notification(self, task: sqlite3.Row):
        """Send overdue task notification."""
        try:
            message = (
                f"🚨 <b>TASK OVERDUE!</b>\n\n"
                f"📝 <b>Task:</b> {task['card_name']}\n"
                f"📊 <b>Board:</b> {task['board_name']}\n"
                f"🗓 <b>Deadline was:</b> {self.format_local_time(task['due_date'])}\n"
                f"⏰ <b>Status:</b> OVERDUE!"
            )
            
            if task['assigned_user_name']:
                message += f"\n👤 <b>Assignee:</b> {task['assigned_user_name']}"
            
            if self.send_to_telegram(message, task['telegram_chat']):
                logger.info(f"🚨 Sent overdue notification for: {task['card_name']}")
                
        except Exception as e:
            logger.error(f"❌ Failed to send overdue notification: {e}")

    def check_pending_assignee_notifications(self):
        """Send notifications about assignee changes."""
        with self.db_lock:
            cursor = self.conn.cursor()
            
            cursor.execute('''
                SELECT h.*, t.board_name, t.card_name, t.telegram_chat 
                FROM assignee_history h
                JOIN tracked_tasks t ON h.card_id = t.card_id AND h.board_id = t.board_id
                WHERE h.notified = FALSE
            ''')
            
            for change in cursor.fetchall():
                self.send_assignee_change_notification(change)
                cursor.execute(
                    "UPDATE assignee_history SET notified = TRUE WHERE id = ?",
                    (change['id'],)
                )
            
            self.conn.commit()

    def send_assignee_change_notification(self, change: sqlite3.Row):
        """Send notification about assignee change."""
        try:
            message = (
                f"👤 <b>Assignee Changed</b>\n\n"
                f"📝 <b>Task:</b> {change['card_name']}\n"
                f"📊 <b>Board:</b> {change['board_name']}\n"
            )
            
            if change['old_user_name']:
                message += f"⬅️ <b>From:</b> {change['old_user_name']}\n"
            else:
                message += f"⬅️ <b>From:</b> Unassigned\n"
                
            if change['new_user_name']:
                message += f"➡️ <b>To:</b> {change['new_user_name']}\n"
            else:
                message += f"➡️ <b>To:</b> Unassigned\n"
            
            message += f"🕒 <b>Changed at:</b> {change['changed_at']}"
            
            if self.send_to_telegram(message, change['telegram_chat']):
                logger.info(f"👤 Sent assignee change notification for: {change['card_name']}")
                
        except Exception as e:
            logger.error(f"❌ Failed to send assignee change notification: {e}")

    def health_check(self) -> Dict[str, Any]:
        """Return health status of the bot."""
        with self.db_lock:
            cursor = self.conn.cursor()
            
            cursor.execute('SELECT COUNT(*) FROM tracked_tasks WHERE is_completed = FALSE')
            active_tasks = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM tracked_tasks WHERE due_date IS NOT NULL AND is_completed = FALSE')
            tasks_with_deadline = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM assignee_history WHERE notified = FALSE')
            pending_notifications = cursor.fetchone()[0]
        
        return {
            'status': 'healthy' if self.error_count < 5 else 'degraded',
            'error_count': self.error_count,
            'last_successful_check': self.last_successful_check,
            'uptime': time.time() - self.start_time if hasattr(self, 'start_time') else 0,
            'active_boards': len(self.boards_info),
            'active_tasks': active_tasks,
            'tasks_with_deadline': tasks_with_deadline,
            'pending_notifications': pending_notifications,
            'cache_size': len(self.lists_cache)
        }

    def run(self):
        """Main loop."""
        self.start_time = time.time()
        
        logger.info("=" * 60)
        logger.info(f"🤖 PLANKA TELEGRAM BOT v3.0")
        logger.info(f"📊 Boards monitored: {len(self.boards_info)}")
        logger.info(f"⏰ Timezone: {self.tz.zone}")
        logger.info(f"⏱️ Deadline check interval: {self.config['deadline_check_interval']}s")
        logger.info(f"🔄 Polling interval: {self.config['poll_interval']}s")
        logger.info("=" * 60)
        
        # Grace period on startup
        logger.info("⏳ Initial grace period (10 seconds)...")
        time.sleep(10)

        last_deadline_check = time.time()
        last_full_check = time.time()
        last_health_check = time.time()

        while True:
            try:
                current_time = time.time()
                
                # Check if we need to reauthenticate (every 12 hours)
                if current_time - self.last_successful_check > 43200:  # 12 hours
                    logger.info("🔄 Token refresh check...")
                    self.auth()
                
                # Process each board with staggered delays
                for board_id, board_info in list(self.boards_info.items()):
                    try:
                        logger.info(f"🔍 Checking board: '{board_info['name']}'...")
                        cards = self.get_board_cards(board_id)
                        if cards:
                            self.check_and_update_tasks(cards)
                            board_info['last_checked'] = current_time
                        time.sleep(2)  # Small delay between boards
                    except Exception as e:
                        logger.error(f"❌ Error checking board {board_id}: {e}")
                
                # Check deadlines
                if current_time - last_deadline_check > self.config['deadline_check_interval']:
                    logger.info("⏰ Checking deadlines...")
                    self.check_deadlines()
                    last_deadline_check = current_time
                
                # Check pending notifications
                self.check_pending_assignee_notifications()
                
                # Hourly stats and cleanup
                if current_time - last_full_check > 3600:
                    health = self.health_check()
                    logger.info(
                        f"📈 Stats: {health['active_tasks']} active tasks, "
                        f"{health['tasks_with_deadline']} with deadlines, "
                        f"{health['pending_notifications']} pending notifications"
                    )
                    
                    # Clean old processed cards (older than 30 days)
                    with self.db_lock:
                        cursor = self.conn.cursor()
                        month_ago = datetime.now(tz=timezone.utc) - timedelta(days=30)
                        cursor.execute(
                            "DELETE FROM processed_cards WHERE processed_at < ?",
                            (month_ago,)
                        )
                        self.conn.commit()
                    
                    last_full_check = current_time
                
                # Health check logging
                if current_time - last_health_check > 300:  # Every 5 minutes
                    health = self.health_check()
                    if health['error_count'] > 0:
                        logger.warning(f"⚠️ Health status: {health}")
                    last_health_check = current_time
                
                logger.info(f"⏳ Sleeping for {self.config['poll_interval']}s...")
                time.sleep(self.config['poll_interval'])
                
            except KeyboardInterrupt:
                logger.info("🛑 Service stopped by user")
                break
            except Exception as e:
                logger.error(f"💥 Main loop error: {e}", exc_info=True)
                time.sleep(30)  # Longer delay on major error

        # Clean shutdown
        self.session.close()
        self.conn.close()
        logger.info("👋 Service stopped gracefully")


def main():
    service = PlankaBotService()
    service.run()


if __name__ == '__main__':
    main()