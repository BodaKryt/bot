import logging
import asyncio
import json
import os
import sqlite3
import re
import matplotlib.pyplot as plt
import pandas as pd
import random
from fastapi import FastAPI, Request
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import textwrap
from datetime import datetime, timedelta
from io import BytesIO
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from telegram.error import TelegramError
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()
TOKEN = os.getenv('BOT_TOKEN')
MAIN_ADMIN_ID = int(os.getenv('MAIN_ADMIN_ID', '2022216691'))
CHANNEL_ID = int(os.getenv('CHANNEL_ID', '-1001591221346'))
POSTS_DB_FILE = os.getenv('POSTS_DB_FILE', 'posts.db')
SUBSCRIPTIONS_DB_FILE = os.getenv('SUBSCRIPTIONS_DB_FILE', 'subscriptions.db')
START_MESSAGE_ID = int(os.getenv('START_MESSAGE_ID', '3'))
MAX_SKIPPED_IDS = int(os.getenv('MAX_SKIPPED_IDS', '170'))

app = FastAPI()

if not TOKEN:
    raise ValueError("Переменная окружения BOT_TOKEN не установлена!")

# Словарь локализации
MESSAGES = {
    'ru': {
        'welcome': '✨ **Добро пожаловать в мир актуальных модов!** ✨\n\n📱 Здесь вы найдёте **самые свежие версии** приложений с крутыми модификациями\n🔔 **Подпишитесь** — и обновления будут приходить мгновенно!\n🔍 **Ищите** по названию — бот найдёт всё за секунды\n👍 **Оценивайте** посты — помогайте другим выбрать лучшее!\n\n👇 Выберите действие ниже и погрузитесь в мир возможностей! 👇',
        'subscribe_prompt': '📝 Введите название приложения для подписки или выберите из списка:',
        'unsubscribe_prompt': '📝 Выберите приложение для отписки:',
        'no_subscriptions': '❌ У вас нет активных подписок.',
        'subscribed': '🔔 Вы подписались на обновления "{app_title}".',
        'unsubscribed': '🔕 Вы отписались от обновлений "{app_title}".',
        'not_subscribed': '❌ Вы не были подписаны на "{app_title}".',
        'empty_app_name': '❌ Укажите название приложения!',
        'db_error': '❌ Ошибка. Попробуйте снова.',
        'no_posts_db': '❌ Поиск недоступен: база данных постов не инициализирована.',
        'admin_only': '❌ Эта команда доступна только главному админу в личных сообщениях!',
        'no_admin_rights': '❌ Бот не имеет прав администратора в канале.',
        'channel_id_error': '❌ Не удалось получить ID канала.',
        'parsing_complete': '✅ Парсинг завершён: обработано {count} постов, пропущено {skipped_count} ID.',
        'buttons_added': '✅ Добавление кнопок завершено: добавлено {count_added} кнопок, пропущено {count_skipped} постов.',
        'error': '❌ Произошла ошибка. Попробуйте снова или обратитесь в поддержку.',
        'search_results': '📱 {title}:\n📦 Версия: {version}\n🛠️ Мод: {mod}\n🔗 Ссылка: {link}\n',
        'read_more': 'Подробности по ссылке: {link}',
    },
    'en': {
        'welcome': '✨ **Welcome to the world of cutting-edge mods!** ✨\n\n📱 Discover **the latest app versions** with powerful modifications\n🔔 **Subscribe** — get updates instantly as they drop!\n🔍 **Search** by name — find anything in seconds\n👍 **Rate posts** — help others pick the best!\n\n👇 Choose an action below and dive into endless possibilities! 👇',
        'subscribe_prompt': '📝 Enter the app name to subscribe or select from the list:',
        'unsubscribe_prompt': '📝 Select an app to unsubscribe from:',
        'no_subscriptions': '❌ You have no active subscriptions.',
        'subscribed': '🔔 You have subscribed to updates for "{app_title}".',
        'unsubscribed': '🔕 You have unsubscribed from updates for "{app_title}".',
        'not_subscribed': '❌ You were not subscribed to "{app_title}".',
        'empty_app_name': '❌ Please specify the app name!',
        'db_error': '❌ An error occurred. Try again.',
        'no_posts_db': '❌ Search unavailable: posts database not initialized.',
        'admin_only': '❌ This command is only available to the main admin in private messages!',
        'no_admin_rights': '❌ The bot does not have admin rights in the channel.',
        'channel_id_error': '❌ Could not retrieve channel ID.',
        'parsing_complete': '✅ Parsing completed: processed {count} posts, skipped {skipped_count} IDs.',
        'buttons_added': '✅ Adding buttons completed: added {count_added} buttons, skipped {count_skipped} posts.',
        'error': '❌ An error occurred. Try again or contact support.',
        'search_results': '📱 {title}:\n📦 Version: {version}\n🛠️ Mod: {mod}\n🔗 Link: {link}\n',
        'read_more': 'Read more: {link}',
    }
}

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

file_handler = RotatingFileHandler('bot.log', maxBytes=10*1024*1024, backupCount=5, encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Блокировка для потокобезопасности SQLite
db_lock = asyncio.Lock()

def init_posts_db():
    logger.debug("Начало инициализации базы данных постов")
    with sqlite3.connect(POSTS_DB_FILE) as conn:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS posts
                     (message_id INTEGER PRIMARY KEY,
                      title TEXT,
                      version TEXT,
                      full_text TEXT,
                      date DATETIME,
                      media_json TEXT,
                      mod TEXT)''')  # ← ДОБАВИЛИ mod TEXT

        # === МИГРАЦИЯ: Добавляем столбец mod, если его нет ===
        c.execute("PRAGMA table_info(posts)")
        columns = [info[1] for info in c.fetchall()]
        if 'mod' not in columns:
            c.execute("ALTER TABLE posts ADD COLUMN mod TEXT")
            logger.info("Добавлен столбец 'mod' в таблицу posts")

        # === ИНДЕКСЫ ===
        c.execute('CREATE INDEX IF NOT EXISTS idx_posts_title ON posts(lower(title))')
        c.execute('CREATE INDEX IF NOT EXISTS idx_posts_date ON posts(date)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_posts_mod ON posts(lower(mod))')  # ← НОВЫЙ ИНДЕКС
        # reactions таблица остаётся без изменений
        c.execute('''CREATE TABLE IF NOT EXISTS reactions
                     (post_id INTEGER,
                      user_id INTEGER,
                      reaction INTEGER,
                      PRIMARY KEY (post_id, user_id))''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_reactions_post_id ON reactions(post_id)')
        conn.commit()
    logger.info("База данных постов инициализирована")

def init_subscriptions_db():
    logger.debug("Начало инициализации базы данных подписок")
    with sqlite3.connect(SUBSCRIPTIONS_DB_FILE) as conn:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS subscriptions
                     (user_id INTEGER,
                      app_title TEXT,
                      PRIMARY KEY (user_id, app_title))''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_subscriptions_app ON subscriptions(lower(app_title))')
        conn.commit()
    logger.info("База данных подписок инициализирована")
    logger.debug("Инициализация базы данных подписок завершена")

init_posts_db()
init_subscriptions_db()

def extract_mod(full_text):
    """Извлекает строку мода из полного текста поста"""
    if not full_text:
        return ''
    for line in full_text.split('\n'):
        line = line.strip()
        if line.startswith('🛠️ Мод:') or line.startswith('🛠️ Mod:') or line.startswith('🛠️ Моды:'):
            # Берём всё после двоеточия
            parts = line.split(':', 1)
            if len(parts) > 1:
                return parts[1].strip()
    return ''

def save_post_to_db(message_id, title, version, full_text, date_str, media_json, mod=''):
    logger.debug(f"Начало сохранения поста ID {message_id}: title={title}, version={version}, mod={mod}")
    try:
        with sqlite3.connect(POSTS_DB_FILE) as conn:
            c = conn.cursor()
            c.execute("""INSERT OR REPLACE INTO posts 
                         (message_id, title, version, full_text, date, media_json, mod) 
                         VALUES (?, ?, ?, ?, ?, ?, ?)""",
                      (message_id, title, version, full_text, date_str, media_json, mod))
            conn.commit()
        logger.info(f"Сохранён пост: {title} (ID: {message_id}, Версия: {version or 'Не указана'}, Мод: {mod or '—'})")
    except Exception as error:
        logger.error(f"Ошибка при сохранении поста ID {message_id}: {error}")

def get_reaction_counts(post_id):
    with sqlite3.connect(POSTS_DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM reactions WHERE post_id = ? AND reaction = 1", (post_id,))
        likes = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM reactions WHERE post_id = ? AND reaction = -1", (post_id,))
        dislikes = c.fetchone()[0]
    return likes, dislikes

def get_user_reaction(post_id, user_id):
    with sqlite3.connect(POSTS_DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT reaction FROM reactions WHERE post_id = ? AND user_id = ?", (post_id, user_id))
        result = c.fetchone()
    return result[0] if result else None

async def update_reaction(post_id, user_id, new_reaction, query):
    async with db_lock:
        with sqlite3.connect(POSTS_DB_FILE) as conn:
            c = conn.cursor()
            current = get_user_reaction(post_id, user_id)
            if current == new_reaction:
                await query.answer("Вы уже поставили эту реакцию!")
                logger.debug(f"Пользователь {user_id} пытался повторно поставить реакцию {new_reaction} на пост {post_id}")
                return False  # Реакция не обновлена
            c.execute("""INSERT OR REPLACE INTO reactions (post_id, user_id, reaction) 
                         VALUES (?, ?, ?)""", (post_id, user_id, new_reaction))
            conn.commit()
            await query.answer(f"Поставлен {'лайк' if new_reaction == 1 else 'дизлайк'}.")
            return True  # Реакция обновлена

def get_user_language(user_id):
    # Заглушка для определения языка пользователя (например, из настроек или БД)
    # Для простоты по умолчанию используется русский
    return 'ru'

async def retry_with_backoff(coro, max_attempts=5, base_delay=1.0):
    for attempt in range(max_attempts):
        try:
            return await coro
        except TelegramError as error:
            if 'flood control exceeded' in str(error).lower():
                match = re.search(r'retry in (\d+) seconds', str(error).lower())
                delay = int(match.group(1)) if match else 20
            elif 'too many requests' in str(error).lower():
                delay = base_delay * (2 ** attempt)
            else:
                logger.error(f"Ошибка Telegram API: {error}")
                raise
            logger.warning(f"Повторная попытка через {delay} секунд (попытка {attempt + 1}/{max_attempts})")
            await asyncio.sleep(delay)
    raise TelegramError("Достигнуто максимальное количество попыток")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    lang = get_user_language(user_id)
    logger.info(f"Команда /start вызвана пользователем {user_id}")

    # Основные действия
    keyboard = [
        [InlineKeyboardButton(f"🔔 {'Подписаться' if lang == 'ru' else 'Subscribe'}", callback_data='subscribe')],
        [InlineKeyboardButton(f"🔕 {'Отписаться' if lang == 'ru' else 'Unsubscribe'}", callback_data='unsubscribe')],
        [InlineKeyboardButton("🎲 Рандомный мод", callback_data='random_mod')],
        [InlineKeyboardButton("🏆 Рейтинг моддеров", callback_data='leaderboard')]
    ]

    # Статистика — общая или админская
    if user_id == MAIN_ADMIN_ID and update.effective_chat.type == 'private':
        # Админские кнопки — группируем по смыслу
        keyboard.extend([
            [InlineKeyboardButton("📊 Админ-статистика", callback_data='show_stats')],
            [InlineKeyboardButton("📥 Парсинг постов", callback_data='parse_posts')],
            [InlineKeyboardButton("👍 Добавить кнопки лайков", callback_data='add_reaction_buttons')],
        ])
        logger.debug("Добавлены админские кнопки")
    else:
        # Обычный пользователь
        keyboard.append([InlineKeyboardButton("📈 " + ("Статистика" if lang == 'ru' else "Statistics"), callback_data='show_stats_user')])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        MESSAGES[lang]['welcome'],
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )
    logger.debug(f"Отправлено приветственное сообщение пользователю {user_id}")

async def get_popular_apps():
    async with db_lock:
        with sqlite3.connect(POSTS_DB_FILE) as conn:
            c = conn.cursor()
            c.execute("SELECT DISTINCT title FROM posts LIMIT 10")
            return [row[0] for row in c.fetchall()]

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    lang = get_user_language(user_id)
    logger.info(f"Пользователь {user_id} нажал кнопку: {data}")
    logger.debug(f"Обработка callback от пользователя {user_id} с данными {data}")

    if data == 'subscribe':
        context.user_data['action'] = 'subscribe'
        keyboard = []
        popular_apps = await get_popular_apps()
        
        # Проверяем, на что уже подписан пользователь
        async with db_lock:
            with sqlite3.connect(SUBSCRIPTIONS_DB_FILE) as conn:
                c = conn.cursor()
                c.execute("SELECT app_title FROM subscriptions WHERE user_id = ?", (user_id,))
                subscribed = {row[0] for row in c.fetchall()}

        for app in popular_apps:
            if app not in subscribed:
                keyboard.append([InlineKeyboardButton(f"{app}", callback_data=f'sub_{app}')])
            else:
                keyboard.append([InlineKeyboardButton(f"{app} (уже подписан)", callback_data='already_subscribed')])

        keyboard.append([InlineKeyboardButton("🔙 Назад в меню", callback_data='cancel_action')])

        reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
        await query.message.reply_text(MESSAGES[lang]['subscribe_prompt'], reply_markup=reply_markup)
    elif data.startswith('sub_'):
        app_title = data[4:]
        
        # Проверяем, уже ли подписан
        async with db_lock:
            with sqlite3.connect(SUBSCRIPTIONS_DB_FILE) as conn:
                c = conn.cursor()
                c.execute("SELECT 1 FROM subscriptions WHERE user_id = ? AND app_title = ?", (user_id, app_title))
                if c.fetchone():
                    await query.answer("Вы уже подписаны на это приложение!")
                    return

        try:
            async with db_lock:
                with sqlite3.connect(SUBSCRIPTIONS_DB_FILE) as conn:
                    c = conn.cursor()
                    c.execute("INSERT INTO subscriptions (user_id, app_title) VALUES (?, ?)", (user_id, app_title))
                    conn.commit()

            # УСПЕХ: отключаем кнопку
            await query.answer("Подписка оформлена!")
            await query.edit_message_reply_markup(reply_markup=None)  # Убираем кнопки

            await query.message.reply_text(MESSAGES[lang]['subscribed'].format(app_title=app_title))
            logger.info(f"Пользователь {user_id} подписался на '{app_title}'")

        except Exception as error:
            logger.error(f"Ошибка подписки: {error}")
            await query.answer("Ошибка. Попробуйте позже.")
    elif data == 'unsubscribe':
        logger.debug("Запрос списка подписок для отписки")
        try:
            async with db_lock:
                with sqlite3.connect(SUBSCRIPTIONS_DB_FILE) as conn:
                    c = conn.cursor()
                    c.execute("SELECT app_title FROM subscriptions WHERE user_id = ?", (user_id,))
                    subs = c.fetchall()
            if not subs:
                await query.message.reply_text(MESSAGES[lang]['no_subscriptions'])
                logger.info(f"У пользователя {user_id} нет подписок")
                return
            keyboard = []
            for app in subs:
                keyboard.append([InlineKeyboardButton(app[0], callback_data=f'unsub_{app[0]}')])
                
            keyboard.append([InlineKeyboardButton("🔙 Назад в меню", callback_data='cancel_action')])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.reply_text(MESSAGES[lang]['unsubscribe_prompt'], reply_markup=reply_markup)
            logger.debug(f"Отправлен список подписок: {len(subs)}")
        except Exception as error:
            logger.error(f"Ошибка при получении подписок для {user_id}: {error}")
            await query.message.reply_text(MESSAGES[lang]['db_error'])
        finally:
            # ВСЕГДА УДАЛЯЕМ action!
            if 'action' in context.user_data:
                del context.user_data['action']
            logger.debug("Действие очищено")
        return
    elif data.startswith('unsub_'):
        app_title = data[6:]
        if query.message.reply_markup is None:
            await query.answer("Это действие уже выполнено.")
            return

        try:
            async with db_lock:
                with sqlite3.connect(SUBSCRIPTIONS_DB_FILE) as conn:
                    c = conn.cursor()
                    c.execute("DELETE FROM subscriptions WHERE user_id = ? AND app_title = ?", (user_id, app_title))
                    deleted = c.rowcount > 0

            if deleted:
                await query.answer("Вы отписались!")
                await query.edit_message_reply_markup(reply_markup=None)
                await query.message.reply_text(MESSAGES[lang]['unsubscribed'].format(app_title=app_title))
                logger.info(f"Пользователь {user_id} отписался от '{app_title}'")
            else:
                await query.answer("Вы не были подписаны.")
        except Exception as error:
            logger.error(f"Ошибка отписки: {error}")
            await query.answer("Ошибка.")
    
    # В button_callback():
    elif data == 'random_mod':
        async with db_lock:
            with sqlite3.connect(POSTS_DB_FILE) as conn:
                c = conn.cursor()
                c.execute("SELECT message_id, title, version, mod FROM posts ORDER BY RANDOM() LIMIT 1")
                row = c.fetchone()
                if not row:
                    await query.answer("Пока нет модов!")
                    return
                mid, title, version, mod = row
                link = f"https://t.me/c/{str(CHANNEL_ID)[4:]}/{mid}"
                text = MESSAGES[lang]['search_results'].format(
                    title=title, version=version or '—', mod=mod or '—', link=link
                )
                keyboard = [[InlineKeyboardButton("Перейти", url=link)]]
                keyboard.append([InlineKeyboardButton("🔙 Назад в меню", callback_data='back')])
                await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                await query.answer("Вот твой случайный мод!")
                
    elif data == 'back':
        logger.debug("Нажата кнопка 'Назад' — возврат в главное меню")

        # Создаём полный фейковый update, совместимый с start()
        class FakeUpdate:
            def __init__(self, query):
                self.effective_user = query.from_user
                self.effective_chat = query.message.chat
                self.message = query.message  # с reply_text и т.д.

        fake_update = FakeUpdate(query)
        await start(fake_update, context)
        await query.answer("Возвращаемся в главное меню!")
                
    elif data == 'leaderboard':
        """Обработчик показа рейтинга моддеров с полной безопасностью MarkdownV2"""
        
        # Получаем топ-10 моддеров по количеству лайков
        async with db_lock:
            with sqlite3.connect(POSTS_DB_FILE) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT user_id, COUNT(*) as likes_count
                    FROM reactions 
                    WHERE reaction = 1 
                    GROUP BY user_id 
                    ORDER BY likes_count DESC 
                    LIMIT 10
                """)
                top_modders = cursor.fetchall()
        
        # Проверяем наличие данных в рейтинге
        if not top_modders:
            empty_leaderboard_msg = (
                "🏆 **ТОП-10 МОДДЕРОВ**\n\n"
                "📭 Рейтинг пока пуст!\n"
                "Станьте первым, кто получит лайки от сообщества!"
            )
            keyboard = [[InlineKeyboardButton("🔙 Назад в меню", callback_data='back')]]
            await query.message.reply_text(
                text=empty_leaderboard_msg,
                parse_mode='MarkdownV2',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        # Функция для безопасного экранирования текста под MarkdownV2
        def escape_markdown_v2(text: str) -> str:
            """Экранирует все специальные символы MarkdownV2"""
            special_chars = r'_*[]()~`>#+-=|{}.!'
            for char in special_chars:
                text = text.replace(char, f'\\{char}')
            return text
        
        # Формируем заголовок рейтинга
        leaderboard_text = "🏆 **ТОП\\-10 МОДДЕРОВ**\n\n"
        
        # Формируем список пользователей с безопасным экранированием
        for position, (user_id, likes) in enumerate(top_modders, 1):
            try:
                # Получаем информацию о пользователе
                user = await context.bot.get_chat(user_id)
                first_name = user.first_name or "Аноним"
                
                # Безопасное экранирование имени пользователя
                safe_name = escape_markdown_v2(first_name)
                leaderboard_text += f"{position}\\. **{safe_name}** — ❤️ {likes}\n"
                
            except Exception as e:
                logger.warning(f"❌ Не удалось получить данные пользователя {user_id}: {e}")
                leaderboard_text += f"{position}\\. *Неизвестный пользователь* — ❤️ {likes}\n"
        
        # Формируем клавиатуру с кнопкой возврата
        keyboard = [
            [InlineKeyboardButton("🔙 Назад в меню", callback_data='back')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Отправляем рейтинг с резервным вариантом при ошибке
        try:
            await query.message.reply_text(
                text=leaderboard_text,
                parse_mode='MarkdownV2',
                reply_markup=reply_markup
            )
            logger.debug("✅ Рейтинг успешно отправлен")
            
        except Exception as send_error:
            logger.error(f"❌ Ошибка при отправке рейтинга: {send_error}")
            
            # Резервный вариант: отправляем без Markdown
            fallback_text = (
                "🏆 ТОП-10 МОДДЕРОВ\n\n"
                + "\n".join([
                    f"{i+1}. {name} — ❤️ {likes}" 
                    for i, (_, _, name, likes) in enumerate([
                        (uid, lks, 
                         (await context.bot.get_chat(uid)).first_name if uid else "Аноним", 
                         lks) 
                        for uid, lks in top_modders
                    ])
                ])
            )
            
            await query.message.reply_text(
                text=fallback_text,
                reply_markup=reply_markup
            )
            logger.debug("✅ Рейтинг отправлен (резервный вариант без разметки)")
        
    elif data.startswith('similar_'):
        post_id = int(data[8:])
        async with db_lock:
            with sqlite3.connect(POSTS_DB_FILE) as conn:
                c = conn.cursor()
                c.execute("SELECT title FROM posts WHERE message_id = ?", (post_id,))
                title = c.fetchone()[0].split()[0]
                c.execute("SELECT message_id, version, mod FROM posts WHERE lower(title) LIKE ? AND message_id != ? LIMIT 3",
                         (f"%{title.lower()}%", post_id))
                rows = c.fetchall()
        
        if rows:
            text = f"**Похожие на {title}:**\n\n"
            for mid, ver, mod in rows:
                link = f"https://t.me/c/{str(CHANNEL_ID)[4:]}/{mid}"
                text += f"• v{ver or '—'} | {mod or '—'} | [Открыть]({link})\n"
            await query.message.reply_text(text, parse_mode='Markdown')
        else:
            await query.answer("Похожих нет!")
            
    elif data.startswith('vote_') and not data.startswith('vote_results'):
        app = data[5:]
        vote = context.bot_data.get('current_vote')
        if vote and app in vote['votes']:
            vote['votes'][app] += 1
            await query.answer(f"Ты проголосовал за {app}!")
    
    elif data.startswith('like_') or data.startswith('dislike_'):
        action, post_id_str = data.split('_')
        post_id = int(post_id_str)
        try:
            new_reaction = 1 if action == 'like' else -1
            updated = await update_reaction(post_id, user_id, new_reaction, query)
            if updated:
                likes, dislikes = get_reaction_counts(post_id)
                keyboard = [
                    [InlineKeyboardButton(f"👍 {likes}", callback_data=f'like_{post_id}'),
                     InlineKeyboardButton(f"👎 {dislikes}", callback_data=f'dislike_{post_id}')]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await retry_with_backoff(
                    context.bot.edit_message_reply_markup(
                        chat_id=query.message.chat_id,
                        message_id=query.message.message_id,
                        reply_markup=reply_markup
                    )
                )
                logger.info(f"Обновлена реакция для поста {post_id} пользователем {user_id}: {action}")
        except Exception as error:
            logger.error(f"Ошибка при обработке реакции для поста {post_id}: {error}")
            await query.answer("Ошибка при обработке реакции.")
    elif data == 'parse_posts' and user_id == MAIN_ADMIN_ID and query.message.chat.type == 'private':
        await parse_posts(query, context)

    elif data == 'add_reaction_buttons' and user_id == MAIN_ADMIN_ID and query.message.chat.type == 'private':
        await add_reaction_buttons(query, context)

    elif data == 'show_stats' and user_id == MAIN_ADMIN_ID and query.message.chat.type == 'private':
        await show_statistics(query, context, is_admin=True)
    
    elif data == 'cancel_action':
        if 'action' in context.user_data:
            del context.user_data['action']
        await query.edit_message_reply_markup(reply_markup=None)
        await query.answer("Действие отменено.")

    elif data == 'show_stats_user':
        await show_statistics(query, context, is_admin=False)

    else:
        await query.message.reply_text(MESSAGES[lang]['admin_only'])

async def add_reaction_buttons(query: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = query.from_user.id
    lang = get_user_language(user_id)
    logger.info(f"Главный админ {user_id} начал добавление кнопок реакций для постов канала")
    logger.debug("Проверка прав доступа")

    try:
        channel = await context.bot.get_chat(CHANNEL_ID)
        channel_id = channel.id
        chat_member = await context.bot.get_chat_member(chat_id=channel_id, user_id=(await context.bot.get_me()).id)
        if chat_member.status not in ['administrator', 'creator']:
            logger.error(f"Бот не имеет прав администратора в канале {channel_id}")
            await query.message.reply_text(MESSAGES[lang]['no_admin_rights'])
            return
    except Exception as error:
        logger.error(f"Ошибка при получении ID канала: {error}")
        await query.message.reply_text(MESSAGES[lang]['channel_id_error'])
        return

    count_added = 0
    count_skipped = 0
    message_id = START_MESSAGE_ID
    logger.debug(f"Начало проверки постов в канале с ID {message_id}")

    while True:
        try:
            message = await retry_with_backoff(
                context.bot.forward_message(
                    chat_id=MAIN_ADMIN_ID,
                    from_chat_id=channel_id,
                    message_id=message_id
                )
            )
            text = message.text or message.caption or ''
            has_document = bool(message.document)
            has_photo = bool(message.photo)
            is_document_only = not text and has_document and not has_photo

            if is_document_only:
                logger.debug(f"Проверка поста ID {message_id} на наличие кнопок")
                try:
                    likes, dislikes = get_reaction_counts(message_id)
                    keyboard = [
                        [InlineKeyboardButton(f"👍 {likes}", callback_data=f'like_{message_id}'),
                         InlineKeyboardButton(f"👎 {dislikes}", callback_data=f'dislike_{message_id}')]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await retry_with_backoff(
                        context.bot.edit_message_reply_markup(
                            chat_id=channel_id,
                            message_id=message_id,
                            reply_markup=reply_markup
                        )
                    )
                    count_added += 1
                    logger.info(f"Добавлены кнопки лайк/дизлайк для поста ID {message_id}")
                except Exception as error:
                    if 'message is not modified' in str(error).lower():
                        logger.debug(f"Пропущен пост ID {message_id}: кнопки уже существуют")
                    else:
                        logger.error(f"Ошибка при добавлении кнопок для поста ID {message_id}: {error}")
                    count_skipped += 1
            else:
                logger.debug(f"Пропущен пост ID {message_id}: не только документ")
                count_skipped += 1

            await context.bot.delete_message(chat_id=MAIN_ADMIN_ID, message_id=message.message_id)
            message_id += 1
            await asyncio.sleep(0.1)
        except Exception as error:
            if 'message to forward not found' in str(error).lower() or 'MESSAGE_ID_INVALID' in str(error).upper():
                count_skipped += 1
                if count_skipped >= MAX_SKIPPED_IDS:
                    logger.info(f"Достигнут конец постов на ID {message_id} после {count_skipped} пропущенных ID")
                    break
                logger.warning(f"Пропущен пост ID {message_id}: сообщение не найдено")
                message_id += 1
                await asyncio.sleep(0.05)
                continue
            elif 'The message can\'t be forwarded' in str(error):
                logger.warning(f"Пропущен пост ID {message_id}: сообщение не может быть пересыллено")
                message_id += 1
                count_skipped += 1
                if count_skipped >= MAX_SKIPPED_IDS:
                    logger.info(f"Достигнут конец постов на ID {message_id} после {count_skipped} пропущенных ID")
                    break
                await asyncio.sleep(0.05)
                continue
            logger.error(f"Ошибка при обработке поста ID {message_id}: {error}")
            message_id += 1
            count_skipped += 1
            if message_id > START_MESSAGE_ID + 1000 or count_skipped >= MAX_SKIPPED_IDS:
                logger.warning(f"Достигнут предел проверки на ID {message_id} (пропущено: {count_skipped})")
                break
            await asyncio.sleep(0.1)

    await query.message.reply_text(MESSAGES[lang]['buttons_added'].format(count_added=count_added, count_skipped=count_skipped))
    logger.info(f"Добавление кнопок завершено: добавлено {count_added} кнопок, пропущено {count_skipped} постов")

async def handle_messages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    bot_id = (await context.bot.get_me()).id
    lang = get_user_language(user_id)
    text = update.message.text.lower() if update.message.text else ''
    if update.message.caption:
        text += ' ' + update.message.caption.lower()
    logger.info(f"Обработка сообщения от пользователя {user_id} в чате {chat_id}: {text}")
    logger.debug(f"Полный текст сообщения: {text}")

    if update.message.from_user.id == bot_id:
        logger.debug(f"Игнорировано сообщение от бота {bot_id}")
        return

    if chat_id == CHANNEL_ID:
        logger.debug(f"Игнорировано сообщение из канала {chat_id}")
        return

    if 'action' in context.user_data:
        action = context.user_data['action']
        app_title = text.strip()
        if not app_title:
            await update.message.reply_text(MESSAGES[lang]['empty_app_name'])
            logger.warning(f"Пустое название приложения для действия {action}")
            return

        try:
            async with db_lock:
                with sqlite3.connect(SUBSCRIPTIONS_DB_FILE) as conn:
                    c = conn.cursor()
                    if action == 'subscribe':
                        logger.debug(f"Попытка подписки на {app_title}")
                        c.execute("INSERT OR IGNORE INTO subscriptions (user_id, app_title) VALUES (?, ?)", 
                                  (user_id, app_title))
                        conn.commit()
                        await update.message.reply_text(MESSAGES[lang]['subscribed'].format(app_title=app_title))
                        logger.info(f"Пользователь {user_id} подписался на '{app_title}'")
            del context.user_data['action']
            logger.debug("Действие очищено")
        except Exception as error:
            logger.error(f"Ошибка при выполнении {action} для пользователя {user_id} на '{app_title}': {error}")
            await update.message.reply_text(MESSAGES[lang]['db_error'])
        return

    if text and update.effective_chat.type in ['private', 'group', 'supergroup']:
        logger.info(f"Выполнение поиска по запросу: {text}")
        try:
            async with db_lock:
                with sqlite3.connect(POSTS_DB_FILE) as conn:
                    c = conn.cursor()
                    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='posts'")
                    if not c.fetchone():
                        logger.error("Таблица posts не существует")
                        await update.message.reply_text(MESSAGES[lang]['no_posts_db'])
                        return
                
                words = set(text.split())
                logger.debug(f"Слова для поиска: {words}")
                found_posts = []
                for word in words:
                    if len(word) > 2:
                        logger.debug(f"Поиск по слову: {word}")
                        c.execute("""SELECT title, version, message_id, full_text, media_json, date, mod 
                             FROM posts 
                             WHERE lower(title) LIKE ? OR lower(mod) LIKE ?
                             ORDER BY date DESC 
                             LIMIT 5""",
                          (f'%{word}%', f'%{word}%'))
                        rows = c.fetchall()
                        logger.debug(f"Найдено строк для {word}: {len(rows)}")
                        for row in rows:
                            title, version, mid, full_text, media_json, date, mod = row
                            mod = row[6]  # mod — 7-й столбец (индекс 6)
                            link = f"https://t.me/c/{str(CHANNEL_ID)[4:]}/{mid}"
                            media = json.loads(media_json) if media_json else []
                            found_posts.append({
                                "title": title,
                                "version": version or 'Не указана',
                                "link": link,
                                "mod": mod or 'Не указана',  # ← теперь из БД
                                "media": media,
                                "date": date,
                                "message_id": mid
                            })
                
                seen_links = set()
                unique_posts = [post for post in found_posts if post['link'] not in seen_links and not seen_links.add(post['link'])]
                unique_posts.sort(key=lambda x: x['date'], reverse=True)
                logger.debug(f"Уникальных постов: {len(unique_posts)}")
                
                if unique_posts:
                    response_parts = []
                    keyboard = []
                    for post in unique_posts:
                        part = MESSAGES[lang]['search_results'].format(
                            title=post['title'], version=post['version'], mod=post['mod'] or 'Не указана', link=post['link']
                        )
                        if len(part) > 2000:  # Ограничение длины сообщения Telegram
                            part = part[:1900] + '\n' + MESSAGES[lang]['read_more'].format(link=post['link'])
                        response_parts.append(part)
                        keyboard.append([InlineKeyboardButton(f"📦 {post['version']}", url=post['link'])])
                        
                    if 'message_id' in post:
                        keyboard.append([
                            InlineKeyboardButton("Похожие", callback_data=f'similar_{post["message_id"]}')
                            ])
                    
                    response = "\n".join(response_parts)
                    if len(response) > 4096:
                        response = response[:4000] + '\n' + MESSAGES[lang]['read_more'].format(link=unique_posts[0]['link'])
                        
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    
                    latest_post = unique_posts[0]
                    media_sent = False
                    for m in latest_post['media']:
                        if m['type'] == 'photo':
                            try:
                                await retry_with_backoff(
                                    update.message.reply_photo(
                                        photo=m['file_id'],
                                        caption=response,
                                        reply_markup=reply_markup
                                    )
                                )
                                media_sent = True
                                logger.info(f"Отправлено фото {m['file_id']} из поста {latest_post['link']}")
                                break
                            except Exception as error:
                                logger.error(f"Ошибка при отправке фото {m['file_id']} из поста {latest_post['link']}: {error}")
                    
                    if not media_sent:
                        await update.message.reply_text(response, reply_markup=reply_markup)
                        logger.info(f"Медиа не отправлено для поста {latest_post['link']}")
                    
                    logger.info(f"Найдено {len(unique_posts)} постов по запросу: {text}")
                else:
                    logger.info(f"Результаты не найдены по запросу: {text}")
        except Exception as error:
            logger.error(f"Ошибка при поиске: {error}")

async def notify_subscribers(subscribers, title, version, message_id, context, lang):
    tasks = []
    for sub in subscribers:
        tasks.append(
            retry_with_backoff(
                context.bot.send_message(
                    chat_id=sub[0],
                    text=MESSAGES[lang]['subscribed'].format(app_title=title) + 
                         f"\nВерсия: {version or 'Не указана'}\nhttps://t.me/c/{str(CHANNEL_ID)[4:]}/{message_id}"
                )
            )
        )
    for i in range(0, len(tasks), 30):
        results = await asyncio.gather(*tasks[i:i+30], return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Ошибка при уведомлении подписчика: {result}")

async def parse_posts(query: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = query.from_user.id
    lang = get_user_language(user_id)
    logger.info(f"Главный админ {user_id} начал парсинг постов")
    logger.debug("Проверка прав доступа")
    try:
        channel = await context.bot.get_chat(CHANNEL_ID)
        channel_id = channel.id
        chat_member = await context.bot.get_chat_member(chat_id=channel_id, user_id=(await context.bot.get_me()).id)
        if chat_member.status not in ['administrator', 'creator']:
            logger.error(f"Бот не имеет прав администратора в канале {channel_id}")
            await query.message.reply_text(MESSAGES[lang]['no_admin_rights'])
            return
    except Exception as error:
        logger.error(f"Ошибка при получении ID канала: {error}")
        await query.message.reply_text(MESSAGES[lang]['channel_id_error'])
        return

    logger.debug("Проверка наличия таблицы постов")
    async with db_lock:
        with sqlite3.connect(POSTS_DB_FILE) as conn:
            c = conn.cursor()
            c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='posts'")
            if not c.fetchone():
                logger.error("Таблица posts не существует")
                await query.message.reply_text(MESSAGES[lang]['no_posts_db'])
                return
            c.execute("SELECT MAX(message_id) FROM posts")
            max_mid_tuple = c.fetchone()
            max_mid = max_mid_tuple[0] if max_mid_tuple and max_mid_tuple[0] is not None else 0
            logger.debug(f"Максимальный ID в БД: {max_mid}")

    message_id = max(max_mid + 1, START_MESSAGE_ID)
    count = 0
    skipped_count = 0
    logger.debug(f"Начало парсинга с ID {message_id}")

    while True:
        try:
            # Пересылаем сообщение админу для получения его содержимого
            message = await retry_with_backoff(
                context.bot.forward_message(
                    chat_id=MAIN_ADMIN_ID,
                    from_chat_id=channel_id,
                    message_id=message_id
                )
            )
            logger.debug(f"Обработка сообщения ID {message_id}")

            # Извлекаем данные сообщения
            text = message.text or message.caption or ''
            media = []
            if message.photo:
                media.append({"type": "photo", "file_id": message.photo[-1].file_id})
            if message.document:
                media.append({"type": "document", "file_id": message.document.file_id})
            media_json = json.dumps(media, ensure_ascii=False) if media else ''

            if text:
                lines = [line.strip() for line in text.split('\n') if line.strip()]
                title_line = lines[0] if lines else ''
                title = title_line.split(' - ')[0].strip() if ' - ' in title_line else title_line.split(' ')[0].strip()
                version = ''
                for line in lines:
                    if line.startswith('📦 Версия:'):
                        version_match = re.search(r'Версия:\s*([\w\.\-]+)', line)
                        if version_match:
                            version = version_match.group(1)
                            break
                if not version:
                    logger.warning(f"Пост ID {message_id} ({title}) не содержит версию")
                full_text = text
                date_str = message.date.isoformat()

                mod = extract_mod(full_text)
                save_post_to_db(message_id, title, version, full_text, date_str, media_json, mod)
                count += 1

            # Удаляем пересланное сообщение
            await context.bot.delete_message(chat_id=MAIN_ADMIN_ID, message_id=message.message_id)
            message_id += 1
            await asyncio.sleep(0.1)  # Небольшая задержка для избежания ограничений Telegram
        except Exception as error:
            if 'message to forward not found' in str(error).lower() or 'MESSAGE_ID_INVALID' in str(error).upper():
                skipped_count += 1
                if count > 0 and skipped_count >= MAX_SKIPPED_IDS:
                    logger.info(f"Достигнут конец постов на ID {message_id} после {skipped_count} пропущенных ID")
                    break
                logger.warning(f"Пропущен пост ID {message_id}: сообщение не найдено")
                message_id += 1
                await asyncio.sleep(0.05)
                continue
            elif 'The message can\'t be forwarded' in str(error):
                logger.warning(f"Пропущен пост ID {message_id}: сообщение не может быть пересылено")
                message_id += 1
                skipped_count += 1
                if count > 0 and skipped_count >= MAX_SKIPPED_IDS:
                    logger.info(f"Достигнут конец постов на ID {message_id} после {skipped_count} пропущенных ID")
                    break
                await asyncio.sleep(0.05)
                continue
            logger.error(f"Ошибка при парсинге поста ID {message_id}: {error}")
            message_id += 1
            skipped_count += 1
            if message_id > max_mid + 1000 or (count > 0 and skipped_count >= MAX_SKIPPED_IDS):
                logger.warning(f"Достигнут предел парсинга на ID {message_id} (пропущено: {skipped_count})")
                break
            await asyncio.sleep(0.1)

    await query.message.reply_text(MESSAGES[lang]['parsing_complete'].format(count=count, skipped_count=skipped_count))
    logger.info(f"Парсинг завершён: обработано {count} постов, пропущено {skipped_count} ID")

async def handle_channel_post(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.channel_post and update.effective_chat.id == CHANNEL_ID:
        logger.info(f"Обработка нового поста канала ID {update.channel_post.message_id}")
        try:
            msg = update.channel_post
            message_id = msg.message_id
            text = msg.text or msg.caption or ''
            logger.debug(f"Текст поста: {text}")
            media = []
            if msg.photo:
                media.append({"type": "photo", "file_id": msg.photo[-1].file_id})
            if msg.document:
                media.append({"type": "document", "file_id": msg.document.file_id})
            media_json = json.dumps(media, ensure_ascii=False) if media else ''
            logger.debug(f"JSON медиа: {media_json}")

            is_document_only = not text and msg.document and not msg.photo

            title = ''
            version = ''
            full_text = ''
            date_str = ''
            lang = 'ru'  # По умолчанию для постов канала
            if text or not is_document_only:
                if text:
                    lines = [line.strip() for line in text.split('\n') if line.strip()]
                    title_line = lines[0] if lines else ''
                    title = title_line.split(' - ')[0].strip() if ' - ' in title_line else title_line.split(' ')[0].strip()
                    version = ''
                    for line in lines:
                        if line.startswith('📦 Версия:'):
                            version_match = re.search(r'Версия:\s*([\w\.\-]+)', line)
                            if version_match:
                                version = version_match.group(1)
                                break
                    if not version:
                        logger.warning(f"Новый пост ID {message_id} ({title}) не содержит версию")
                    full_text = text
                    date_str = msg.date.isoformat()

                    try:
                        await retry_with_backoff(
                            context.bot.forward_message(
                                chat_id=MAIN_ADMIN_ID,
                                from_chat_id=msg.chat_id,
                                message_id=message_id
                            )
                        )
                        await context.bot.delete_message(chat_id=MAIN_ADMIN_ID, message_id=message_id)
                        logger.info(f"Переслан и удалён пост ID {message_id} админу")
                    except Exception as error:
                        logger.error(f"Ошибка при пересылке или удалении поста ID {message_id} админу {MAIN_ADMIN_ID}: {error}")

                    mod = extract_mod(full_text)
                    save_post_to_db(message_id, title, version, full_text, date_str, media_json, mod)

                    async with db_lock:
                        with sqlite3.connect(SUBSCRIPTIONS_DB_FILE) as conn:
                            c = conn.cursor()
                            c.execute("SELECT user_id FROM subscriptions WHERE lower(app_title) = ?", (title.lower(),))
                            subscribers = c.fetchall()
                            logger.debug(f"Подписчики для {title}: {len(subscribers)}")
                    await notify_subscribers(subscribers, title, version, message_id, context, lang)
                    logger.info(f"Обработан новый пост: {title} (ID: {message_id}, Версия: {version or 'Не указана'})")

            if is_document_only:
                try:
                    likes, dislikes = get_reaction_counts(message_id)
                    keyboard = [
                        [InlineKeyboardButton(f"👍 {likes}", callback_data=f'like_{message_id}'),
                         InlineKeyboardButton(f"👎 {dislikes}", callback_data=f'dislike_{message_id}')]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await retry_with_backoff(
                        context.bot.edit_message_reply_markup(
                            chat_id=CHANNEL_ID,
                            message_id=message_id,
                            reply_markup=reply_markup
                        )
                    )
                    logger.info(f"Добавлены кнопки лайк/дизлайк для поста ID {message_id}")
                except Exception as error:
                    logger.error(f"Ошибка при добавлении кнопок для поста ID {message_id}: {error}")

        except Exception as error:
            logger.error(f"Ошибка при обработке поста канала ID {message_id}: {error}")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Обновление {update} вызвало ошибку: {context.error}")
    

async def show_statistics(query: Update, context: ContextTypes.DEFAULT_TYPE, is_admin: bool = False) -> None:
    user_id = query.from_user.id
    lang = get_user_language(user_id)
    logger.info(f"Статистика запрошена: user={user_id}, admin={is_admin}")

    try:
        stats = {}

        # === Основные метрики ===
        async with db_lock:
            with sqlite3.connect(SUBSCRIPTIONS_DB_FILE) as conn:
                c = conn.cursor()
                c.execute("SELECT COUNT(DISTINCT user_id) FROM subscriptions")
                stats['total_users'] = c.fetchone()[0] or 0

        with sqlite3.connect(POSTS_DB_FILE) as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM posts")
            stats['total_posts'] = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM posts WHERE date >= datetime('now', '-7 days')")
            stats['posts_last_7d'] = c.fetchone()[0]
            
        # ДЕБАГ: проверим реакции вручную
        with sqlite3.connect(POSTS_DB_FILE) as conn:
            c = conn.cursor()
            c.execute("SELECT post_id, user_id, reaction FROM reactions LIMIT 10")
            reactions = c.fetchall()
            logger.debug(f"ДЕБАГ: реакции в БД: {reactions}")

            c.execute("SELECT message_id, title FROM posts WHERE message_id IN (SELECT post_id FROM reactions)")
            posts_with_reactions = c.fetchall()
            logger.debug(f"ДЕБАГ: посты с реакциями: {posts_with_reactions}")

        async with db_lock:
            with sqlite3.connect(SUBSCRIPTIONS_DB_FILE) as conn:
                c = conn.cursor()
                c.execute("SELECT COUNT(*) FROM subscriptions")
                stats['total_subscriptions'] = c.fetchone()[0]
                if is_admin:
                    c.execute("""
                        SELECT app_title, COUNT(*) as cnt 
                        FROM subscriptions 
                        GROUP BY lower(app_title) 
                        ORDER BY cnt DESC 
                        LIMIT 5
                    """)
                    stats['top_apps_by_subs'] = c.fetchall()

        if is_admin:
            with sqlite3.connect(POSTS_DB_FILE) as conn:
                c = conn.cursor()
                c.execute("SELECT COUNT(*) FROM reactions WHERE reaction = 1")
                stats['total_likes'] = c.fetchone()[0]
                c.execute("SELECT COUNT(*) FROM reactions WHERE reaction = -1")
                stats['total_dislikes'] = c.fetchone()[0]
                c.execute("""
                    SELECT 
                        COALESCE(p.title, 'Пост #' || r.post_id) as title,
                        SUM(CASE WHEN r.reaction = 1 THEN 1 ELSE 0 END) as likes,
                        SUM(CASE WHEN r.reaction = -1 THEN 1 ELSE 0 END) as dislikes
                    FROM reactions r
                    LEFT JOIN posts p ON r.post_id = p.message_id
                    GROUP BY r.post_id
                    ORDER BY (likes + dislikes) DESC
                    LIMIT 5
                """)
                stats['top_rated_posts'] = c.fetchall()
                c.execute("""
                    SELECT title, COUNT(*) as versions
                    FROM posts
                    GROUP BY lower(title)
                    ORDER BY versions DESC
                    LIMIT 5
                """)
                stats['top_apps_by_posts'] = c.fetchall()

        # Размер БД
        stats['db_size_posts'] = f"{os.path.getsize(POSTS_DB_FILE) / 1024 / 1024:.2f} МБ"
        stats['db_size_subs'] = f"{os.path.getsize(SUBSCRIPTIONS_DB_FILE) / 1024 / 1024:.2f} МБ"

        # Аптайм
        start_time = context.application.bot_data.get('start_time', time.time())
        uptime_min = int((time.time() - start_time) / 60)
        stats['uptime'] = f"{uptime_min} мин" if uptime_min < 1440 else f"{uptime_min // 1440}д {uptime_min % 1440 // 60}ч"

        stats['current_time'] = datetime.now().strftime("%d.%m.%Y %H:%M")

        # === Текстовая часть ===
        if is_admin:
            top_subs = "\n".join([f"• {app}: {cnt}" for app, cnt in stats['top_apps_by_subs']]) or "—"
            top_posts = "\n".join([
                f"• **{title[:28]}{'...' if len(title)>28 else ''}**\n   {likes}  {dislikes}"
                for title, likes, dislikes in stats['top_rated_posts']
            ]) or "—"
            top_apps_posts = "\n".join([f"• {app}: {cnt} версий" for app, cnt in stats['top_apps_by_posts']]) or "—"

            text_message = f"""
📊 **Панель администратора**

👥 **Сообщество**
• Всего пользователей: `{stats['total_users']}`
• Активных подписок: `{stats['total_subscriptions']}`

🚀 **Контент**
• Всего постов: `{stats['total_posts']}`
• Новых за неделю: `{stats['posts_last_7d']} 🆕`

❤️ **Реакции**
• Лайков: `{stats['total_likes']} 💚`
• Дизлайков: `{stats['total_dislikes']} 💔`

🏆 **Топ-5 по подпискам**
{top_subs}

📈 **Самые обсуждаемые приложения**
{top_apps_posts}

🔥 **Самые реактивные посты**
{top_posts}

⚙️ **Система**
• БД постов: `{stats['db_size_posts']}`
• БД подписок: `{stats['db_size_subs']}`
• Бот работает: `{stats['uptime']} ⏳`
• Сейчас: `{stats['current_time']}`

👇 Графики ниже — они говорят громче цифр!
""".strip()
        else:
            text_message = f"""
📊 **Статистика бота**

Бот живёт и растёт! Вот что происходит прямо сейчас:

👥 Пользователей: `{stats['total_users']}`
🔔 Активных подписок: `{stats['total_subscriptions']}`

📦 Постов в архиве: `{stats['total_posts']}`
🆕 Новых за неделю: `{stats['posts_last_7d']}`

💾 БД постов: `{stats['db_size_posts']}`
🕒 Обновлено: `{stats['current_time']}`

Спасибо, что ты с нами! ❤️
""".strip()

        keyboard = [[InlineKeyboardButton("Обновить", callback_data='show_stats' if is_admin else 'show_stats_user')]]
        keyboard.append([InlineKeyboardButton("🔙 Назад в меню", callback_data='back')])
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.message.reply_text(text_message, parse_mode='Markdown', reply_markup=reply_markup)

        # === Графики (только для админа) ===
        if not is_admin:
            return
        # Единый стиль для всех графиков
        plt.rcParams.update({
            'font.family': 'sans-serif',
            'font.size': 10,
            'axes.facecolor': '#1e1e1e',
            'axes.edgecolor': '#444',
            'axes.linewidth': 0.8,
            'axes.grid': True,
            'grid.color': '#333',
            'grid.linestyle': '--',
            'grid.alpha': 0.5,
            'figure.facecolor': '#1e1e1e',
            'text.color': 'white',
            'axes.labelcolor': '#ccc',
            'xtick.color': '#aaa',
            'ytick.color': '#aaa',
            'axes.titlelocation': 'left',
            'axes.titlesize': 12,
            'axes.titlepad': 16,
        })

        #plt.style.use('dark_background')
        fig_size = (8, 4.5)

        # 1. Активность постов
        with sqlite3.connect(POSTS_DB_FILE) as conn:
            df = pd.read_sql_query("""
                SELECT date(date) as post_date, COUNT(*) as count
                FROM posts
                WHERE date >= datetime('now', '-7 days')
                GROUP BY date(date)
                ORDER BY post_date
            """, conn)
            df['post_date'] = pd.to_datetime(df['post_date'])

        if not df.empty:
            plt.figure(figsize=fig_size)
            plt.plot(df['post_date'], df['count'], marker='o', color='#00d1b2', linewidth=2.5, markersize=6)
            plt.fill_between(df['post_date'], df['count'], color='#00d1b2', alpha=0.25)
            plt.title('Активность за неделю', fontsize=14, fontweight='bold', color='#00d1b2')
            plt.xlabel('Дата', color='#bbb')
            plt.ylabel('Новых постов', color='#bbb')
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))
            plt.gcf().autofmt_xdate()
            plt.tight_layout()

            bio = BytesIO()
            plt.figtext(
                0.02, 0.02,
                f"Обновлено: {datetime.now().strftime('%d.%m %H:%M')}",
                fontsize=8, color='#666', ha='left'
            )
            plt.savefig(bio, format='png', dpi=150, facecolor='#1e1e1e')
            bio.seek(0)
            plt.close()
            await query.message.reply_photo(photo=bio, caption="📆 *Активность постов за 7 дней*", parse_mode='Markdown', reply_markup=reply_markup)

        # 2. Круговая: подписки
        if stats.get('top_apps_by_subs'):
            labels = [app for app, _ in stats['top_apps_by_subs']]
            sizes = [cnt for _, cnt in stats['top_apps_by_subs']]
            colors = ['#ff6b6b', '#4ecdc4', '#45b7d1', '#f9ca24', '#f3722c']
            plt.figure(figsize=(6, 6))
            wedges, texts, autotexts = plt.pie(
                sizes, labels=labels, autopct='%1.0f%%', 
                colors=colors, startangle=90,
                wedgeprops=dict(width=0.5, edgecolor='white', linewidth=1.2)  # кольцевой стиль + обводка
            )
            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontweight('bold')
            plt.title('Топ-5 по подпискам', color='#ffcc00', fontweight='bold', pad=20)
            plt.tight_layout()

            bio = BytesIO()
            plt.figtext(
                0.02, 0.02,
                f"Обновлено: {datetime.now().strftime('%d.%m %H:%M')}",
                fontsize=8, color='#666', ha='left'
            )
            plt.savefig(bio, format='png', bbox_inches='tight', facecolor='#2b2b2b')
            bio.seek(0)
            plt.close()
            await query.message.reply_photo(photo=bio, caption="📣 *Распределение подписок*", parse_mode='Markdown', reply_markup=reply_markup)

        # 3. Лайки vs Дизлайки
                # 3. Лайки vs Дизлайки
        logger.debug(f"1 Подготовка графика реакций. Данные: {stats.get('top_rated_posts')}")
        top_rated = stats.get('top_rated_posts')
        if top_rated:
            # Фильтруем None и пустые заголовки
            logger.debug(f"2 Подготовка графика реакций. Данные: {stats.get('top_rated_posts')}")
            valid_posts = []
            for row in top_rated:
                title, likes, dislikes = row
                if title is None:
                    title = "Без названия"
                title = str(title).strip()
                if title:  # не пустой
                    valid_posts.append((title, likes or 0, dislikes or 0))
            
            if valid_posts:
                titles = [t[:20] + '...' if len(t) > 20 else t for t, _, _ in valid_posts]
                likes = [l for _, l, _ in valid_posts]
                dislikes = [d for _, _, d in valid_posts]

                plt.figure(figsize=fig_size)
                x = range(len(titles))
                plt.bar([i - 0.2 for i in x], likes, width=0.4, label='Лайки', color='#00d1b2')
                plt.bar([i + 0.2 for i in x], dislikes, width=0.4, label='Дизлайки', color='#ff6b6b')
                plt.xticks(x, titles, rotation=15, ha='right')
                plt.ylabel('Реакций', color='#ccc')
                plt.title('Топ постов по активности', color='#00d1b2', fontweight='bold')
                plt.legend()
                plt.grid(True, alpha=0.3, axis='y')
                plt.figtext(0.02, 0.02, f"Обновлено: {stats['current_time']}", fontsize=8, color='#666', ha='left')
                plt.tight_layout()

                bio = BytesIO()
                plt.savefig(bio, format='png', facecolor='#1e1e1e')
                bio.seek(0)
                plt.close()
                await query.message.reply_photo(
                    photo=bio,
                    caption="🔥 *Топ-5 постов по реакциям*\nСамые обсуждаемые моды этой недели",
                    parse_mode='Markdown',
                    reply_markup=reply_markup
                )

    except Exception as e:
        logger.error(f"Ошибка при генерации статистики: {e}")
        await query.message.reply_text(MESSAGES[lang]['error'])

async def send_daily_report(context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info("Запуск ежедневного отчёта")
    try:
        class FakeQuery:
            def __init__(self):
                self.from_user = type('User', (), {'id': MAIN_ADMIN_ID})
                self.message = type('Message', (), {
                    'chat_id': MAIN_ADMIN_ID,
                    'reply_text': lambda text, **kwargs: context.bot.send_message(
                        chat_id=MAIN_ADMIN_ID, text=text, **kwargs
                    ),
                    'reply_photo': lambda photo, **kwargs: context.bot.send_photo(
                        chat_id=MAIN_ADMIN_ID, photo=photo, **kwargs
                    )
                })

        await show_statistics(FakeQuery(), context, is_admin=True)
        logger.info("Ежедневный отчёт отправлен")
    except Exception as e:
        logger.error(f"Ошибка ежедневного отчёта: {e}")

async def send_mod_of_the_day(context: ContextTypes.DEFAULT_TYPE):
    async with db_lock:
        with sqlite3.connect(POSTS_DB_FILE) as conn:
            c = conn.cursor()
            yesterday = (datetime.now() - timedelta(days=1)).isoformat()
            c.execute("""
                SELECT p.message_id, p.title, p.version, p.mod,
                       COUNT(CASE WHEN r.reaction = 1 THEN 1 END) as likes
                FROM posts p
                LEFT JOIN reactions r ON p.message_id = r.post_id
                WHERE p.date >= ?
                GROUP BY p.message_id
                ORDER BY likes DESC, p.date DESC
                LIMIT 1
            """, (yesterday,))
            row = c.fetchone()
    
    if not row:
        return
    
    mid, title, version, mod, likes = row
    link = f"https://t.me/c/{str(CHANNEL_ID)[4:]}/{mid}"
    text = f"**МОД ДНЯ**\n\n"
    text += f"**{title}**\n"
    text += f"Версия: {version or '—'}\n"
    text += f"Мод: {mod or '—'}\n"
    text += f"❤️ {likes} лайков за сутки!\n\n"
    text += f"[Скачать мод]({link})"
    
    # Рассылка всем пользователям
    with sqlite3.connect(SUBSCRIPTIONS_DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT DISTINCT user_id FROM subscriptions")
        users = c.fetchall()
    
    for user_id, in users[:100]:  # лимит
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=text,
                parse_mode='Markdown',
                disable_web_page_preview=True
            )
        except:
            pass

def main():
    # Создаём приложение
    application = Application.builder().token(TOKEN).build()

    # Добавляем хендлеры ДО инициализации
    application.add_error_handler(error_handler)
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(MessageHandler(filters.Chat(chat_id=CHANNEL_ID), handle_channel_post))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_messages))

    # Время старта
    application.bot_data['start_time'] = time.time()

    # Планировщик
    job_queue = application.job_queue
    if job_queue:
        job_queue.run_daily(
            callback=send_daily_report,
            time=datetime.strptime("09:00", "%H:%M").time(),
            days=(0, 1, 2, 3, 4, 5, 6),
            name="daily_admin_report"
        )
        logger.info("Ежедневный отчёт запланирован на 09:00")
    
    job_queue.run_daily(
        send_mod_of_the_day,
        time=datetime.strptime("12:00", "%H:%M").time(),
        name="mod_of_the_day"
    )
    logger.info("Ежедневный отчёт МОД ДНЯ запланирован на 12:00")

    logger.info("Бот запущен")

    @app.post("/")
async def webhook(request: Request):
    update = Update.de_json(await request.json(), application.bot)
    await application.process_update(update)
    return {"ok": True}

@app.get("/")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    main()  # ← Просто main()
