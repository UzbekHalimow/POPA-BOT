import os
import asyncio
import json
import logging

import aiosqlite
from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message
from prometheus_client import Counter, Gauge, start_http_server

logging.warning("🔧 IMPORTS DONE 🔧")

# Настройки из переменных окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")
INTERVAL_MINUTES_STR = os.getenv("INTERVAL_MINUTES")
logging.warning(f"🌍 ENV: BOT_TOKEN={BOT_TOKEN is not None}, CHANNEL_ID={CHANNEL_ID}, INTERVAL_MINUTES_STR={INTERVAL_MINUTES_STR}")
if INTERVAL_MINUTES_STR is None:
    raise ValueError("INTERVAL_MINUTES must be set in .env")
INTERVAL_MINUTES = int(INTERVAL_MINUTES_STR)

# Список разрешённых пользователей.
# Предпочтительно использовать ALLOWED_USER_IDS=\"id1,id2,id3\".
# Для совместимости остаётся поддержка одиночного ALLOWED_USER_ID.
ALLOWED_USER_IDS_RAW = os.getenv("ALLOWED_USER_IDS")
if ALLOWED_USER_IDS_RAW:
    ALLOWED_USER_IDS = {
        int(part.strip())
        for part in ALLOWED_USER_IDS_RAW.split(",")
        if part.strip()
    }
else:
    single_id = int(os.getenv("ALLOWED_USER_ID", "0"))
    ALLOWED_USER_IDS = {single_id} if single_id != 0 else set()

DB_PATH = os.getenv("DB_PATH", "data/posts.db")

logging.warning("🤖 BOT CREATING...")
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# Временное хранилище для медиа-групп, ожидающих записи
pending_groups: dict[str, dict] = {}
flush_tasks: dict[str, asyncio.Task] = {}  # group_id -> task

logging.warning("✅ BOT CREATED")

# Метрики Prometheus
QUEUE_SIZE_GAUGE = Gauge("bot_queue_size", "Количество постов в очереди на публикацию")
PUBLISHED_POSTS_COUNTER = Counter("bot_published_posts_total", "Количество опубликованных постов")
PUBLISH_ERRORS_COUNTER = Counter("bot_publish_errors_total", "Ошибки при публикации постов")
INCOMING_MESSAGES_COUNTER = Counter("bot_incoming_messages_total", "Обработанные входящие сообщения")

# Проверка доступа
def is_allowed(user_id: int) -> bool:
    return user_id in ALLOWED_USER_IDS

async def _flush_group(group_id: str):
    """После небольшой задержки сохраняем собранную медиа-группу в очередь и отвечаем пользователю."""
    await asyncio.sleep(1.5)  # ждать, пока последующие сообщения группы придут
    data = pending_groups.pop(group_id, None)
    flush_tasks.pop(group_id, None)
    if not data:
        return
    media_list = data["items"]
    msg = data.get("message")
    await add_post_to_queue(media_list)
    if msg:
        try:
            await msg.reply(f"✅ Медиа-группа добавлена в очередь ({len(media_list)} файлов)")
        except Exception:
            pass


# Инициализация БД
async def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                media_json TEXT NOT NULL
            )
        ''')
        await db.commit()

# Добавление поста в очередь
async def add_post_to_queue(media_list):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO posts (media_json) VALUES (?)",
            (json.dumps(media_list),)
        )
        await db.commit()

# Получение следующего поста (и удаление его из очереди)
async def get_next_post():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id, media_json FROM posts ORDER BY id LIMIT 1") as cursor:
            row = await cursor.fetchone()
        if row:
            post_id, media_json = row
            await db.execute("DELETE FROM posts WHERE id = ?", (post_id,))
            await db.commit()
            return json.loads(media_json)
    return None

# Получение всех постов в очереди (без удаления)
async def get_all_posts():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id, media_json FROM posts ORDER BY id") as cursor:
            rows = await cursor.fetchall()
    return [(row[0], json.loads(row[1])) for row in rows]


# Получение конкретного поста по id (без удаления)
async def get_post_by_id(post_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id, media_json FROM posts WHERE id = ?", (post_id,)) as cursor:
            row = await cursor.fetchone()
    if not row:
        return None
    _, media_json = row
    return json.loads(media_json)


def _build_media_group(media_list):
    media_group: list[types.InputMedia] = []
    for item in media_list:
        if item["type"] == "photo":
            media_group.append(types.InputMediaPhoto(media=item["file_id"]))
        elif item["type"] == "video":
            media_group.append(types.InputMediaVideo(media=item["file_id"]))
        elif item["type"] == "animation":
            media_group.append(types.InputMediaAnimation(media=item["file_id"]))
    return media_group


async def send_media_group_to(chat_id: int | str, media_list):
    media_group = _build_media_group(media_list)
    if media_group:
        await bot.send_media_group(chat_id=chat_id, media=media_group)


# Отправка медиагруппы в канал
async def send_media_group(media_list):
    await send_media_group_to(CHANNEL_ID, media_list)

# Фоновая задача публикации
async def scheduled_publisher():
    logging.warning("🔥 START SCHEDULED PUBLISHER 🔥")
    try:
        logging.warning(f"🚀 SCHEDULED PUBLISHER ЗАПУЩЕН с интервалом {INTERVAL_MINUTES} минут")
        iteration = 0
        while True:
            iteration += 1
            try:
                logging.warning(f"⏱️ Итерация #{iteration}: проверка очереди постов...")
                # Логируем количество постов в очереди
                async with aiosqlite.connect(DB_PATH) as db:
                    async with db.execute("SELECT COUNT(*) FROM posts") as cursor:
                        count = await cursor.fetchone()
                        count = count[0] if count else 0

                # обновляем метрику размера очереди
                QUEUE_SIZE_GAUGE.set(count)
                logging.warning(f"📊 Постов в очереди: {count}")
                media_list = await get_next_post()
                if media_list:
                    try:
                        logging.warning(f"📤 Отправка поста с {len(media_list)} медиа...")
                        await send_media_group(media_list)
                        PUBLISHED_POSTS_COUNTER.inc()
                        logging.warning(f"✅ Опубликован пост с {len(media_list)} медиа")
                    except Exception as e:
                        PUBLISH_ERRORS_COUNTER.inc()
                        logging.error(f"❌ Ошибка при публикации: {e}", exc_info=True)
                else:
                    logging.warning("⏸️ Очередь пуста")
                sleep_time = INTERVAL_MINUTES * 60
                logging.warning(f"😴 Спим {INTERVAL_MINUTES} минут ({sleep_time} сек) до следующей проверки...")
                await asyncio.sleep(sleep_time)
            except asyncio.CancelledError:
                logging.warning("📛 Scheduled publisher отменен")
                break
            except Exception as e:
                logging.error(f"❌ Ошибка в итерации {iteration}: {e}", exc_info=True)
                await asyncio.sleep(5)
    except Exception as e:
        logging.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА в scheduled_publisher: {e}", exc_info=True)

# Обработчик одиночных медиа (только для разрешённого пользователя)
# Пропускаем сообщения, относящиеся к медиа-группе — они обрабатываются другим хендлером.
@dp.message(lambda message: (message.photo or message.video or message.animation) and not message.media_group_id)
async def handle_single_media(message: Message):
    INCOMING_MESSAGES_COUNTER.inc()
    if not is_allowed(message.from_user.id):
        await message.reply("⛔ Доступ запрещён.")
        return

    media_list = []
    if message.photo:
        file_id = message.photo[-1].file_id
        media_list.append({"type": "photo", "file_id": file_id})
    elif message.video:
        media_list.append({"type": "video", "file_id": message.video.file_id})
    elif message.animation:
        media_list.append({"type": "animation", "file_id": message.animation.file_id})

    if media_list:
        await add_post_to_queue(media_list)
        # Получаем количество в очереди
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT COUNT(*) FROM posts") as cursor:
                count = await cursor.fetchone()
                count = count[0] if count else 0
        await message.reply(f"✅ Медиа добавлено в очередь. Всего в очереди: {count}")

# Обработчик медиа-группы (несколько файлов в одном сообщении)
@dp.message(lambda message: message.media_group_id is not None)
async def handle_media_group(message: Message):
    INCOMING_MESSAGES_COUNTER.inc()
    if not is_allowed(message.from_user.id):
        await message.reply("⛔ Доступ запрещён.")
        return

    group_id = message.media_group_id
    if group_id not in pending_groups:
        pending_groups[group_id] = {"items": [], "message": message}
    container = pending_groups[group_id]

    if message.photo:
        file_id = message.photo[-1].file_id
        container["items"].append({"type": "photo", "file_id": file_id})
    elif message.video:
        container["items"].append({"type": "video", "file_id": message.video.file_id})
    elif message.animation:
        container["items"].append({"type": "animation", "file_id": message.animation.file_id})

    # запуск отложенной записи, если ещё не создана
    if group_id not in flush_tasks:
        flush_tasks[group_id] = asyncio.create_task(_flush_group(group_id))

# Команда для проверки очереди
@dp.message(Command("queue"))
async def cmd_queue(message: Message):
    if not is_allowed(message.from_user.id):
        await message.reply("⛔ Доступ запрещён.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM posts") as cursor:
            count = await cursor.fetchone()
            count = count[0] if count else 0
    await message.reply(f"📊 В очереди постов: {count}")


@dp.message(Command("queue_list"))
async def cmd_queue_list(message: Message):
    if not is_allowed(message.from_user.id):
        await message.reply("⛔ Доступ запрещён.")
        return

    posts = await get_all_posts()
    if not posts:
        await message.reply("📭 Очередь пуста.")
        return

    lines = []
    for post_id, media_list in posts:
        lines.append(f"ID {post_id}: {len(media_list)} медиа")

    text = "📋 Посты в очереди:\n" + "\n".join(lines)
    # Ограничим длину сообщения на всякий случай
    if len(text) > 3500:
        text = text[:3490] + "\n…"
    await message.reply(text)


@dp.message(Command("queue_show"))
async def cmd_queue_show(message: Message):
    if not is_allowed(message.from_user.id):
        await message.reply("⛔ Доступ запрещён.")
        return

    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.reply("❓ Укажи ID поста: /queue_show <id>")
        return

    try:
        post_id = int(parts[1])
    except ValueError:
        await message.reply("⚠️ ID должен быть числом, пример: /queue_show 5")
        return

    media_list = await get_post_by_id(post_id)
    if not media_list:
        await message.reply(f"📭 Пост с ID {post_id} не найден в очереди.")
        return

    await send_media_group_to(message.chat.id, media_list)
    await message.reply(f"👆 Это содержимое отложенного поста с ID {post_id}")


@dp.message(Command("queue_preview"))
async def cmd_queue_preview(message: Message):
    if not is_allowed(message.from_user.id):
        await message.reply("⛔ Доступ запрещён.")
        return

    posts = await get_all_posts()
    if not posts:
        await message.reply("📭 Очередь пуста.")
        return

    await message.reply(f"🔍 Показываю {len(posts)} пост(ов) из очереди по порядку.")
    for post_id, media_list in posts:
        await send_media_group_to(message.chat.id, media_list)
        await message.answer(f"ID этого поста: {post_id}")

# Команда для тестовой публикации
@dp.message(Command("testpost"))
async def cmd_testpost(message: Message):
    if not is_allowed(message.from_user.id):
        await message.reply("⛔ Доступ запрещён.")
        return

    media_list = await get_next_post()
    if media_list:
        try:
            await send_media_group(media_list)
            await message.reply(f"✅ Тестовый пост опубликован в канал. Медиа: {len(media_list)} шт.")
        except Exception as e:
            await message.reply(f"❌ Ошибка при публикации: {e}")
    else:
        await message.reply("📭 Очередь пуста. Нечего публиковать.")

# Запуск
async def main():
    logging.warning("🚀 MAIN START 🚀")
    # стартуем HTTP-сервер метрик Prometheus
    metrics_port = int(os.getenv("METRICS_PORT", "8000"))
    logging.warning(f"📈 Запуск Prometheus metrics server на порту {metrics_port}")
    start_http_server(metrics_port)
    logging.info("Инициализация БД...")
    await init_db()
    logging.warning("📦 DB INIT DONE")
    logging.info("Запуск фоновой задачи scheduled_publisher...")
    task = asyncio.create_task(scheduled_publisher())
    logging.warning(f"🎯 TASK CREATED: {task}")
    logging.info(f"Фоновая задача создана: {task}")
    retry_delay = 3
    try:
        while True:
            try:
                logging.warning("🔄 START POLLING")
                await dp.start_polling(bot)
                # Если polling завершился штатно — выходим.
                break
            except Exception as e:
                logging.error(f"Ошибка в start_polling: {e}", exc_info=True)
                logging.warning(f"Повторный запуск polling через {retry_delay} сек...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)
    finally:
        task.cancel()

if __name__ == "__main__":
    logging.warning("📜 SCRIPT START 📜")
    try:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        asyncio.run(main())
    except Exception as e:
        logging.warning(f"💥 CRASH: {e}")
        import traceback
        traceback.print_exc()
