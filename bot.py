"""
Scheduler + Auto-Reaction Telegram Bot
---------------------------------------
- Barcha boshqaruv INLINE tugmalar orqali (buyruq yozish shart emas)
- /schedule oqimi: kanal tanlash -> kontent yuborish -> vaqt kiritish -> inline tasdiqlash
- Belgilangan vaqtda xabar kanalga copy_message orqali yuboriladi
- Bot admin bo'lgan HAR BIR kanaldagi HAR QANDAY post (o'zinikimi, boshqasinikimi)
  ga avtomatik ⚡ reaksiya bosiladi
- DATABASE: mahalliy fayl EMAS (Render'da disk vaqtinchalik, redeploy'da o'chib ketadi).
  Buning o'rniga alohida Telegram supergroup ishlatiladi: bot shu guruhda bitta
  pinned xabarni JOB'lar bilan tahrirlab boradi. Restart/redeploy bo'lganda ham
  get_chat orqali pinned xabar o'qilib, holat tiklanadi.
- Webhook orqali ishlaydi (Render uchun moslashtirilgan, aiohttp)

ENV o'zgaruvchilar:
    BOT_TOKEN        - bot tokeni (majburiy)
    WEBHOOK_URL       - https://sizning-domen.onrender.com  (majburiy)
    CHANNELS          - reaksiya bosiladigan / post qilinadigan kanal ID lari, vergul bilan
                        masalan: -1001234567890,-1009876543210 (majburiy)
    SUPERADMIN_ID     - botni boshqaradigan foydalanuvchi Telegram ID (majburiy)
    DB_GROUP_ID       - "database" sifatida ishlatiladigan supergroup ID (majburiy)
                        bot shu guruhda ADMIN bo'lishi va xabar pin qila olishi kerak
    PORT              - (ixtiyoriy, default 8080)
    TIMEZONE          - (ixtiyoriy, default Asia/Tashkent)

requirements.txt:
    aiogram==3.13.1
    APScheduler==3.10.4
    aiohttp

Ishga tushirish (Render uchun): python bot.py
"""

import asyncio
import json
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    ReactionTypeEmoji,
)
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger

# ---------------------------------------------------------------------------
# Sozlamalar
# ---------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("scheduler-bot")

BOT_TOKEN = os.environ["BOT_TOKEN"]
WEBHOOK_URL = os.environ["WEBHOOK_URL"].rstrip("/")
WEBHOOK_PATH = "/webhook"
FULL_WEBHOOK_URL = f"{WEBHOOK_URL}{WEBHOOK_PATH}"

CHANNELS = [int(x.strip()) for x in os.environ["CHANNELS"].split(",") if x.strip()]
SUPERADMIN_ID = int(os.environ["SUPERADMIN_ID"])
DB_GROUP_ID = int(os.environ["DB_GROUP_ID"])
PORT = int(os.environ.get("PORT", 8080))
TZ = ZoneInfo(os.environ.get("TIMEZONE", "Asia/Tashkent"))

REACTION_EMOJI = "⚡"
DB_MARKER = "#JOBS_DB"  # pinned xabarni topish/aniqlash uchun belgi

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

scheduler = AsyncIOScheduler(timezone=TZ)

# in-memory holat (har doim DB_GROUP dagi pinned xabar bilan sinxron saqlanadi)
JOBS: dict = {}
DB_MESSAGE_ID: int | None = None

# ---------------------------------------------------------------------------
# "Database": supergroupdagi pinned xabar orqali saqlash
# ---------------------------------------------------------------------------

def _serialize_jobs() -> str:
    body = json.dumps(JOBS, ensure_ascii=False)
    return f"{DB_MARKER}\n<code>{body}</code>"


def _deserialize_jobs(text: str) -> dict:
    try:
        raw = text.split("\n", 1)[1]
        raw = raw.replace("<code>", "").replace("</code>", "")
        return json.loads(raw)
    except Exception:
        return {}


async def db_load_on_startup() -> None:
    """Bot ishga tushganda DB_GROUP dagi pinned xabarni o'qib, holatni tiklaydi."""
    global JOBS, DB_MESSAGE_ID
    try:
        chat = await bot.get_chat(DB_GROUP_ID)
        pinned = chat.pinned_message
        if pinned and pinned.text and pinned.text.startswith(DB_MARKER):
            JOBS = _deserialize_jobs(pinned.text)
            DB_MESSAGE_ID = pinned.message_id
            log.info("DB tiklandi: %d ta job topildi", len(JOBS))
            return
    except Exception:
        log.exception("DB o'qishda xato, yangi DB xabari yaratiladi")

    # pinned xabar topilmadi -> yangisini yaratamiz
    msg = await bot.send_message(DB_GROUP_ID, _serialize_jobs())
    try:
        await bot.pin_chat_message(DB_GROUP_ID, msg.message_id, disable_notification=True)
    except Exception:
        log.exception("Xabarni pin qilishda xato (bot admin emasmi?)")
    DB_MESSAGE_ID = msg.message_id


async def db_save() -> None:
    """JOBS o'zgarganda DB_GROUP dagi pinned xabarni yangilaydi."""
    global DB_MESSAGE_ID
    text = _serialize_jobs()
    if DB_MESSAGE_ID is None:
        msg = await bot.send_message(DB_GROUP_ID, text)
        DB_MESSAGE_ID = msg.message_id
        try:
            await bot.pin_chat_message(DB_GROUP_ID, msg.message_id, disable_notification=True)
        except Exception:
            log.exception("Pin qilishda xato")
        return
    try:
        await bot.edit_message_text(chat_id=DB_GROUP_ID, message_id=DB_MESSAGE_ID, text=text)
    except Exception:
        log.exception("DB xabarini yangilashda xato, yangi xabar yaratilmoqda")
        msg = await bot.send_message(DB_GROUP_ID, text)
        DB_MESSAGE_ID = msg.message_id
        try:
            await bot.pin_chat_message(DB_GROUP_ID, msg.message_id, disable_notification=True)
        except Exception:
            pass

# ---------------------------------------------------------------------------
# Xabarni yuborish (scheduled ish)
# ---------------------------------------------------------------------------

async def send_scheduled_post(job_id: str) -> None:
    job = JOBS.get(job_id)
    if not job:
        return
    try:
        await bot.copy_message(
            chat_id=job["target_chat_id"],
            from_chat_id=job["from_chat_id"],
            message_id=job["message_id"],
        )
        log.info("Job %s yuborildi -> %s", job_id, job["target_chat_id"])
    except Exception:
        log.exception("Job %s yuborishda xato", job_id)
    finally:
        JOBS.pop(job_id, None)
        await db_save()


def schedule_job(job_id: str, run_time: datetime) -> None:
    scheduler.add_job(
        send_scheduled_post,
        trigger=DateTrigger(run_date=run_time, timezone=TZ),
        args=[job_id],
        id=job_id,
        replace_existing=True,
    )


def restore_scheduler_jobs() -> None:
    now = datetime.now(TZ)
    expired = []
    for job_id, job in list(JOBS.items()):
        run_time = datetime.fromisoformat(job["run_time"])
        if run_time <= now:
            expired.append(job_id)
            continue
        schedule_job(job_id, run_time)
    for job_id in expired:
        JOBS.pop(job_id, None)

# ---------------------------------------------------------------------------
# FSM: /schedule oqimi
# ---------------------------------------------------------------------------

class ScheduleStates(StatesGroup):
    choosing_channel = State()
    waiting_content = State()
    waiting_datetime = State()
    confirming = State()


def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Rejalashtirish", callback_data="menu:schedule")],
        [InlineKeyboardButton(text="📋 Ro'yxat", callback_data="menu:list")],
    ])


def channels_kb() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=str(ch), callback_data=f"ch:{ch}")] for ch in CHANNELS]
    rows.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Tasdiqlash", callback_data="confirm:yes"),
            InlineKeyboardButton(text="❌ Bekor qilish", callback_data="confirm:no"),
        ]
    ])


def job_list_kb() -> InlineKeyboardMarkup:
    rows = []
    for job_id, job in JOBS.items():
        label = f"{job['run_time'][:16].replace('T', ' ')} -> {job['target_chat_id']}"
        rows.append([
            InlineKeyboardButton(text=label, callback_data="noop"),
            InlineKeyboardButton(text="❌", callback_data=f"del:{job_id}"),
        ])
    rows.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def is_superadmin(user_id: int | None) -> bool:
    return user_id == SUPERADMIN_ID


@router.message(CommandStart())
async def cmd_start(message: Message):
    if message.chat.type != "private" or not is_superadmin(message.from_user.id):
        return
    await message.answer("Salom! Nima qilamiz?", reply_markup=main_menu_kb())


@router.callback_query(F.data == "menu:back")
async def menu_back(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Nima qilamiz?", reply_markup=main_menu_kb())
    await callback.answer()


@router.callback_query(F.data == "menu:schedule")
async def menu_schedule(callback: CallbackQuery, state: FSMContext):
    if not is_superadmin(callback.from_user.id):
        return await callback.answer()
    if not CHANNELS:
        return await callback.answer("CHANNELS sozlanmagan.", show_alert=True)
    await state.set_state(ScheduleStates.choosing_channel)
    await callback.message.edit_text("Qaysi kanalga yuborilsin?", reply_markup=channels_kb())
    await callback.answer()


@router.callback_query(F.data == "menu:list")
async def menu_list(callback: CallbackQuery):
    if not is_superadmin(callback.from_user.id):
        return await callback.answer()
    if not JOBS:
        await callback.message.edit_text(
            "Rejalashtirilgan post yo'q.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:back")]
            ]),
        )
    else:
        await callback.message.edit_text("Rejalashtirilgan postlar:", reply_markup=job_list_kb())
    await callback.answer()


@router.callback_query(F.data.startswith("del:"))
async def delete_job(callback: CallbackQuery):
    if not is_superadmin(callback.from_user.id):
        return await callback.answer()
    job_id = callback.data.split(":", 1)[1]
    if job_id in JOBS:
        JOBS.pop(job_id)
        await db_save()
        try:
            scheduler.remove_job(job_id)
        except Exception:
            pass
    if JOBS:
        await callback.message.edit_text("Rejalashtirilgan postlar:", reply_markup=job_list_kb())
    else:
        await callback.message.edit_text(
            "Rejalashtirilgan post yo'q.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:back")]
            ]),
        )
    await callback.answer("O'chirildi.")


@router.callback_query(F.data == "noop")
async def noop(callback: CallbackQuery):
    await callback.answer()


@router.callback_query(ScheduleStates.choosing_channel, F.data.startswith("ch:"))
async def choose_channel(callback: CallbackQuery, state: FSMContext):
    channel_id = int(callback.data.split(":", 1)[1])
    await state.update_data(target_chat_id=channel_id)
    await state.set_state(ScheduleStates.waiting_content)
    await callback.message.edit_text(
        "Endi menga yubormoqchi bo'lgan xabaringizni yuboring "
        "(matn, rasm, video, hujjat - qanday bo'lsa shunday ketadi)."
    )
    await callback.answer()


@router.message(ScheduleStates.waiting_content)
async def receive_content(message: Message, state: FSMContext):
    await state.update_data(from_chat_id=message.chat.id, message_id=message.message_id)
    await state.set_state(ScheduleStates.waiting_datetime)
    await message.answer(
        "Qachon yuborilsin? Formatda yozing:\n"
        "<code>YYYY-MM-DD HH:MM</code>\n"
        f"Masalan: <code>2026-07-10 18:30</code> ({TZ.key} vaqti bo'yicha)"
    )


@router.message(ScheduleStates.waiting_datetime)
async def receive_datetime(message: Message, state: FSMContext):
    text = message.text.strip() if message.text else ""
    try:
        run_time = datetime.strptime(text, "%Y-%m-%d %H:%M").replace(tzinfo=TZ)
    except ValueError:
        await message.answer("Format noto'g'ri. Masalan: 2026-07-10 18:30 shaklida yuboring.")
        return
    if run_time <= datetime.now(TZ):
        await message.answer("Bu vaqt allaqachon o'tib ketgan. Kelajakdagi vaqt kiriting.")
        return

    await state.update_data(run_time=run_time.isoformat())
    data = await state.get_data()
    await state.set_state(ScheduleStates.confirming)
    await message.answer(
        "Tasdiqlaysizmi?\n"
        f"Kanal: <code>{data['target_chat_id']}</code>\n"
        f"Vaqt: <code>{run_time.strftime('%Y-%m-%d %H:%M')}</code>",
        reply_markup=confirm_kb(),
    )


@router.callback_query(ScheduleStates.confirming, F.data.startswith("confirm:"))
async def confirm_schedule(callback: CallbackQuery, state: FSMContext):
    action = callback.data.split(":", 1)[1]
    data = await state.get_data()
    await state.clear()

    if action == "no":
        await callback.message.edit_text("Bekor qilindi.", reply_markup=main_menu_kb())
        return await callback.answer()

    run_time = datetime.fromisoformat(data["run_time"])
    job_id = f"job_{int(run_time.timestamp())}_{data['message_id']}"
    JOBS[job_id] = {
        "target_chat_id": data["target_chat_id"],
        "from_chat_id": data["from_chat_id"],
        "message_id": data["message_id"],
        "run_time": data["run_time"],
    }
    await db_save()
    schedule_job(job_id, run_time)

    await callback.message.edit_text(
        f"✅ Rejalashtirildi.\nID: <code>{job_id}</code>",
        reply_markup=main_menu_kb(),
    )
    await callback.answer()

# ---------------------------------------------------------------------------
# Avtomatik reaksiya: har bir kanaldagi HAR QANDAY postga ⚡
# ---------------------------------------------------------------------------

@router.channel_post()
async def auto_react(message: Message):
    if message.chat.id not in CHANNELS:
        return
    try:
        await bot.set_message_reaction(
            chat_id=message.chat.id,
            message_id=message.message_id,
            reaction=[ReactionTypeEmoji(emoji=REACTION_EMOJI)],
        )
    except Exception:
        log.exception("Reaksiya bosishda xato: chat=%s msg=%s", message.chat.id, message.message_id)

# ---------------------------------------------------------------------------
# Webhook / aiohttp server
# ---------------------------------------------------------------------------

async def health(request: web.Request) -> web.Response:
    return web.Response(text="ok")


async def on_startup(app: web.Application) -> None:
    await bot.set_webhook(FULL_WEBHOOK_URL, drop_pending_updates=True)
    await db_load_on_startup()
    restore_scheduler_jobs()
    scheduler.start()
    log.info("Webhook o'rnatildi: %s", FULL_WEBHOOK_URL)


async def on_shutdown(app: web.Application) -> None:
    await bot.delete_webhook()
    scheduler.shutdown(wait=False)


def main() -> None:
    app = web.Application()
    app.router.add_get("/health", health)
    SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    web.run_app(app, host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
