"""
Scheduler + Auto-Reaction Telegram Bot (multi-user, dinamik kanallar)
----------------------------------------------------------------------
- CHANNELS yoki SUPERADMIN_ID kabi qattiq (static) ENV yo'q. Kanallar
  DINAMIK ravishda aniqlanadi: botni istalgan kanalga ADMIN qilib
  qo'shsangiz, bot buni avtomatik his qiladi (my_chat_member) va o'sha
  kanalni DB ga (supergroup) qo'shadi.
- Har qanday foydalanuvchi botdan foydalana oladi: /schedule bosilganda
  bot foydalanuvchiga faqat U HAM, BOT HAM admin bo'lgan kanallarni
  ko'rsatadi (get_chat_member orqali tekshiriladi) - ya'ni kimdir
  boshqa birovning kanaliga post rejalashtira olmaydi.
- Bot admin bo'lgan HAR BIR kanaldagi HAR QANDAY post (o'zinikimi,
  boshqasinikimi) ga avtomatik ⚡ reaksiya bosiladi.
- DATABASE: mahalliy fayl EMAS (Render'da disk vaqtinchalik, redeploy'da
  o'chib ketadi). Buning o'rniga alohida Telegram supergroup ishlatiladi:
  bot shu guruhda bitta pinned xabarni (kanallar ro'yxati + rejalar)
  JSON holida tahrirlab boradi. Restart bo'lganda ham get_chat orqali
  pinned xabar o'qilib, holat tiklanadi.
- Webhook orqali ishlaydi (Render uchun moslashtirilgan, aiohttp)

ENV o'zgaruvchilar:
    BOT_TOKEN     - bot tokeni (majburiy)
    WEBHOOK_URL   - https://sizning-domen.onrender.com  (majburiy)
    DB_GROUP_ID   - "database" sifatida ishlatiladigan supergroup ID (majburiy)
                    bot shu guruhda ADMIN bo'lishi va xabar pin qila olishi kerak
    PORT          - (ixtiyoriy, default 8080)
    TIMEZONE      - (ixtiyoriy, default Asia/Tashkent)

requirements.txt:
    aiogram==3.13.1
    APScheduler==3.10.4
    aiohttp

Ishga tushirish (Render uchun): python bot.py
"""

import json
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    ChatMemberUpdated,
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

DB_GROUP_ID = int(os.environ["DB_GROUP_ID"])
PORT = int(os.environ.get("PORT", 8080))
TZ = ZoneInfo(os.environ.get("TIMEZONE", "Asia/Tashkent"))

REACTION_EMOJI = "⚡"
DB_MARKER = "#BOT_DB"
ADMIN_STATUSES = (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR)

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

scheduler = AsyncIOScheduler(timezone=TZ)

# in-memory holat, har doim DB_GROUP dagi pinned xabar bilan sinxron
CHANNELS: dict = {}   # {"<chat_id>": {"title": str}}
JOBS: dict = {}       # {job_id: {...}}
DB_MESSAGE_ID: int | None = None

# ---------------------------------------------------------------------------
# "Database": supergroupdagi pinned xabar orqali saqlash
# ---------------------------------------------------------------------------

def _serialize_state() -> str:
    body = json.dumps({"channels": CHANNELS, "jobs": JOBS}, ensure_ascii=False)
    return f"{DB_MARKER}\n<code>{body}</code>"


def _deserialize_state(text: str) -> tuple[dict, dict]:
    try:
        raw = text.split("\n", 1)[1]
        raw = raw.replace("<code>", "").replace("</code>", "")
        data = json.loads(raw)
        return data.get("channels", {}), data.get("jobs", {})
    except Exception:
        return {}, {}


async def db_load_on_startup() -> None:
    global CHANNELS, JOBS, DB_MESSAGE_ID
    try:
        chat = await bot.get_chat(DB_GROUP_ID)
        pinned = chat.pinned_message
        if pinned and pinned.text and pinned.text.startswith(DB_MARKER):
            CHANNELS, JOBS = _deserialize_state(pinned.text)
            DB_MESSAGE_ID = pinned.message_id
            log.info("DB tiklandi: %d kanal, %d job", len(CHANNELS), len(JOBS))
            return
    except Exception:
        log.exception("DB o'qishda xato, yangi DB xabari yaratiladi")

    msg = await bot.send_message(DB_GROUP_ID, _serialize_state())
    try:
        await bot.pin_chat_message(DB_GROUP_ID, msg.message_id, disable_notification=True)
    except Exception:
        log.exception("Xabarni pin qilishda xato (bot admin emasmi?)")
    DB_MESSAGE_ID = msg.message_id


async def db_save() -> None:
    global DB_MESSAGE_ID
    text = _serialize_state()
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
# Kanallarni dinamik aniqlash: bot biror kanalga admin qilib qo'shilganda
# ---------------------------------------------------------------------------

@router.my_chat_member()
async def on_bot_membership_changed(event: ChatMemberUpdated) -> None:
    if event.chat.type not in ("channel",):
        return
    chat_id = str(event.chat.id)
    new_status = event.new_chat_member.status

    if new_status in ADMIN_STATUSES:
        if chat_id not in CHANNELS:
            CHANNELS[chat_id] = {"title": event.chat.title or chat_id}
            await db_save()
            log.info("Yangi kanal ro'yxatga qo'shildi: %s (%s)", event.chat.title, chat_id)
    else:
        if chat_id in CHANNELS:
            CHANNELS.pop(chat_id, None)
            await db_save()
            log.info("Kanal ro'yxatdan olib tashlandi (bot admin emas): %s", chat_id)

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
# Ruxsat tekshirish: foydalanuvchi shu kanalda ADMIN bo'lishi shart
# ---------------------------------------------------------------------------

async def user_admin_channels(user_id: int) -> dict:
    """Foydalanuvchi ADMIN bo'lgan (va bot ham admin bo'lgan) kanallar."""
    result = {}
    for chat_id_str, info in CHANNELS.items():
        try:
            member = await bot.get_chat_member(int(chat_id_str), user_id)
            if member.status in ADMIN_STATUSES:
                result[chat_id_str] = info
        except Exception:
            continue
    return result

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


def channels_kb(channels: dict) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=info["title"], callback_data=f"ch:{chat_id}")]
        for chat_id, info in channels.items()
    ]
    rows.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Tasdiqlash", callback_data="confirm:yes"),
            InlineKeyboardButton(text="❌ Bekor qilish", callback_data="confirm:no"),
        ]
    ])


def job_list_kb(user_id: int) -> InlineKeyboardMarkup:
    rows = []
    for job_id, job in JOBS.items():
        if job.get("created_by") != user_id:
            continue
        title = CHANNELS.get(str(job["target_chat_id"]), {}).get("title", str(job["target_chat_id"]))
        label = f"{job['run_time'][:16].replace('T', ' ')} -> {title}"
        rows.append([
            InlineKeyboardButton(text=label, callback_data="noop"),
            InlineKeyboardButton(text="❌", callback_data=f"del:{job_id}"),
        ])
    rows.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.message(CommandStart())
async def cmd_start(message: Message):
    if message.chat.type != "private":
        return
    await message.answer(
        "Salom! Meni istalgan kanalingizga ADMIN qilib qo'shing, "
        "shundan so'ng o'sha kanalga post rejalashtira olasiz.\n\nNima qilamiz?",
        reply_markup=main_menu_kb(),
    )


@router.callback_query(F.data == "menu:back")
async def menu_back(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Nima qilamiz?", reply_markup=main_menu_kb())
    await callback.answer()


@router.callback_query(F.data == "menu:schedule")
async def menu_schedule(callback: CallbackQuery, state: FSMContext):
    my_channels = await user_admin_channels(callback.from_user.id)
    if not my_channels:
        return await callback.answer(
            "Siz admin bo'lgan va bot ham admin bo'lgan kanal topilmadi. "
            "Avval botni kanalingizga admin qilib qo'shing.",
            show_alert=True,
        )
    await state.set_state(ScheduleStates.choosing_channel)
    await callback.message.edit_text("Qaysi kanalga yuborilsin?", reply_markup=channels_kb(my_channels))
    await callback.answer()


@router.callback_query(F.data == "menu:list")
async def menu_list(callback: CallbackQuery):
    my_jobs = {jid: j for jid, j in JOBS.items() if j.get("created_by") == callback.from_user.id}
    if not my_jobs:
        await callback.message.edit_text(
            "Sizda rejalashtirilgan post yo'q.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:back")]
            ]),
        )
    else:
        await callback.message.edit_text("Rejalashtirilgan postlaringiz:", reply_markup=job_list_kb(callback.from_user.id))
    await callback.answer()


@router.callback_query(F.data.startswith("del:"))
async def delete_job(callback: CallbackQuery):
    job_id = callback.data.split(":", 1)[1]
    job = JOBS.get(job_id)
    if job and job.get("created_by") == callback.from_user.id:
        JOBS.pop(job_id, None)
        await db_save()
        try:
            scheduler.remove_job(job_id)
        except Exception:
            pass

    my_jobs = {jid: j for jid, j in JOBS.items() if j.get("created_by") == callback.from_user.id}
    if my_jobs:
        await callback.message.edit_text("Rejalashtirilgan postlaringiz:", reply_markup=job_list_kb(callback.from_user.id))
    else:
        await callback.message.edit_text(
            "Sizda rejalashtirilgan post yo'q.",
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
    # xavfsizlik: foydalanuvchi haqiqatan ham shu kanalda admin ekanini qayta tekshiramiz
    try:
        member = await bot.get_chat_member(channel_id, callback.from_user.id)
        if member.status not in ADMIN_STATUSES:
            return await callback.answer("Siz bu kanalda admin emassiz.", show_alert=True)
    except Exception:
        return await callback.answer("Kanalni tekshirib bo'lmadi.", show_alert=True)

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
    title = CHANNELS.get(str(data["target_chat_id"]), {}).get("title", str(data["target_chat_id"]))
    await state.set_state(ScheduleStates.confirming)
    await message.answer(
        "Tasdiqlaysizmi?\n"
        f"Kanal: <b>{title}</b>\n"
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
        "created_by": callback.from_user.id,
    }
    await db_save()
    schedule_job(job_id, run_time)

    await callback.message.edit_text(
        f"✅ Rejalashtirildi.\nID: <code>{job_id}</code>",
        reply_markup=main_menu_kb(),
    )
    await callback.answer()

# ---------------------------------------------------------------------------
# Avtomatik reaksiya: har bir (dinamik) kanaldagi HAR QANDAY postga ⚡
# ---------------------------------------------------------------------------

@router.channel_post()
async def auto_react(message: Message):
    if str(message.chat.id) not in CHANNELS:
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
