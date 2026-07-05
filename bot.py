"""
Scheduler + Auto-Reaction Telegram Bot (multi-user, dinamik kanallar)
----------------------------------------------------------------------
- Kanallar DINAMIK aniqlanadi: botni istalgan kanalga admin qilib
  qo'shsangiz, bot avtomatik his qiladi (my_chat_member) va DB ga
  (supergroup) qo'shadi.
- Kanal qo'shilganda, qo'shgan odamdan ikkita narsa so'raladi:
    1) Reklama tarmog'iga qo'shilishga ROZIMI (ixtiyoriy, Ha/Yo'q) -
       rozi bo'lsa ham, bo'lmasa ham kanal baribir ro'yxatga qo'shiladi,
       farqi faqat reklama tarmog'idan xabar olish-olmaslikda.
    2) Bu kanal uchun qaysi reaksiya qo'llanilsin (inline tanlagich).
  Ikkalasini ham istalgan payt "Kanallarim" bo'limidan o'zgartirish mumkin.
- Har qanday foydalanuvchi botdan foydalana oladi, LEKIN faqat O'ZI ADMIN
  bo'lgan (va bot ham admin bo'lgan) kanallar bilan ishlay oladi -
  get_chat_member orqali har doim tekshiriladi.
- "Reklama tarmog'i" - FAQAT reklamaga ROZILIK bergan kanallarga xabar
  yuboradi (ixtiyoriy/consensual reklama tarmog'i - Telegram Ads kabi
  mantiq: kanal egasi ochiq roziligi bilan).
- Bot admin bo'lgan har bir kanaldagi HAR QANDAY post (o'zinikimi,
  boshqasinikimi) ga o'sha kanal uchun tanlangan reaksiya avtomatik bosiladi.
- "📊 Statistika" - o'z kanallaringiz bo'yicha obunachilar soni
  (get_chat_member_count). ESLATMA: Bot API premium obunachilar sonini
  bermaydi - bu faqat Telegram ilovasidagi kanal "Statistics" bo'limida
  (500+ obunachi bo'lganda) ko'rinadi, botlar orqali olib bo'lmaydi.
- DATABASE: mahalliy fayl EMAS (Render'da disk vaqtinchalik). Buning
  o'rniga alohida Telegram supergroup ishlatiladi - pinned xabar JSON
  holida (kanallar + reaksiya + reklama roziligi + rejalar) saqlanadi.
- Webhook orqali ishlaydi (Render uchun moslashtirilgan, aiohttp)

ENV o'zgaruvchilar:
    BOT_TOKEN, WEBHOOK_URL, DB_GROUP_ID (majburiy)
    PORT, TIMEZONE (ixtiyoriy)

requirements.txt:
    aiogram==3.13.1
    APScheduler==3.10.4
    aiohttp
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

DB_MARKER = "#BOT_DB"
ADMIN_STATUSES = (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR)
DEFAULT_REACTION = "⚡"

REACTIONS = [
    "⚡", "👍", "👎", "❤️", "🔥", "🥰", "👏", "😁",
    "🤔", "🎉", "🙏", "👌", "💯", "🤣", "😢", "🤯",
]

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

scheduler = AsyncIOScheduler(timezone=TZ)

# {"<chat_id>": {"title": str, "reaction": str, "added_by": int, "ads_consent": bool}}
CHANNELS: dict = {}
JOBS: dict = {}
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
# Kanal qo'shilganda: reklama roziligi so'raladi, keyin reaksiya tanlagichi
# ---------------------------------------------------------------------------

def reaction_picker_kb(chat_id: str) -> InlineKeyboardMarkup:
    rows, row = [], []
    for idx, emoji in enumerate(REACTIONS):
        row.append(InlineKeyboardButton(text=emoji, callback_data=f"setreact:{chat_id}:{idx}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def ads_consent_kb(chat_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Roziman", callback_data=f"adsconsent:{chat_id}:yes"),
            InlineKeyboardButton(text="❌ Yo'q", callback_data=f"adsconsent:{chat_id}:no"),
        ]
    ])


@router.my_chat_member()
async def on_bot_membership_changed(event: ChatMemberUpdated) -> None:
    if event.chat.type != "channel":
        return
    chat_id = str(event.chat.id)
    new_status = event.new_chat_member.status

    if new_status in ADMIN_STATUSES:
        if chat_id not in CHANNELS:
            CHANNELS[chat_id] = {
                "title": event.chat.title or chat_id,
                "reaction": DEFAULT_REACTION,
                "added_by": event.from_user.id if event.from_user else None,
                "ads_consent": False,
            }
            await db_save()
            log.info("Yangi kanal qo'shildi: %s (%s)", event.chat.title, chat_id)
            if event.from_user:
                try:
                    await bot.send_message(
                        event.from_user.id,
                        f"✅ <b>{event.chat.title}</b> kanaliga admin qilib qo'shildim.\n\n"
                        "Bu kanalni boshqa reklama beruvchilardan (reklama tarmog'i) "
                        "e'lon olishga qo'shishni xohlaysizmi? Istalgan payt "
                        "\"Kanallarim\" bo'limidan o'zgartirishingiz mumkin.",
                        reply_markup=ads_consent_kb(chat_id),
                    )
                except Exception:
                    log.info("DM yuborib bo'lmadi (foydalanuvchi bot bilan /start bosmagan)")
    else:
        if chat_id in CHANNELS:
            CHANNELS.pop(chat_id, None)
            await db_save()
            log.info("Kanal ro'yxatdan olib tashlandi (bot admin emas): %s", chat_id)


@router.callback_query(F.data.startswith("adsconsent:"))
async def set_ads_consent(callback: CallbackQuery):
    _, chat_id, answer = callback.data.split(":", 2)
    try:
        member = await bot.get_chat_member(int(chat_id), callback.from_user.id)
        if member.status not in ADMIN_STATUSES:
            return await callback.answer("Siz bu kanalda admin emassiz.", show_alert=True)
    except Exception:
        return await callback.answer("Kanalni tekshirib bo'lmadi.", show_alert=True)
    if chat_id not in CHANNELS:
        return await callback.answer("Kanal topilmadi.", show_alert=True)

    CHANNELS[chat_id]["ads_consent"] = (answer == "yes")
    await db_save()
    note = "✅ Reklama tarmog'iga qo'shildingiz." if answer == "yes" else "Reklama tarmog'iga qo'shilmadingiz (istasangiz keyin yoqasiz)."
    await callback.message.edit_text(note)

    await bot.send_message(
        callback.from_user.id,
        f"Endi <b>{CHANNELS[chat_id]['title']}</b> uchun qaysi reaksiya qo'llanilsin?",
        reply_markup=reaction_picker_kb(chat_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("setreact:"))
async def set_reaction(callback: CallbackQuery):
    _, chat_id, idx_str = callback.data.split(":", 2)
    try:
        member = await bot.get_chat_member(int(chat_id), callback.from_user.id)
        if member.status not in ADMIN_STATUSES:
            return await callback.answer("Siz bu kanalda admin emassiz.", show_alert=True)
    except Exception:
        return await callback.answer("Kanalni tekshirib bo'lmadi.", show_alert=True)
    if chat_id not in CHANNELS:
        return await callback.answer("Kanal topilmadi.", show_alert=True)

    emoji = REACTIONS[int(idx_str)]
    CHANNELS[chat_id]["reaction"] = emoji
    await db_save()
    await callback.message.edit_text(f"✅ Bu kanal uchun reaksiya: {emoji}")
    await callback.answer("Saqlandi.")

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
# Ruxsat tekshirish
# ---------------------------------------------------------------------------

async def user_admin_channels(user_id: int) -> dict:
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
# FSM
# ---------------------------------------------------------------------------

class ScheduleStates(StatesGroup):
    choosing_channel = State()
    waiting_content = State()
    waiting_datetime = State()
    confirming = State()


class BroadcastStates(StatesGroup):
    selecting_channels = State()
    waiting_content = State()
    confirming = State()


class AdsNetworkStates(StatesGroup):
    waiting_content = State()
    confirming = State()


def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Rejalashtirish", callback_data="menu:schedule")],
        [InlineKeyboardButton(text="📢 Hoziroq yuborish", callback_data="menu:broadcast")],
        [InlineKeyboardButton(text="📣 Reklama tarmog'i", callback_data="menu:adsnetwork")],
        [InlineKeyboardButton(text="📋 Ro'yxat", callback_data="menu:list")],
        [InlineKeyboardButton(text="⚙️ Kanallarim", callback_data="menu:mychannels")],
        [InlineKeyboardButton(text="📊 Statistika", callback_data="menu:stats")],
    ])


def channels_kb(channels: dict, prefix: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=info["title"], callback_data=f"{prefix}:{chat_id}")]
        for chat_id, info in channels.items()
    ]
    rows.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def broadcast_select_kb(channels: dict, selected: list) -> InlineKeyboardMarkup:
    rows = []
    for chat_id, info in channels.items():
        mark = "✅ " if chat_id in selected else ""
        rows.append([InlineKeyboardButton(text=f"{mark}{info['title']}", callback_data=f"bcsel:{chat_id}")])
    rows.append([InlineKeyboardButton(text="➡️ Davom etish", callback_data="bcnext")])
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


def my_channels_kb(channels: dict) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=info["title"], callback_data=f"chdetail:{chat_id}")]
        for chat_id, info in channels.items()
    ]
    rows.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def channel_detail_kb(chat_id: str, info: dict) -> InlineKeyboardMarkup:
    ads_label = "🔔 Reklama: YOQILGAN" if info.get("ads_consent") else "🔕 Reklama: O'CHIRILGAN"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Reaksiya: {info.get('reaction', DEFAULT_REACTION)}", callback_data=f"chreact:{chat_id}")],
        [InlineKeyboardButton(text=ads_label, callback_data=f"adstoggle:{chat_id}")],
        [InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:mychannels")],
    ])


@router.message(CommandStart())
async def cmd_start(message: Message):
    if message.chat.type != "private":
        return
    await message.answer(
        "Salom! Meni istalgan kanalingizga ADMIN qilib qo'shing - "
        "avtomatik tanib olaman.\n\nNima qilamiz?",
        reply_markup=main_menu_kb(),
    )


@router.callback_query(F.data == "menu:back")
async def menu_back(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Nima qilamiz?", reply_markup=main_menu_kb())
    await callback.answer()


@router.callback_query(F.data == "menu:mychannels")
async def menu_mychannels(callback: CallbackQuery):
    my_channels = await user_admin_channels(callback.from_user.id)
    if not my_channels:
        return await callback.answer("Siz admin bo'lgan kanal topilmadi.", show_alert=True)
    await callback.message.edit_text(
        "Kanallaringiz (batafsil sozlash uchun bosing):",
        reply_markup=my_channels_kb(my_channels),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("chdetail:"))
async def channel_detail(callback: CallbackQuery):
    chat_id = callback.data.split(":", 1)[1]
    try:
        member = await bot.get_chat_member(int(chat_id), callback.from_user.id)
        if member.status not in ADMIN_STATUSES:
            return await callback.answer("Siz bu kanalda admin emassiz.", show_alert=True)
    except Exception:
        return await callback.answer("Kanalni tekshirib bo'lmadi.", show_alert=True)
    info = CHANNELS.get(chat_id)
    if not info:
        return await callback.answer("Kanal topilmadi.", show_alert=True)
    await callback.message.edit_text(f"<b>{info['title']}</b>", reply_markup=channel_detail_kb(chat_id, info))
    await callback.answer()


@router.callback_query(F.data.startswith("adstoggle:"))
async def toggle_ads(callback: CallbackQuery):
    chat_id = callback.data.split(":", 1)[1]
    try:
        member = await bot.get_chat_member(int(chat_id), callback.from_user.id)
        if member.status not in ADMIN_STATUSES:
            return await callback.answer("Siz bu kanalda admin emassiz.", show_alert=True)
    except Exception:
        return await callback.answer("Kanalni tekshirib bo'lmadi.", show_alert=True)
    info = CHANNELS.get(chat_id)
    if not info:
        return await callback.answer("Kanal topilmadi.", show_alert=True)
    info["ads_consent"] = not info.get("ads_consent", False)
    await db_save()
    await callback.message.edit_reply_markup(reply_markup=channel_detail_kb(chat_id, info))
    await callback.answer("Yangilandi.")


@router.callback_query(F.data.startswith("chreact:"))
async def choose_channel_reaction(callback: CallbackQuery):
    chat_id = callback.data.split(":", 1)[1]
    try:
        member = await bot.get_chat_member(int(chat_id), callback.from_user.id)
        if member.status not in ADMIN_STATUSES:
            return await callback.answer("Siz bu kanalda admin emassiz.", show_alert=True)
    except Exception:
        return await callback.answer("Kanalni tekshirib bo'lmadi.", show_alert=True)
    await callback.message.edit_text("Reaksiyani tanlang:", reply_markup=reaction_picker_kb(chat_id))
    await callback.answer()


@router.callback_query(F.data == "menu:schedule")
async def menu_schedule(callback: CallbackQuery, state: FSMContext):
    my_channels = await user_admin_channels(callback.from_user.id)
    if not my_channels:
        return await callback.answer("Siz admin bo'lgan va bot ham admin bo'lgan kanal topilmadi.", show_alert=True)
    await state.set_state(ScheduleStates.choosing_channel)
    await callback.message.edit_text("Qaysi kanalga yuborilsin?", reply_markup=channels_kb(my_channels, "ch"))
    await callback.answer()


@router.callback_query(F.data == "menu:list")
async def menu_list(callback: CallbackQuery):
    my_jobs = {jid: j for jid, j in JOBS.items() if j.get("created_by") == callback.from_user.id}
    if not my_jobs:
        await callback.message.edit_text(
            "Sizda rejalashtirilgan post yo'q.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:back")]]),
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
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:back")]]),
        )
    await callback.answer("O'chirildi.")


@router.callback_query(F.data == "noop")
async def noop(callback: CallbackQuery):
    await callback.answer()


@router.callback_query(ScheduleStates.choosing_channel, F.data.startswith("ch:"))
async def choose_channel(callback: CallbackQuery, state: FSMContext):
    channel_id = int(callback.data.split(":", 1)[1])
    try:
        member = await bot.get_chat_member(channel_id, callback.from_user.id)
        if member.status not in ADMIN_STATUSES:
            return await callback.answer("Siz bu kanalda admin emassiz.", show_alert=True)
    except Exception:
        return await callback.answer("Kanalni tekshirib bo'lmadi.", show_alert=True)
    await state.update_data(target_chat_id=channel_id)
    await state.set_state(ScheduleStates.waiting_content)
    await callback.message.edit_text("Endi menga yubormoqchi bo'lgan xabaringizni yuboring.")
    await callback.answer()


@router.message(ScheduleStates.waiting_content)
async def receive_content(message: Message, state: FSMContext):
    await state.update_data(from_chat_id=message.chat.id, message_id=message.message_id)
    await state.set_state(ScheduleStates.waiting_datetime)
    await message.answer(
        "Qachon yuborilsin? Formatda yozing:\n<code>YYYY-MM-DD HH:MM</code>\n"
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
        f"Tasdiqlaysizmi?\nKanal: <b>{title}</b>\nVaqt: <code>{run_time.strftime('%Y-%m-%d %H:%M')}</code>",
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
    await callback.message.edit_text(f"✅ Rejalashtirildi.\nID: <code>{job_id}</code>", reply_markup=main_menu_kb())
    await callback.answer()

# ---------------------------------------------------------------------------
# "Hoziroq yuborish" - faqat o'zi admin bo'lgan bir nechta kanalga birdan
# ---------------------------------------------------------------------------

@router.callback_query(F.data == "menu:broadcast")
async def menu_broadcast(callback: CallbackQuery, state: FSMContext):
    my_channels = await user_admin_channels(callback.from_user.id)
    if not my_channels:
        return await callback.answer("Siz admin bo'lgan kanal topilmadi.", show_alert=True)
    await state.set_state(BroadcastStates.selecting_channels)
    await state.update_data(selected=[])
    await callback.message.edit_text(
        "Qaysi kanallarga yuborilsin? (bir nechtasini tanlashingiz mumkin)",
        reply_markup=broadcast_select_kb(my_channels, []),
    )
    await callback.answer()


@router.callback_query(BroadcastStates.selecting_channels, F.data.startswith("bcsel:"))
async def broadcast_toggle(callback: CallbackQuery, state: FSMContext):
    chat_id = callback.data.split(":", 1)[1]
    my_channels = await user_admin_channels(callback.from_user.id)
    if chat_id not in my_channels:
        return await callback.answer("Bu kanal sizga tegishli emas.", show_alert=True)
    data = await state.get_data()
    selected = data.get("selected", [])
    if chat_id in selected:
        selected.remove(chat_id)
    else:
        selected.append(chat_id)
    await state.update_data(selected=selected)
    await callback.message.edit_reply_markup(reply_markup=broadcast_select_kb(my_channels, selected))
    await callback.answer()


@router.callback_query(BroadcastStates.selecting_channels, F.data == "bcnext")
async def broadcast_next(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get("selected"):
        return await callback.answer("Kamida bitta kanal tanlang.", show_alert=True)
    await state.set_state(BroadcastStates.waiting_content)
    await callback.message.edit_text("Yubormoqchi bo'lgan xabaringizni yuboring.")
    await callback.answer()


@router.message(BroadcastStates.waiting_content)
async def broadcast_receive_content(message: Message, state: FSMContext):
    await state.update_data(from_chat_id=message.chat.id, message_id=message.message_id)
    data = await state.get_data()
    my_channels = await user_admin_channels(message.from_user.id)
    titles = [my_channels[c]["title"] for c in data["selected"] if c in my_channels]
    await state.set_state(BroadcastStates.confirming)
    await message.answer("Tasdiqlaysizmi?\nQuyidagi kanallarga yuboriladi:\n- " + "\n- ".join(titles), reply_markup=confirm_kb())


@router.callback_query(BroadcastStates.confirming, F.data.startswith("confirm:"))
async def broadcast_confirm(callback: CallbackQuery, state: FSMContext):
    action = callback.data.split(":", 1)[1]
    data = await state.get_data()
    await state.clear()
    if action == "no":
        await callback.message.edit_text("Bekor qilindi.", reply_markup=main_menu_kb())
        return await callback.answer()

    my_channels = await user_admin_channels(callback.from_user.id)
    sent, failed = 0, 0
    for chat_id in data.get("selected", []):
        if chat_id not in my_channels:
            failed += 1
            continue
        try:
            await bot.copy_message(chat_id=int(chat_id), from_chat_id=data["from_chat_id"], message_id=data["message_id"])
            sent += 1
        except Exception:
            log.exception("Broadcast xato: %s", chat_id)
            failed += 1

    await callback.message.edit_text(
        f"✅ Yuborildi: {sent} ta kanalga" + (f", {failed} tasi muvaffaqiyatsiz." if failed else "."),
        reply_markup=main_menu_kb(),
    )
    await callback.answer()

# ---------------------------------------------------------------------------
# "Reklama tarmog'i" - faqat ROZILIK bergan kanallarga yuboriladi
# ---------------------------------------------------------------------------

@router.callback_query(F.data == "menu:adsnetwork")
async def menu_adsnetwork(callback: CallbackQuery, state: FSMContext):
    consenting = {cid: info for cid, info in CHANNELS.items() if info.get("ads_consent")}
    if not consenting:
        return await callback.answer("Hozircha reklamaga rozi bo'lgan kanal yo'q.", show_alert=True)
    await state.set_state(AdsNetworkStates.waiting_content)
    await callback.message.edit_text(
        f"Reklama tarmog'ida {len(consenting)} ta rozi bo'lgan kanal bor.\n"
        "Yubormoqchi bo'lgan reklama xabaringizni yuboring."
    )
    await callback.answer()


@router.message(AdsNetworkStates.waiting_content)
async def adsnetwork_receive_content(message: Message, state: FSMContext):
    await state.update_data(from_chat_id=message.chat.id, message_id=message.message_id)
    consenting = {cid: info for cid, info in CHANNELS.items() if info.get("ads_consent")}
    titles = [info["title"] for info in consenting.values()]
    await state.set_state(AdsNetworkStates.confirming)
    await message.answer(
        "Tasdiqlaysizmi? Quyidagi (reklamaga ROZI) kanallarga yuboriladi:\n- " + "\n- ".join(titles),
        reply_markup=confirm_kb(),
    )


@router.callback_query(AdsNetworkStates.confirming, F.data.startswith("confirm:"))
async def adsnetwork_confirm(callback: CallbackQuery, state: FSMContext):
    action = callback.data.split(":", 1)[1]
    data = await state.get_data()
    await state.clear()
    if action == "no":
        await callback.message.edit_text("Bekor qilindi.", reply_markup=main_menu_kb())
        return await callback.answer()

    # xavfsizlik: yuborish paytida ham faqat HALI ROZI bo'lganlarga yuboramiz
    consenting = {cid: info for cid, info in CHANNELS.items() if info.get("ads_consent")}
    sent, failed = 0, 0
    for chat_id in consenting:
        try:
            await bot.copy_message(chat_id=int(chat_id), from_chat_id=data["from_chat_id"], message_id=data["message_id"])
            sent += 1
        except Exception:
            log.exception("Reklama tarmog'ida xato: %s", chat_id)
            failed += 1

    await callback.message.edit_text(
        f"✅ Reklama yuborildi: {sent} ta kanalga" + (f", {failed} tasi muvaffaqiyatsiz." if failed else "."),
        reply_markup=main_menu_kb(),
    )
    await callback.answer()

# ---------------------------------------------------------------------------
# "📊 Statistika" - o'z kanallaringiz bo'yicha obunachilar soni
# ---------------------------------------------------------------------------

@router.callback_query(F.data == "menu:stats")
async def menu_stats(callback: CallbackQuery):
    my_channels = await user_admin_channels(callback.from_user.id)
    if not my_channels:
        return await callback.answer("Siz admin bo'lgan kanal topilmadi.", show_alert=True)

    lines = []
    for chat_id, info in my_channels.items():
        try:
            count = await bot.get_chat_member_count(int(chat_id))
        except Exception:
            count = "?"
        lines.append(f"<b>{info['title']}</b>: {count} obunachi")

    text = "\n".join(lines) + (
        "\n\n<i>Eslatma: Premium obunachilar soni Bot API orqali berilmaydi - "
        "buni faqat Telegram ilovasidagi kanal Statistics bo'limida (500+ "
        "obunachi bo'lganda) ko'rishingiz mumkin.</i>"
    )
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:back")]]),
    )
    await callback.answer()

# ---------------------------------------------------------------------------
# Avtomatik reaksiya
# ---------------------------------------------------------------------------

@router.channel_post()
async def auto_react(message: Message):
    chat_id = str(message.chat.id)
    if chat_id not in CHANNELS:
        return
    emoji = CHANNELS[chat_id].get("reaction", DEFAULT_REACTION)
    try:
        await bot.set_message_reaction(
            chat_id=message.chat.id,
            message_id=message.message_id,
            reaction=[ReactionTypeEmoji(emoji=emoji)],
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
