import asyncio
import aiosqlite
import json
import os
import re
import time
from datetime import datetime, timezone, timedelta
from uuid import uuid4
from io import BytesIO
from html import escape

from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, BaseMiddleware
from aiogram.client.session.aiohttp import AiohttpSession

from aiogram.types import (
    Message,
    BusinessConnection,
    BusinessMessagesDeleted,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    BufferedInputFile,
    LabeledPrice,
    PreCheckoutQuery,
)

from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

PANIC_DB_PATH = "business_bot.db"

from database import (
    init_db,
    create_or_update_user,
    get_user,
    get_user_by_connection,
    has_access,
    get_subscription_status,
    save_business_connection,
    save_message,
    save_edited_message,
    mark_message_deleted,
    get_all_users,
    get_user_statistics,
    grant_subscription,
    get_owner_notifications_enabled,
    set_owner_notifications_enabled,
    get_message_info,
    set_chat_mute,
    get_chat_mute,
    clear_chat_mute,
    get_expired_mutes,
    mark_service_deleted_message,
    consume_service_deleted_message,
    set_chat_warning,
    get_chat_warning,
    decrement_chat_warning,
    clear_chat_warning,
    create_payment,
    get_payment_by_payload,
    complete_payment,
    fail_payment,
    get_auto_reply_settings,
    update_auto_reply_settings,
    claim_auto_reply_cooldown,
    reset_auto_reply_cooldown,
    set_auto_reply_chat_disabled,
    is_auto_reply_chat_disabled,
)


# =========================================================
# CONFIG
# =========================================================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID_RAW = os.getenv("OWNER_ID")

PROXY = os.getenv(
    "PROXY",
    "socks5://127.0.0.1:10808",
)

# =========================================================
# PAYMENT CONFIG — TELEGRAM STARS ONLY
# =========================================================

PAYMENT_PLANS = {
    "month": {
        "name": "1 месяц",
        "days": 30,
        "stars": 200,
    },
    "3months": {
        "name": "3 месяца",
        "days": 90,
        "stars": 540,
    },
    "year": {
        "name": "1 год",
        "days": 365,
        "stars": 2000,
    },
    "lifetime": {
        "name": "Навсегда",
        "days": None,
        "stars": 2600,
    },
}

if not BOT_TOKEN:
    raise RuntimeError(
        "BOT_TOKEN не найден в файле .env"
    )

if not OWNER_ID_RAW:
    raise RuntimeError(
        "OWNER_ID не найден в файле .env"
    )

try:
    OWNER_ID = int(OWNER_ID_RAW)

except ValueError:
    raise RuntimeError(
        "OWNER_ID должен быть числом."
    )


# =========================================================
# FORCE SUBSCRIPTION
# =========================================================

FORCE_SUB_CHANNEL = "@reefercm"
FORCE_SUB_CHANNEL_URL = "https://t.me/reefercm"


bot = None


# =========================================================
# CUSTOM COMMAND PRESETS
# =========================================================

START_TEXT = (
    "<b>reefercm - твой помощник в мире Telegram</b>\n"
    "────────────────\n\n"
    "> <i>Преимущество над чатами и быстрая автоматизация в одном месте.</i>\n\n"
    "ℹ️ Нажмите <b>«Возможности бота»</b> ниже, чтобы просмотреть "
    "весь список доступных команд и возможностей.\n"
    "Поддержка @reefercm_sup"
)

CUSTOM_COMMANDS_FILE = "custom_commands.json"
CUSTOM_COMMAND_MAX = 30
CUSTOM_ACTIONS_MAX = 3

# Реестр сообщений, которые бот сам отправил в Business-чаты.
# Нужен для .panic: Telegram не даёт получить старую историю,
# поэтому запоминаем ID своих сообщений при отправке.
BOT_SENT_MESSAGES_FILE = "bot_sent_business_messages.json"
_bot_sent_messages_lock = asyncio.Lock()


async def _remember_bot_sent_message(chat_id: int, connection_id: str, message_id: int):
    async with _bot_sent_messages_lock:
        try:
            data = {}
            if os.path.exists(BOT_SENT_MESSAGES_FILE):
                with open(BOT_SENT_MESSAGES_FILE, "r", encoding="utf-8") as file:
                    loaded = json.load(file)
                    if isinstance(loaded, dict):
                        data = loaded
            key = f"{connection_id}:{chat_id}"
            ids = [int(x) for x in data.get(key, [])]
            if int(message_id) not in ids:
                ids.append(int(message_id))
            data[key] = ids[-5000:]
            temp = BOT_SENT_MESSAGES_FILE + ".tmp"
            with open(temp, "w", encoding="utf-8") as file:
                json.dump(data, file, ensure_ascii=False)
            os.replace(temp, BOT_SENT_MESSAGES_FILE)
        except Exception as e:
            print(f"⚠️ Не удалось запомнить сообщение бота {message_id}: {e}")


async def _get_bot_sent_message_ids(chat_id: int, connection_id: str, before_message_id: int | None = None):
    async with _bot_sent_messages_lock:
        try:
            if not os.path.exists(BOT_SENT_MESSAGES_FILE):
                return []
            with open(BOT_SENT_MESSAGES_FILE, "r", encoding="utf-8") as file:
                data = json.load(file)
            ids = [int(x) for x in data.get(f"{connection_id}:{chat_id}", [])]
            if before_message_id is not None:
                ids = [x for x in ids if x < before_message_id]
            return sorted(set(ids), reverse=True)
        except Exception as e:
            print(f"⚠️ Не удалось получить сообщения бота для .panic: {e}")
            return []


async def _forget_bot_sent_message(chat_id: int, connection_id: str, message_id: int):
    async with _bot_sent_messages_lock:
        try:
            if not os.path.exists(BOT_SENT_MESSAGES_FILE):
                return
            with open(BOT_SENT_MESSAGES_FILE, "r", encoding="utf-8") as file:
                data = json.load(file)
            key = f"{connection_id}:{chat_id}"
            ids = [int(x) for x in data.get(key, []) if int(x) != int(message_id)]
            if ids:
                data[key] = ids
            else:
                data.pop(key, None)
            temp = BOT_SENT_MESSAGES_FILE + ".tmp"
            with open(temp, "w", encoding="utf-8") as file:
                json.dump(data, file, ensure_ascii=False)
            os.replace(temp, BOT_SENT_MESSAGES_FILE)
        except Exception as e:
            print(f"⚠️ Не удалось очистить ID сообщения бота: {e}")

_custom_commands_lock = asyncio.Lock()


def _load_custom_commands():
    try:
        if not os.path.exists(CUSTOM_COMMANDS_FILE):
            return {}
        with open(CUSTOM_COMMANDS_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        print(f"⚠️ Не удалось загрузить заготовки: {e}")
        return {}


def _save_custom_commands(data):
    temp_file = CUSTOM_COMMANDS_FILE + ".tmp"
    with open(temp_file, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
    os.replace(temp_file, CUSTOM_COMMANDS_FILE)


async def get_custom_commands(user_id: int):
    async with _custom_commands_lock:
        data = _load_custom_commands()
        commands = data.get(str(user_id), {})
        return commands if isinstance(commands, dict) else {}


async def save_custom_command(user_id: int, trigger: str, actions: list[str]):
    async with _custom_commands_lock:
        data = _load_custom_commands()
        user_commands = data.setdefault(str(user_id), {})
        user_commands[trigger] = actions
        _save_custom_commands(data)


async def delete_custom_command(user_id: int, trigger: str):
    async with _custom_commands_lock:
        data = _load_custom_commands()
        user_commands = data.get(str(user_id), {})
        if trigger not in user_commands:
            return False
        del user_commands[trigger]
        if user_commands:
            data[str(user_id)] = user_commands
        else:
            data.pop(str(user_id), None)
        _save_custom_commands(data)
        return True


# =========================================================
# ANTI-DUPLICATE EVENTS
# =========================================================

_processed_events = {}

EVENT_CACHE_TTL = 300


def is_duplicate_event(
    event_type: str,
    connection_id: str | None,
    chat_id: int | None,
    message_id: int | None,
):
    now = time.monotonic()

    expired = [
        key
        for key, timestamp in _processed_events.items()
        if now - timestamp > EVENT_CACHE_TTL
    ]

    for key in expired:
        _processed_events.pop(
            key,
            None,
        )

    event_key = (
        event_type,
        connection_id,
        chat_id,
        message_id,
    )

    if event_key in _processed_events:

        print()
        print("🛡 DUPLICATE EVENT ПРОПУЩЕН")
        print(f"   Type: {event_type}")
        print(f"   Connection: {connection_id}")
        print(f"   Chat ID: {chat_id}")
        print(f"   Message ID: {message_id}")

        return True

    _processed_events[event_key] = now

    return False


# =========================================================
# ANTI-DUPLICATE ONE-TIME REPLIES
# =========================================================

_processed_one_time_replies = {}

ONE_TIME_REPLY_CACHE_TTL = 600


def is_duplicate_one_time_reply(
    user_id: int,
    file_id: str,
):
    """
    Защита от повторной отправки одного и того же
    одноразового медиа одному и тому же пользователю.
    """

    now = time.monotonic()

    expired = [
        key
        for key, timestamp in _processed_one_time_replies.items()
        if now - timestamp > ONE_TIME_REPLY_CACHE_TTL
    ]

    for key in expired:
        _processed_one_time_replies.pop(
            key,
            None,
        )

    key = (
        user_id,
        file_id,
    )

    if key in _processed_one_time_replies:

        print()
        print("=" * 60)
        print("🛡 DUPLICATE ONE-TIME REPLY ПРОПУЩЕН")
        print("=" * 60)
        print(f"User ID: {user_id}")
        print(f"File ID: {file_id}")
        print(
            "Причина: это же одноразовое медиа "
            "уже было отправлено этому пользователю."
        )
        print("=" * 60)

        return True

    _processed_one_time_replies[key] = now

    return False


# =========================================================
# ONE-TIME MEDIA SETTINGS
# =========================================================

one_time_media_notifications_enabled = True


# =========================================================
# FSM
# =========================================================

class AdminGrantStates(StatesGroup):
    waiting_user_id = State()


class CustomCommandStates(StatesGroup):
    waiting_trigger = State()
    waiting_action = State()


class AutoReplyStates(StatesGroup):
    waiting_day_text = State()
    waiting_night_text = State()
    waiting_interval = State()
    waiting_day_start = State()
    waiting_day_end = State()
    waiting_night_start = State()
    waiting_night_end = State()


# =========================================================
# OWNER
# =========================================================

def is_owner(
    user_id: int,
):
    return user_id == OWNER_ID


async def is_subscribed_to_force_channel(
    user_id: int,
):
    """Проверяет обязательную подписку пользователя на @reefercm."""

    if is_owner(user_id):
        return True

    if bot is None:
        return False

    try:
        member = await bot.get_chat_member(
            chat_id=FORCE_SUB_CHANNEL,
            user_id=user_id,
        )

        status = member.status

        if status in {
            "creator",
            "administrator",
            "member",
        }:
            return True

        if status == "restricted" and getattr(member, "is_member", False):
            return True

        return False

    except Exception as e:
        print(
            f"⚠️ Не удалось проверить подписку {user_id} "
            f"на {FORCE_SUB_CHANNEL}: {e}"
        )
        return False


def force_subscription_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📢 Подписаться на канал",
                    url=FORCE_SUB_CHANNEL_URL,
                )
            ],
            [
                InlineKeyboardButton(
                    text="✅ Проверить подписку",
                    callback_data="check_channel_subscription",
                )
            ],
        ]
    )


async def send_force_subscription_message(target):
    text = (
        "🔒 <b>Доступ к боту ограничен</b>\n\n"
        "Для использования <b>reefercm</b> необходимо "
        "подписаться на наш Telegram-канал <b>@reefercm</b>.\n\n"
        "После подписки нажмите кнопку «✅ Проверить подписку»."
    )

    if isinstance(target, CallbackQuery):
        if target.message:
            await target.message.edit_text(
                text,
                parse_mode="HTML",
                reply_markup=force_subscription_keyboard(),
            )
        await target.answer(
            "Сначала подпишитесь на @reefercm.",
            show_alert=True,
        )
        return

    await target.answer(
        text,
        parse_mode="HTML",
        reply_markup=force_subscription_keyboard(),
    )


async def user_has_access(
    user_id: int,
):
    if is_owner(user_id):
        return True

    if not await is_subscribed_to_force_channel(user_id):
        return False

    return await has_access(user_id)


# =========================================================
# FORCE SUBSCRIPTION MIDDLEWARE
# =========================================================

class ForceSubscriptionMiddleware(BaseMiddleware):

    async def __call__(self, handler, event, data):
        user = getattr(event, "from_user", None)

        if user is None:
            return await handler(event, data)

        if is_owner(user.id):
            return await handler(event, data)

        # Кнопка проверки подписки должна работать даже без подписки.
        if isinstance(event, CallbackQuery) and (
            event.data == "check_channel_subscription"
        ):
            return await handler(event, data)

        # /start всегда должен быть доступен, чтобы пользователь мог
        # получить экран обязательной подписки.
        if isinstance(event, Message):
            text = (event.text or "").strip()
            if text == "/start":
                return await handler(event, data)

        if not await is_subscribed_to_force_channel(user.id):
            await send_force_subscription_message(event)
            return

        return await handler(event, data)


# =========================================================
# KEYBOARDS
# =========================================================

def main_keyboard():

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🤖 Возможности бота",
                    callback_data="bot_features",
                )
            ],
            [
                InlineKeyboardButton(
                    text="📝 Заготовки команд",
                    callback_data="custom_commands",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🤖 Автоответы",
                    callback_data="auto_replies",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🔗 Подключение",
                    callback_data="connection_tutorial",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🟢 ⭐ Подписка",
                    callback_data="subscription",
                )
            ],
        ]
    )


def back_keyboard():

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="◀️ Назад",
                    callback_data="back_to_main",
                )
            ]
        ]
    )


def admin_keyboard(
    notifications_enabled: bool = True,
):

    status = (
        "🟢 ВКЛ"
        if notifications_enabled
        else "🔴 ВЫКЛ"
    )

    one_time_status = (
        "🟢 ВКЛ"
        if one_time_media_notifications_enabled
        else "🔴 ВЫКЛ"
    )

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="👥 Пользователи",
                    callback_data="admin_users",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⭐ Выдать подписку",
                    callback_data="admin_grant",
                )
            ],
            [
                InlineKeyboardButton(
                    text="📊 Статистика",
                    callback_data="admin_stats",
                )
            ],
            [
                InlineKeyboardButton(
                    text=(
                        "🔔 Уведомления от пользователей: "
                        f"{status}"
                    ),
                    callback_data="admin_toggle_notifications",
                )
            ],
            [
                InlineKeyboardButton(
                    text=(
                        "📦 Одноразовые медиа OWNER: "
                        f"{one_time_status}"
                    ),
                    callback_data="admin_toggle_one_time",
                )
            ],
        ]
    )


def grant_keyboard():

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="7 дней",
                    callback_data="grant_7",
                ),
                InlineKeyboardButton(
                    text="30 дней",
                    callback_data="grant_30",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="90 дней",
                    callback_data="grant_90",
                ),
                InlineKeyboardButton(
                    text="365 дней",
                    callback_data="grant_365",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="❌ Отмена",
                    callback_data="admin_cancel_grant",
                )
            ],
        ]
    )


# =========================================================
# MUTE BUTTON
# =========================================================

def mute_control_keyboard(
    owner_id: int,
    chat_id: int,
    business_connection_id: str,
):

    """
    Кнопка управления мутом.

    В callback_data передаём:
    owner_id
    chat_id
    business_connection_id

    Таким образом кнопка всегда знает,
    какой именно Business Chat нужно размутить.
    """

    callback_data = (
        f"remove_mute:"
        f"{owner_id}:"
        f"{chat_id}:"
        f"{business_connection_id}"
    )

    # Telegram ограничивает callback_data 64 байтами.
    # Business Connection ID обычно короткий, но проверяем.
    if len(callback_data.encode("utf-8")) > 64:

        print()
        print("=" * 60)
        print("⚠️ CALLBACK DATA СЛИШКОМ ДЛИННЫЙ")
        print("=" * 60)
        print(f"Length: {len(callback_data.encode('utf-8'))}")
        print(f"Owner: {owner_id}")
        print(f"Chat: {chat_id}")
        print(f"Connection: {business_connection_id}")
        print("=" * 60)

        return None

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔊 Снять мут",
                    callback_data=callback_data,
                )
            ]
        ]
    )


# =========================================================
# NOTIFICATION
# =========================================================

async def send_notification(
    notification: str,
    owner_id: int,
    source_user_id: int | None = None,
    media_type: str | None = None,
    file_id: str | None = None,
):
    """
    Центральная система уведомлений.

    Основной получатель:
        владелец конкретного Business Connection.

    OWNER_ID получает дополнительную копию,
    если включены глобальные уведомления.
    """

    if bot is None:
        return

    recipients = set()

    # =====================================================
    # ПРОВЕРКА АВТОРА
    # =====================================================

    if (
        source_user_id is not None
        and source_user_id == owner_id
    ):
        print(
            "ℹ️ Уведомление не отправляется: "
            "сообщение принадлежит владельцу Business Connection."
        )
        return

    # =====================================================
    # ОСНОВНОЙ ПОЛУЧАТЕЛЬ
    # =====================================================

    recipients.add(owner_id)

    print(
        f"🔔 Основной получатель уведомления: "
        f"{owner_id}"
    )

    # =====================================================
    # OWNER — ГЛОБАЛЬНАЯ КОПИЯ
    # =====================================================

    if owner_id != OWNER_ID:

        notifications_enabled = (
            await get_owner_notifications_enabled()
        )

        if notifications_enabled:

            recipients.add(OWNER_ID)

            print(
                "🔔 OWNER получает дополнительную "
                "копию уведомления."
            )

        else:

            print(
                "🔕 Глобальные уведомления OWNER выключены."
            )

    # =====================================================
    # ОТПРАВКА
    # =====================================================

    for recipient_id in recipients:

        try:

            if media_type == "photo" and file_id:

                await bot.send_photo(
                    chat_id=recipient_id,
                    photo=file_id,
                    caption=notification[:1024],
                    parse_mode="HTML",
                )

            elif media_type == "voice" and file_id:

                await bot.send_voice(
                    chat_id=recipient_id,
                    voice=file_id,
                    caption=notification[:1024],
                    parse_mode="HTML",
                )

            elif media_type == "document" and file_id:

                await bot.send_document(
                    chat_id=recipient_id,
                    document=file_id,
                    caption=notification[:1024],
                    parse_mode="HTML",
                )

            elif media_type == "video" and file_id:

                await bot.send_video(
                    chat_id=recipient_id,
                    video=file_id,
                    caption=notification[:1024],
                    parse_mode="HTML",
                )

            elif media_type == "audio" and file_id:

                await bot.send_audio(
                    chat_id=recipient_id,
                    audio=file_id,
                    caption=notification[:1024],
                    parse_mode="HTML",
                )

            elif media_type == "animation" and file_id:

                await bot.send_animation(
                    chat_id=recipient_id,
                    animation=file_id,
                    caption=notification[:1024],
                    parse_mode="HTML",
                )

            elif media_type == "video_note" and file_id:

                await bot.send_video_note(
                    chat_id=recipient_id,
                    video_note=file_id,
                )

                await bot.send_message(
                    chat_id=recipient_id,
                    text=notification,
                    parse_mode="HTML",
                )

            elif media_type == "sticker" and file_id:

                await bot.send_sticker(
                    chat_id=recipient_id,
                    sticker=file_id,
                )

                await bot.send_message(
                    chat_id=recipient_id,
                    text=notification,
                    parse_mode="HTML",
                )

            else:

                await bot.send_message(
                    chat_id=recipient_id,
                    text=notification,
                    parse_mode="HTML",
                )

            print(
                f"✅ Уведомление отправлено "
                f"{recipient_id}"
            )

        except Exception as e:

            print(
                f"❌ Ошибка отправки уведомления "
                f"{recipient_id}: {e}"
            )


# =========================================================
# START
# =========================================================

async def start_command(
    message: Message,
):

    if not message.from_user:
        return

    telegram_id = message.from_user.id

    await create_or_update_user(
        telegram_id=telegram_id,
        username=message.from_user.username,
        full_name=message.from_user.full_name,
        is_owner=is_owner(telegram_id),
    )

    if not await is_subscribed_to_force_channel(telegram_id):
        await send_force_subscription_message(message)
        return

    await message.answer(
        START_TEXT,
        parse_mode="HTML",
        reply_markup=main_keyboard(),
    )


async def check_channel_subscription_callback(
    callback: CallbackQuery,
):
    if not callback.from_user:
        await callback.answer()
        return

    user_id = callback.from_user.id

    if await is_subscribed_to_force_channel(user_id):
        await callback.message.edit_text(
            START_TEXT,
            parse_mode="HTML",
            reply_markup=main_keyboard(),
        )
        await callback.answer("✅ Подписка подтверждена!")
        return

    await callback.answer(
        "❌ Подписка на @reefercm не найдена.",
        show_alert=True,
    )


# =========================================================
# ROOT
# =========================================================

async def root_command(
    message: Message,
):

    if not message.from_user:
        return

    if not is_owner(message.from_user.id):

        await message.answer(
            "⛔ Доступ запрещён."
        )

        return

    notifications_enabled = (
        await get_owner_notifications_enabled()
    )

    await message.answer(
        "👑 <b>Админ-панель</b>\n\n"
        "Выбери действие:",
        parse_mode="HTML",
        reply_markup=admin_keyboard(
            notifications_enabled
        ),
    )


# =========================================================
# BUSINESS CONNECTION
# =========================================================

async def handle_business_connection(
    connection: BusinessConnection,
):

    print()
    print("=" * 50)
    print("BUSINESS CONNECTION")
    print("=" * 50)

    print(f"Connection ID: {connection.id}")
    print(f"User Chat ID: {connection.user_chat_id}")
    print(f"Владелец: {connection.user.full_name}")
    print(f"Активен: {connection.is_enabled}")
    print("=" * 50)

    telegram_id = connection.user_chat_id

    existing_user = await get_user(
        telegram_id
    )

    if not existing_user:

        await create_or_update_user(
            telegram_id=telegram_id,
            username=connection.user.username,
            full_name=connection.user.full_name,
            is_owner=is_owner(telegram_id),
        )

    await save_business_connection(
        telegram_id=telegram_id,
        business_connection_id=connection.id,
        is_enabled=connection.is_enabled,
    )


# =========================================================
# FEATURES
# =========================================================

def bot_features_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📌 Пассивные",
                    callback_data="features_passive",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⚙️ Команды",
                    callback_data="features_commands",
                )
            ],
            [
                InlineKeyboardButton(
                    text="◀️ Назад",
                    callback_data="back_to_main",
                )
            ],
        ]
    )


def features_passive_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="💾 Сохранение сообщений",
                    callback_data="feature_passive_messages",
                )
            ],
            [
                InlineKeyboardButton(
                    text="📦 Одноразовые медиа",
                    callback_data="feature_passive_media",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🤖 Автоответы",
                    callback_data="feature_passive_autoreplies",
                )
            ],
            [
                InlineKeyboardButton(
                    text="◀️ Назад",
                    callback_data="bot_features",
                )
            ],
        ]
    )


def features_commands_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔇 .mute",
                    callback_data="feature_command_mute",
                ),
                InlineKeyboardButton(
                    text="🔊 .unmute",
                    callback_data="feature_command_unmute",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="⚠️ .warn",
                    callback_data="feature_command_warn",
                ),
                InlineKeyboardButton(
                    text="📨 .spam",
                    callback_data="feature_command_spam",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🚨 .panic",
                    callback_data="feature_command_panic",
                )
            ],
            [
                InlineKeyboardButton(text="🤖 .autoreply_off", callback_data="feature_command_autoreply_off"),
                InlineKeyboardButton(text="🤖 .autoreply_on", callback_data="feature_command_autoreply_on"),
            ],
            [
                InlineKeyboardButton(text="📊 .autoreply_status", callback_data="feature_command_autoreply_status"),
            ],
            [
                InlineKeyboardButton(
                    text="◀️ Назад",
                    callback_data="bot_features",
                )
            ],
        ]
    )


async def bot_features_callback(
    callback: CallbackQuery,
):
    await callback.message.edit_text(
        "🤖 <b>Возможности бота</b>\n\n"
        "Выбери категорию, чтобы посмотреть доступные "
        "функции и команды.",
        parse_mode="HTML",
        reply_markup=bot_features_keyboard(),
    )
    await callback.answer()


async def features_passive_callback(
    callback: CallbackQuery,
):
    await callback.message.edit_text(
        "📌 <b>Пассивные</b>\n\n"
        "Функции, которые работают автоматически "
        "и не требуют запуска команды.",
        parse_mode="HTML",
        reply_markup=features_passive_keyboard(),
    )
    await callback.answer()


async def features_commands_callback(
    callback: CallbackQuery,
):
    await callback.message.edit_text(
        "⚙️ <b>Команды</b>\n\n"
        "Функции, которые запускаются вручную "
        "с помощью команд.",
        parse_mode="HTML",
        reply_markup=features_commands_keyboard(),
    )
    await callback.answer()


async def feature_detail_callback(
    callback: CallbackQuery,
):
    data = callback.data or ""

    texts = {
        "feature_passive_messages": (
            "💾 <b>Сохранение сообщений</b>\n\n"
            "Бот автоматически сохраняет входящие сообщения "
            "из подключённого Business-аккаунта.\n\n"
            "Сохраняются текст, данные отправителя, тип медиа "
            "и необходимые данные для дальнейшей обработки.\n\n"
            "Сохранённые данные используются для отслеживания "
            "изменений и удалений сообщений."
        ),
        "feature_passive_media": (
            "📦 <b>Сохранение одноразовых медиа</b>\n\n"
            "Бот умеет обрабатывать одноразовые фотографии, "
            "видео, кружки и GIF при работе с Reply.\n\n"
            "Медиа обрабатывается в памяти и не сохраняется "
            "на диск."
        ),
        "feature_passive_autoreplies": (
            "🤖 <b>Автоответы</b>\n\n"
            "Автоматические ответы на входящие сообщения "
            "по заданным условиям.\n\n"
            "Функция предназначена для автоматизации общения "
            "и отправки заранее заданных ответов."
        ),
        "feature_command_mute": (
            "🔇 <b>.mute</b>\n\n"
            "Временно ограничивает общение с пользователем.\n\n"
            "Формат:\n"
            "<code>.mute [минуты]</code>\n\n"
            "Пример:\n"
            "<code>.mute 30</code>"
        ),
        "feature_command_unmute": (
            "🔊 <b>.unmute</b>\n\n"
            "Снимает действующий мут с пользователя.\n\n"
            "Формат:\n"
            "<code>.unmute</code>"
        ),
        "feature_command_warn": (
            "⚠️ <b>.warn</b>\n\n"
            "Выдаёт предупреждение пользователю. "
            "При достижении установленного количества "
            "предупреждений может применяться ограничение.\n\n"
            "Формат:\n"
            "<code>.warn [количество]</code>"
        ),
        "feature_command_spam": (
            "📨 <b>.spam</b>\n\n"
            "Отправляет указанное количество сообщений.\n\n"
            "Формат:\n"
            "<code>.spam [количество] [текст]</code>\n\n"
            "Максимум — <b>3 сообщения</b> за один запуск команды."
        ),
        "feature_command_autoreply_off": (
            "🤖 <b>.autoreply_off</b>\n\n"
            "Отключает автоответы только в текущем Business-чате.\n"
            "Общие настройки автоответов не меняются."
        ),
        "feature_command_autoreply_on": (
            "🤖 <b>.autoreply_on</b>\n\n"
            "Снова разрешает автоответы в текущем Business-чате, "
            "если общие автоответы включены."
        ),
        "feature_command_autoreply_status": (
            "📊 <b>.autoreply_status</b>\n\n"
            "Показывает, включены ли автоответы в текущем чате, "
            "и учитывает общий переключатель автоответов."
        ),
        "feature_command_panic": (
            "🚨 <b>.panic</b>\n\n"
            "Экстренная команда для удаления сохранённых "
            "предыдущих сообщений бота из Business-чата.\n\n"
            "Примеры:\n"
            "<code>.panic 10</code> — удалить 10 предыдущих сообщений.\n"
            "<code>.panic all</code> — удалить всю доступную "
            "сохранённую историю."
        ),
    }

    text = texts.get(data)
    if not text:
        await callback.answer("❌ Функция не найдена.", show_alert=True)
        return

    back_callback = (
        "features_passive"
        if data.startswith("feature_passive_")
        else "features_commands"
    )

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="◀️ Назад",
                    callback_data=back_callback,
                )
            ]
        ]
    )

    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=keyboard,
    )
    await callback.answer()


# =========================================================
# SUBSCRIPTION
# =========================================================

# =========================================================
# CUSTOM COMMAND PRESETS UI
# =========================================================

def custom_commands_keyboard(commands: dict):
    rows = []
    for index, trigger in enumerate(commands.keys()):
        rows.append([
            InlineKeyboardButton(
                text=f"🗑 {trigger}",
                callback_data=f"custom_delete:{index}",
            )
        ])
    rows.append([
        InlineKeyboardButton(
            text="➕ Создать заготовку",
            callback_data="custom_create",
        )
    ])
    rows.append([
        InlineKeyboardButton(
            text="◀️ Назад",
            callback_data="back_to_main",
        )
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def custom_action_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Сохранить",
                    callback_data="custom_save",
                ),
                InlineKeyboardButton(
                    text="❌ Отмена",
                    callback_data="custom_cancel",
                ),
            ]
        ]
    )


def _format_custom_commands_text(commands: dict):
    if not commands:
        return (
            "📝 <b>Заготовки команд</b>\n\n"
            "У тебя пока нет заготовок.\n\n"
            "Нажми ниже «➕ Создать заготовку» и следуй инструкциям"
        )

    lines = ["📝 <b>Заготовки команд</b>", ""]
    for trigger, actions in commands.items():
        lines.append(f"🔹 <code>{escape(trigger)}</code>")
        for index, action in enumerate(actions, 1):
            lines.append(f"   {index}. {escape(str(action))}")
        lines.append("")
    return "\n".join(lines)


async def custom_commands_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    if not await user_has_access(user_id):
        await callback.answer("⛔ Нет активного доступа.", show_alert=True)
        return

    commands = await get_custom_commands(user_id)
    await callback.message.edit_text(
        _format_custom_commands_text(commands),
        parse_mode="HTML",
        reply_markup=custom_commands_keyboard(commands),
    )
    await callback.answer()


async def custom_create_callback(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not await user_has_access(user_id):
        await callback.answer("⛔ Нет активного доступа.", show_alert=True)
        return

    commands = await get_custom_commands(user_id)
    if len(commands) >= CUSTOM_COMMAND_MAX:
        await callback.answer(
            f"❌ Максимум {CUSTOM_COMMAND_MAX} заготовок.",
            show_alert=True,
        )
        return

    await state.clear()
    await state.set_state(CustomCommandStates.waiting_trigger)
    await callback.message.edit_text(
        "📝 <b>Создание заготовки</b>\n\n"
        "Шаг 1 из 2. Отправь команду-триггер.\n\n"
        "Например: <code>.стоп</code>\n\n"
        "Триггер должен начинаться с точки, быть без пробелов "
        "и содержать только буквы, цифры или _.\n\n"
        "Для отмены: <code>/cancel</code>",
        parse_mode="HTML",
    )
    await callback.answer()


async def custom_trigger_message(message: Message, state: FSMContext):
    if not message.from_user or not message.text:
        return

    user_id = message.from_user.id
    if not await user_has_access(user_id):
        await state.clear()
        return

    text = message.text.strip()
    if text == "/cancel":
        await state.clear()
        await message.answer(
            "❌ Создание заготовки отменено.",
            reply_markup=main_keyboard(),
        )
        return

    if not re.fullmatch(r"\.[A-Za-zА-Яа-яЁё0-9_]{1,30}", text):
        await message.answer(
            "❌ Некорректный триггер.\n\n"
            "Пример: <code>.стоп</code>\nБез пробелов.",
            parse_mode="HTML",
        )
        return

    if text.casefold() in {
        ".mute", ".unmute", ".warn", ".spam", ".panic",
        ".autoreply_off", ".autoreply_on", ".autoreply_status",
    }:
        await message.answer(
            "❌ Эта команда уже используется ботом. Выбери другой триггер."
        )
        return

    commands = await get_custom_commands(user_id)
    if any(key.casefold() == text.casefold() for key in commands):
        await message.answer(
            "❌ Такая заготовка уже существует. Выбери другой триггер."
        )
        return

    await state.update_data(trigger=text, actions=[])
    await state.set_state(CustomCommandStates.waiting_action)
    await message.answer(
        "📝 <b>Шаг 2 из 2</b>\n\n"
        f"Триггер: <code>{escape(text)}</code>\n\n"
        "Отправь действие №1. Это может быть обычный текст или команда:\n"
        "<code>.mute 30</code>\n"
        "<code>.unmute</code>\n"
        "<code>.warn 3</code>\n"
        "<code>.spam 3 Текст</code>\n"
        "<code>.panic 10</code> — удалить 10 предыдущих сообщений\n"
        "<code>.panic all</code> — удалить всю сохранённую историю\n\n"
        "Можно добавить от 1 до 3 действий. После первого появится кнопка сохранения.\n\n"
        "Для отмены: <code>/cancel</code>",
        parse_mode="HTML",
    )


async def custom_action_message(message: Message, state: FSMContext):
    if not message.from_user or not message.text:
        return

    user_id = message.from_user.id
    if not await user_has_access(user_id):
        await state.clear()
        return

    text = message.text.strip()
    if text == "/cancel":
        await state.clear()
        await message.answer(
            "❌ Создание заготовки отменено.",
            reply_markup=main_keyboard(),
        )
        return

    data = await state.get_data()
    trigger = data.get("trigger")
    actions = list(data.get("actions") or [])
    if not trigger:
        await state.clear()
        return

    if not text:
        await message.answer("❌ Действие не может быть пустым.")
        return

    if len(text) > 4000:
        await message.answer(
            "❌ Действие слишком длинное. Максимум 4000 символов."
        )
        return

    actions.append(text)
    await state.update_data(actions=actions)

    if len(actions) >= CUSTOM_ACTIONS_MAX:
        await save_custom_command(
            user_id,
            trigger,
            actions[:CUSTOM_ACTIONS_MAX],
        )
        await state.clear()
        await message.answer(
            "✅ <b>Заготовка сохранена!</b>\n\n"
            f"Триггер: <code>{escape(trigger)}</code>\n"
            + "\n".join(
                f"{i}. {escape(a)}"
                for i, a in enumerate(actions, 1)
            ),
            parse_mode="HTML",
            reply_markup=main_keyboard(),
        )
        return

    await message.answer(
        "✅ Действие добавлено.\n\n"
        f"Триггер: <code>{escape(trigger)}</code>\n"
        f"Добавлено: <b>{len(actions)}/{CUSTOM_ACTIONS_MAX}</b>\n\n"
        + "\n".join(
            f"{i}. {escape(a)}"
            for i, a in enumerate(actions, 1)
        )
        + "\n\nМожно сохранить сейчас или отправить ещё одно действие.",
        parse_mode="HTML",
        reply_markup=custom_action_keyboard(),
    )


async def custom_save_callback(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not await user_has_access(user_id):
        await state.clear()
        await callback.answer("⛔ Нет активного доступа.", show_alert=True)
        return

    data = await state.get_data()
    trigger = data.get("trigger")
    actions = list(data.get("actions") or [])
    if not trigger or not actions:
        await callback.answer(
            "❌ Добавь хотя бы одно действие.",
            show_alert=True,
        )
        return

    await save_custom_command(
        user_id,
        trigger,
        actions[:CUSTOM_ACTIONS_MAX],
    )
    await state.clear()

    commands = await get_custom_commands(user_id)
    await callback.message.edit_text(
        _format_custom_commands_text(commands),
        parse_mode="HTML",
        reply_markup=custom_commands_keyboard(commands),
    )
    await callback.answer("✅ Заготовка сохранена.")


async def custom_cancel_callback(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    commands = await get_custom_commands(callback.from_user.id)
    await callback.message.edit_text(
        _format_custom_commands_text(commands),
        parse_mode="HTML",
        reply_markup=custom_commands_keyboard(commands),
    )
    await callback.answer("❌ Создание отменено.")


async def custom_delete_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    if not await user_has_access(user_id):
        await callback.answer("⛔ Нет активного доступа.", show_alert=True)
        return

    try:
        index = int(callback.data.split(":", 1)[1])
    except (ValueError, AttributeError):
        await callback.answer("❌ Некорректная заготовка.", show_alert=True)
        return

    commands = await get_custom_commands(user_id)
    triggers = list(commands.keys())
    if index < 0 or index >= len(triggers):
        await callback.answer("❌ Заготовка не найдена.", show_alert=True)
        return

    trigger = triggers[index]
    await delete_custom_command(user_id, trigger)
    commands = await get_custom_commands(user_id)
    await callback.message.edit_text(
        _format_custom_commands_text(commands),
        parse_mode="HTML",
        reply_markup=custom_commands_keyboard(commands),
    )
    await callback.answer(f"🗑 Заготовка {trigger} удалена.")


# =========================================================
# SUBSCRIPTION
# =========================================================

async def connection_tutorial_callback(
    callback: CallbackQuery,
):
    text = (
        "🔗 <b>Подключение бота</b>\n\n"
        "Чтобы бот начал работать с твоими чатами, "
        "его нужно подключить через Telegram Business.\n\n"
        "<b>Как подключить:</b>\n"
        "1. Открой <b>Telegram → Настройки</b>.\n"
        "2. Перейди в раздел <b>Telegram для бизнеса</b>.\n"
        "3. Открой пункт <b>Чат-боты</b>.\n"
        "4. Добавь бота <b>reefercm</b> по его username.\n"
        "5. Выбери чаты, к которым бот должен иметь доступ.\n"
        "6. Подтверди подключение.\n\n"
        "После подключения бот получит Business Connection "
        "и сможет работать с выбранными чатами.\n\n"
        "⚠️ <b>Важно:</b> подключение выполняется в настройках "
        "Telegram Business именно того аккаунта, который ты хочешь подключить."
    )

    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=back_keyboard(),
    )
    await callback.answer()



# =========================================================
# AUTO REPLIES
# =========================================================

def auto_replies_keyboard(settings, connection_id):
    enabled = bool(settings.get("enabled", 0))
    interval = int(settings.get("interval_minutes", 30) or 30)

    enabled_text = "🟢 ВКЛ" if enabled else "🔴 ВЫКЛ"

    if interval >= 60 and interval % 60 == 0:
        interval_text = f"{interval // 60} ч."
    else:
        interval_text = f"{interval} мин."

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"🤖 Автоответы: {enabled_text}",
                    callback_data=f"auto_reply_toggle:{connection_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="☀️ Изменить дневной ответ",
                    callback_data=f"auto_reply_day_text:{connection_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🌙 Изменить ночной ответ",
                    callback_data=f"auto_reply_night_text:{connection_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text=f"⏱ Интервал: {interval_text}",
                    callback_data=f"auto_reply_interval:{connection_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text=f"☀️ Начало дня: {settings.get('day_start', '08:00')}",
                    callback_data=f"auto_reply_day_start:{connection_id}",
                ),
                InlineKeyboardButton(
                    text=f"☀️ Конец дня: {settings.get('day_end', '23:00')}",
                    callback_data=f"auto_reply_day_end:{connection_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=f"🌙 Начало ночи: {settings.get('night_start', '23:00')}",
                    callback_data=f"auto_reply_night_start:{connection_id}",
                ),
                InlineKeyboardButton(
                    text=f"🌙 Конец ночи: {settings.get('night_end', '08:00')}",
                    callback_data=f"auto_reply_night_end:{connection_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="◀️ Назад",
                    callback_data="back_to_main",
                )
            ],
        ]
    )


async def _auto_replies_text(connection_id: str):
    settings = await get_auto_reply_settings(connection_id)

    enabled = bool(settings.get("enabled", 0))

    day_text = settings.get("day_text") or "Не задан"
    night_text = settings.get("night_text") or "Не задан"

    if len(day_text) > 500:
        day_text = day_text[:500] + "…"
    if len(night_text) > 500:
        night_text = night_text[:500] + "…"

    status = "🟢 ВКЛЮЧЕНЫ" if enabled else "🔴 ВЫКЛЮЧЕНЫ"

    return (
        "🤖 <b>Автоответы</b>\n\n"
        f"<b>Статус:</b> {status}\n\n"
        "☀️ <b>Дневной автоответ:</b>\n"
        f"{escape(str(day_text))}\n\n"
        "🌙 <b>Ночной автоответ:</b>\n"
        f"{escape(str(night_text))}\n\n"
        f"⏱ <b>Интервал:</b> "
        f"{settings.get('interval_minutes', 30)} мин.\n"
        f"☀️ <b>День:</b> "
        f"{settings.get('day_start', '08:00')} — "
        f"{settings.get('day_end', '23:00')}\n"
        f"🌙 <b>Ночь:</b> "
        f"{settings.get('night_start', '23:00')} — "
        f"{settings.get('night_end', '08:00')}\n\n"
        "<i>Интервал считается отдельно для каждого чата. Время периодов указано по Москве (МСК, UTC+3).</i>"
    ), settings


async def show_auto_replies(callback: CallbackQuery):
    user_id = callback.from_user.id
    if not await user_has_access(user_id):
        await callback.answer("⛔ Нет активного доступа.", show_alert=True)
        return

    # Для обычного пользователя показываем его подключение, если оно одно.
    # Если подключений несколько, предлагаем выбрать конкретное.
    async with aiosqlite.connect(PANIC_DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT business_connection_id
            FROM business_connections
            WHERE telegram_id = ? AND is_enabled = 1
            ORDER BY created_at ASC
            """,
            (user_id,),
        )
        rows = await cursor.fetchall()

    if not rows:
        await callback.message.edit_text(
            "🤖 <b>Автоответы</b>\n\n"
            "Сначала подключи Telegram Business.\n\n"
            "Нажми «🔗 Подключение», чтобы открыть инструкцию.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="🔗 Подключение",
                            callback_data="connection_tutorial",
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            text="◀️ Назад",
                            callback_data="back_to_main",
                        )
                    ],
                ]
            ),
        )
        await callback.answer()
        return

    if len(rows) > 1:
        buttons = [
            [
                InlineKeyboardButton(
                    text=f"🔗 Подключение {i}",
                    callback_data=f"auto_reply_select:{row[0]}",
                )
            ]
            for i, row in enumerate(rows, 1)
        ]
        buttons.append([
            InlineKeyboardButton(
                text="◀️ Назад",
                callback_data="back_to_main",
            )
        ])
        await callback.message.edit_text(
            "🤖 <b>Автоответы</b>\n\n"
            "Выбери Business Connection, для которого хочешь настроить автоответы:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        )
        await callback.answer()
        return

    connection_id = rows[0][0]
    text, settings = await _auto_replies_text(connection_id)
    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=auto_replies_keyboard(settings, connection_id),
    )
    await callback.answer()


def _parse_hhmm(value: str):
    if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", value):
        return None
    return value


def _auto_reply_period(settings, now_hhmm):
    def in_period(start, end):
        if start == end:
            return True
        if start < end:
            return start <= now_hhmm < end
        return now_hhmm >= start or now_hhmm < end

    if in_period(
        settings.get("day_start", "08:00"),
        settings.get("day_end", "23:00"),
    ):
        return "day"

    if in_period(
        settings.get("night_start", "23:00"),
        settings.get("night_end", "08:00"),
    ):
        return "night"

    return None


async def handle_auto_reply(message: Message, owner_id: int, connection_id: str):
    """
    Автоответы для входящих Business-сообщений.
    Онлайн-статус владельца намеренно НЕ проверяется:
    Telegram Bot API не предоставляет его для Business Connection.
    """

    sender_id = message.from_user.id if message.from_user else None

    if sender_id is None or sender_id == owner_id:
        return

    settings = await get_auto_reply_settings(connection_id)

    if not settings.get("enabled"):
        return

    # Индивидуальное исключение для пары Business Connection + чат.
    if await is_auto_reply_chat_disabled(connection_id, message.chat.id):
        return

    now = datetime.now(timezone(timedelta(hours=3)))
    now_hhmm = now.strftime("%H:%M")

    period = _auto_reply_period(settings, now_hhmm)

    if period == "day":
        reply_text = (settings.get("day_text") or "").strip()
    elif period == "night":
        reply_text = (settings.get("night_text") or "").strip()
    else:
        return

    if not reply_text:
        return

    interval = int(settings.get("interval_minutes", 30) or 30)
    if interval < 1:
        interval = 1

    if not await claim_auto_reply_cooldown(
        business_connection_id=connection_id,
        chat_id=message.chat.id,
        interval_minutes=interval,
    ):
        return

    try:
        await bot.send_message(
            chat_id=message.chat.id,
            text=reply_text,
            business_connection_id=connection_id,
        )

        print(
            f"🤖 Автоответ отправлен: "
            f"connection={connection_id}, "
            f"chat={message.chat.id}, "
            f"period={period}, "
            f"interval={interval}m"
        )

    except Exception as e:
        # Если Telegram не принял сообщение, возвращаем возможность
        # повторить отправку при следующем входящем сообщении.
        await reset_auto_reply_cooldown(
            business_connection_id=connection_id,
            chat_id=message.chat.id,
        )

        print(
            f"❌ Ошибка отправки автоответа: {e}"
        )


async def auto_reply_day_text_callback(callback: CallbackQuery, state: FSMContext):
    connection_id = callback.data.split(':', 1)[1]
    user_id = callback.from_user.id
    if not await user_has_access(user_id):
        await callback.answer("⛔ Нет активного доступа.", show_alert=True)
        return

    if await get_user_by_connection(connection_id) != user_id:
        await callback.answer("⛔ Это подключение тебе не принадлежит.", show_alert=True)
        return

    await state.clear()
    await state.update_data(auto_reply_field="day_text", auto_reply_connection_id=connection_id)
    await state.set_state(AutoReplyStates.waiting_day_text)
    await callback.message.edit_text(
        "☀️ <b>Дневной автоответ</b>\n\n"
        "Отправь текст, который бот будет отправлять днём.\n"
        "Максимум 4000 символов.\n\n"
        "Для отмены: <code>/cancel</code>",
        parse_mode="HTML",
    )
    await callback.answer()


async def auto_reply_night_text_callback(callback: CallbackQuery, state: FSMContext):
    connection_id = callback.data.split(':', 1)[1]
    user_id = callback.from_user.id
    if not await user_has_access(user_id):
        await callback.answer("⛔ Нет активного доступа.", show_alert=True)
        return

    if await get_user_by_connection(connection_id) != user_id:
        await callback.answer("⛔ Это подключение тебе не принадлежит.", show_alert=True)
        return

    await state.clear()
    await state.set_state(AutoReplyStates.waiting_night_text)
    await state.update_data(auto_reply_connection_id=connection_id)
    await callback.message.edit_text(
        "🌙 <b>Ночной автоответ</b>\n\n"
        "Отправь текст, который бот будет отправлять ночью.\n"
        "Максимум 4000 символов.\n\n"
        "Для отмены: <code>/cancel</code>",
        parse_mode="HTML",
    )
    await callback.answer()


async def auto_reply_interval_callback(callback: CallbackQuery):
    connection_id = callback.data.split(':', 1)[1]
    user_id = callback.from_user.id
    if not await user_has_access(user_id):
        await callback.answer("⛔ Нет активного доступа.", show_alert=True)
        return

    if await get_user_by_connection(connection_id) != user_id:
        await callback.answer("⛔ Это подключение тебе не принадлежит.", show_alert=True)
        return

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="5 мин.", callback_data=f"auto_reply_set_interval:5:{connection_id}"),
                InlineKeyboardButton(text="15 мин.", callback_data=f"auto_reply_set_interval:15:{connection_id}"),
            ],
            [
                InlineKeyboardButton(text="30 мин.", callback_data=f"auto_reply_set_interval:30:{connection_id}"),
                InlineKeyboardButton(text="1 час", callback_data=f"auto_reply_set_interval:60:{connection_id}"),
            ],
            [
                InlineKeyboardButton(text="2 часа", callback_data=f"auto_reply_set_interval:120:{connection_id}"),
                InlineKeyboardButton(text="6 часов", callback_data=f"auto_reply_set_interval:360:{connection_id}"),
            ],
            [
                InlineKeyboardButton(text="✏️ Свой интервал", callback_data=f"auto_reply_custom_interval:{connection_id}"),
            ],
            [
                InlineKeyboardButton(text="◀️ Назад", callback_data="auto_replies"),
            ],
        ]
    )
    await callback.message.edit_text(
        "⏱ <b>Интервал автоответов</b>\n\n"
        "Интервал действует отдельно в каждом чате.\n"
        "Например, 30 минут означает: после ответа в конкретном чате "
        "следующий автоответ в этом же чате будет не раньше чем через 30 минут.",
        parse_mode="HTML",
        reply_markup=kb,
    )
    await callback.answer()


async def auto_reply_time_callback(callback: CallbackQuery, state: FSMContext, field: str):
    user_id = callback.from_user.id
    if not await user_has_access(user_id):
        await callback.answer("⛔ Нет активного доступа.", show_alert=True)
        return

    connection_id = callback.data.split(":", 1)[1]
    if await get_user_by_connection(connection_id) != user_id:
        await callback.answer("⛔ Это подключение тебе не принадлежит.", show_alert=True)
        return

    prompts = {
        "day_start": "☀️ Введи начало дня в формате <code>HH:MM</code>, например <code>08:00</code>.",
        "day_end": "☀️ Введи конец дня в формате <code>HH:MM</code>, например <code>23:00</code>.",
        "night_start": "🌙 Введи начало ночи в формате <code>HH:MM</code>, например <code>23:00</code>.",
        "night_end": "🌙 Введи конец ночи в формате <code>HH:MM</code>, например <code>08:00</code>.",
    }
    state_map = {
        "day_start": AutoReplyStates.waiting_day_start,
        "day_end": AutoReplyStates.waiting_day_end,
        "night_start": AutoReplyStates.waiting_night_start,
        "night_end": AutoReplyStates.waiting_night_end,
    }
    await state.clear()
    await state.set_state(state_map[field])
    await state.update_data(auto_reply_field=field, auto_reply_connection_id=connection_id)
    await callback.message.edit_text(
        prompts[field] + "\n\nДля отмены: <code>/cancel</code>",
        parse_mode="HTML",
    )
    await callback.answer()



async def auto_reply_toggle_callback(callback: CallbackQuery):
    connection_id = callback.data.split(':', 1)[1]
    # Toggle uses the exact selected connection.
    # connection in callback data on selection, but Telegram callback data
    # is intentionally kept short. We use the user's first active connection
    # as the default for the toggle; multi-connection users can select one.
    user_id = callback.from_user.id
    if not await user_has_access(user_id):
        await callback.answer("⛔ Нет активного доступа.", show_alert=True)
        return

    owner_id = await get_user_by_connection(connection_id)
    if owner_id != user_id:
        await callback.answer("⛔ Это подключение тебе не принадлежит.", show_alert=True)
        return

    settings = await get_auto_reply_settings(connection_id)
    await update_auto_reply_settings(
        connection_id,
        enabled=0 if settings.get("enabled") else 1,
    )
    text, settings = await _auto_replies_text(connection_id)
    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=auto_replies_keyboard(settings, connection_id),
    )
    await callback.answer()


async def _save_auto_reply_text_message(message: Message, state: FSMContext, field: str):
    if not message.from_user or not message.text:
        return

    user_id = message.from_user.id
    if not await user_has_access(user_id):
        await state.clear()
        return

    value = message.text.strip()

    if value == "/cancel":
        await state.clear()
        await message.answer("❌ Изменение отменено.", reply_markup=main_keyboard())
        return

    if not value:
        await message.answer("❌ Текст не может быть пустым.")
        return

    if len(value) > 4000:
        await message.answer("❌ Максимум 4000 символов.")
        return

    state_data = await state.get_data()
    connection_id = state_data.get("auto_reply_connection_id")

    if not connection_id:
        await state.clear()
        await message.answer("❌ Business Connection не выбран.", reply_markup=main_keyboard())
        return

    if await get_user_by_connection(connection_id) != user_id:
        await state.clear()
        await message.answer("❌ Это подключение тебе не принадлежит.", reply_markup=main_keyboard())
        return

    await update_auto_reply_settings(connection_id, **{field: value})
    await state.clear()
    await message.answer(
        "✅ Автоответ сохранён.",
        reply_markup=main_keyboard(),
    )


async def _save_auto_reply_number_message(message: Message, state: FSMContext, field: str):
    if not message.from_user or not message.text:
        return

    user_id = message.from_user.id
    if not await user_has_access(user_id):
        await state.clear()
        return

    value = message.text.strip()

    if value == "/cancel":
        await state.clear()
        await message.answer("❌ Изменение отменено.", reply_markup=main_keyboard())
        return

    if field == "interval_minutes":
        try:
            number = int(value)
        except ValueError:
            await message.answer("❌ Введи целое число минут.")
            return

        if not 1 <= number <= 10080:
            await message.answer("❌ Интервал должен быть от 1 до 10080 минут.")
            return
    else:
        if _parse_hhmm(value) is None:
            await message.answer(
                "❌ Неверный формат. Используй HH:MM, например 08:30."
            )
            return
        number = value

    state_data = await state.get_data()
    connection_id = state_data.get("auto_reply_connection_id")

    if not connection_id:
        await state.clear()
        await message.answer("❌ Business Connection не выбран.", reply_markup=main_keyboard())
        return

    if await get_user_by_connection(connection_id) != user_id:
        await state.clear()
        await message.answer("❌ Это подключение тебе не принадлежит.", reply_markup=main_keyboard())
        return

    await update_auto_reply_settings(connection_id, **{field: number})
    await state.clear()
    await message.answer(
        "✅ Настройка сохранена.",
        reply_markup=main_keyboard(),
    )


async def auto_reply_set_interval_callback(callback: CallbackQuery):
    try:
        parts = callback.data.split(":")
        minutes = int(parts[1])
        connection_id = parts[2]
    except Exception:
        await callback.answer("❌ Некорректный интервал.", show_alert=True)
        return

    user_id = callback.from_user.id
    if not await user_has_access(user_id):
        await callback.answer("⛔ Нет активного доступа.", show_alert=True)
        return

    if await get_user_by_connection(connection_id) != user_id:
        await callback.answer("⛔ Это подключение тебе не принадлежит.", show_alert=True)
        return

    await update_auto_reply_settings(connection_id, interval_minutes=minutes)
    text, settings = await _auto_replies_text(connection_id)
    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=auto_replies_keyboard(settings, connection_id),
    )
    await callback.answer("✅ Интервал изменён.")


async def auto_reply_custom_interval_callback(callback: CallbackQuery, state: FSMContext):
    connection_id = callback.data.split(':', 1)[1]
    user_id = callback.from_user.id
    if not await user_has_access(user_id):
        await callback.answer("⛔ Нет активного доступа.", show_alert=True)
        return

    if await get_user_by_connection(connection_id) != user_id:
        await callback.answer("⛔ Это подключение тебе не принадлежит.", show_alert=True)
        return

    await state.clear()
    await state.set_state(AutoReplyStates.waiting_interval)
    await state.update_data(auto_reply_connection_id=connection_id)
    await callback.message.edit_text(
        "✏️ <b>Свой интервал</b>\n\n"
        "Введи количество минут от 1 до 10080.\n"
        "Например: <code>30</code>.\n\n"
        "Для отмены: <code>/cancel</code>",
        parse_mode="HTML",
    )
    await callback.answer()


async def auto_reply_select_callback(callback: CallbackQuery):
    connection_id = callback.data.split(":", 1)[1]
    user_id = callback.from_user.id
    owner_id = await get_user_by_connection(connection_id)
    if owner_id != user_id:
        await callback.answer("⛔ Это подключение тебе не принадлежит.", show_alert=True)
        return

    text, settings = await _auto_replies_text(connection_id)
    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=auto_replies_keyboard(settings, connection_id),
    )
    await callback.answer()


async def subscription_callback(
    callback: CallbackQuery,
):
    telegram_id = callback.from_user.id
    status = await get_subscription_status(telegram_id)

    if status["status"] == "owner":
        text = (
            "👑 <b>Владелец</b>\n\n"
            "Постоянный доступ."
        )
        keyboard = back_keyboard()

    elif status["status"] == "lifetime":
        text = (
            "♾️ <b>Пожизненная подписка</b>\n\n"
            "Статус: 🟢 Активна\n"
            "Действует: <b>навсегда</b>."
        )
        keyboard = back_keyboard()

    elif status["status"] == "trial":
        text = (
            "🎁 <b>Пробный период</b>\n\n"
            "Статус: 🟢 Активен\n"
            "Длительность: 7 дней\n\n"
            "Действует до: "
            f"<code>{escape(format_subscription_date(status['expires_at']))}</code>\n\n"
            "⭐ <b>Тарифы подписки</b>\n"
            "Выбери нужный срок ниже:"
        )
        keyboard = subscription_keyboard()

    elif status["status"] == "active":
        text = (
            "⭐ <b>Подписка</b>\n\n"
            "Статус: 🟢 Активна\n"
            "Действует до: "
            f"<code>{escape(format_subscription_date(status['expires_at']))}</code>\n\n"
            "Ты можешь оформить подписку ещё раз — новый срок "
            "<b>добавится к текущему</b>.\n\n"
            "⭐ <b>Тарифы</b>:"
        )
        keyboard = subscription_keyboard()

    else:
        text = (
            "⭐ <b>Подписка</b>\n\n"
            "Статус: 🔴 Неактивна\n\n"
            "Выбери срок подписки ниже:"
        )
        keyboard = subscription_keyboard()

    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=keyboard,
    )
    await callback.answer()


# =========================================================
# PAYMENTS / SUBSCRIPTION
# =========================================================

def format_subscription_date(value) -> str:
    if not value:
        return "—"

    try:
        from datetime import datetime
        return datetime.fromisoformat(str(value)).strftime("%d.%m.%Y")
    except (ValueError, TypeError):
        return str(value)[:10]


def format_mute_datetime(value) -> str:
    if not value:
        return "—"

    try:
        from datetime import datetime
        return datetime.fromisoformat(str(value)).strftime("%d.%m.%Y %H:%M")
    except (ValueError, TypeError):
        return str(value).replace("T", " ")[:16]


def subscription_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="1 месяц — 200 ⭐",
                    callback_data="pay_stars:month",
                )
            ],
            [
                InlineKeyboardButton(
                    text="3 месяца — 540 ⭐",
                    callback_data="pay_stars:3months",
                )
            ],
            [
                InlineKeyboardButton(
                    text="1 год — 2000 ⭐",
                    callback_data="pay_stars:year",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Навсегда — 2600 ⭐",
                    callback_data="pay_stars:lifetime",
                )
            ],
            [
                InlineKeyboardButton(
                    text="◀️ Назад",
                    callback_data="back_to_main",
                )
            ],
        ]
    )


def _subscription_status_text(status: dict) -> str:
    if status["status"] == "owner":
        return "👑 <b>Владелец</b>\n\nПостоянный доступ."

    if status["status"] == "lifetime":
        return (
            "♾️ <b>Пожизненная подписка</b>\n\n"
            "Статус: 🟢 Активна\n"
            "Действует: <b>навсегда</b>."
        )

    if status["status"] == "trial":
        return (
            "🎁 <b>Пробный период</b>\n\n"
            "Статус: 🟢 Активен\n"
            "Действует до: "
            f"<code>{escape(format_subscription_date(status['expires_at']))}</code>\n\n"
            "Выбери тариф ниже."
        )

    if status["status"] == "active":
        return (
            "⭐ <b>Подписка</b>\n\n"
            "Статус: 🟢 Активна\n"
            "Действует до: "
            f"<code>{escape(format_subscription_date(status['expires_at']))}</code>\n\n"
            "Можно оформить следующий срок заранее — он добавится к текущему."
        )

    return "⭐ <b>Подписка</b>\n\nСтатус: 🔴 Неактивна\n\nВыбери тариф ниже."


async def create_stars_invoice_callback(
    callback: CallbackQuery,
):
    telegram_id = callback.from_user.id

    try:
        plan_id = callback.data.split(":", 1)[1]
    except (AttributeError, IndexError):
        await callback.answer("❌ Некорректный тариф.", show_alert=True)
        return

    plan = PAYMENT_PLANS.get(plan_id)
    if plan is None:
        await callback.answer("❌ Тариф не найден.", show_alert=True)
        return

    payload = f"stars:{telegram_id}:{plan_id}:{uuid4().hex}"

    try:
        payment_id = await create_payment(
            telegram_id=telegram_id,
            plan_id=plan_id,
            plan_name=plan["name"],
            payment_method="stars",
            amount=plan["stars"],
            currency="XTR",
            invoice_payload=payload,
        )

        await bot.send_invoice(
            chat_id=telegram_id,
            title=f"reefercm — {plan['name']}",
            description=(
                f"Подписка на {plan['name'].lower()}. "
                "Оплата производится Telegram Stars."
            ),
            payload=payload,
            currency="XTR",
            prices=[
                LabeledPrice(
                    label=plan["name"],
                    amount=plan["stars"],
                )
            ],
            provider_token="",
        )

        await callback.answer("⭐ Счёт на оплату отправлен.")

    except Exception as e:
        print(f"❌ Ошибка создания Stars invoice: {e}")
        try:
            await fail_payment(payment_id)
        except Exception:
            pass
        await callback.answer(
            "❌ Не удалось создать счёт. Попробуй ещё раз.",
            show_alert=True,
        )


async def pre_checkout_handler(
    query: PreCheckoutQuery,
):
    payment = await get_payment_by_payload(query.invoice_payload)

    if payment is None:
        await query.answer(
            ok=False,
            error_message="Платёж не найден. Открой оплату заново.",
        )
        return

    if payment["telegram_id"] != query.from_user.id:
        await query.answer(
            ok=False,
            error_message="Этот счёт принадлежит другому пользователю.",
        )
        return

    if payment["currency"] != "XTR":
        await query.answer(
            ok=False,
            error_message="Некорректная валюта платежа.",
        )
        return

    if payment["amount"] != query.total_amount:
        await query.answer(
            ok=False,
            error_message="Сумма платежа не совпадает с тарифом.",
        )
        return

    if payment["status"] != "pending":
        await query.answer(
            ok=False,
            error_message="Этот платёж уже обработан.",
        )
        return

    await query.answer(ok=True)


async def successful_payment_handler(
    message: Message,
):
    successful_payment = message.successful_payment
    if successful_payment is None:
        return

    payment = await get_payment_by_payload(
        successful_payment.invoice_payload
    )

    if payment is None:
        await message.answer(
            "⚠️ Платёж получен, но запись о нём не найдена. "
            "Свяжись с поддержкой."
        )
        return

    if payment["telegram_id"] != message.from_user.id:
        await message.answer(
            "⚠️ Платёж получен, но пользователь платежа не совпадает."
        )
        return

    if (
        successful_payment.currency != "XTR"
        or successful_payment.total_amount != payment["amount"]
    ):
        await message.answer(
            "⚠️ Платёж получен, но его параметры не совпадают с тарифом. "
            "Свяжись с поддержкой."
        )
        return

    result = await complete_payment(
        payment_id=payment["id"],
        telegram_payment_charge_id=(
            successful_payment.telegram_payment_charge_id
        ),
    )

    if not result.get("ok"):
        await message.answer(
            "⚠️ Оплата прошла, но не удалось активировать подписку. "
            "Свяжись с поддержкой."
        )
        return

    plan = PAYMENT_PLANS.get(payment["plan_id"])

    if payment["plan_id"] == "lifetime":
        expiration_text = "навсегда"
    else:
        status = await get_subscription_status(message.from_user.id)
        expiration_text = format_subscription_date(status.get("expires_at"))

    await message.answer(
        "✅ <b>Оплата успешно получена!</b>\n\n"
        f"Тариф: <b>{escape(plan['name'] if plan else payment['plan_name'])}</b>\n"
        f"Стоимость: <b>{payment['amount']} ⭐</b>\n"
        f"Подписка действует до: <b>{escape(expiration_text)}</b>",
        parse_mode="HTML",
        reply_markup=back_keyboard(),
    )


# =========================================================
# BACK
# =========================================================

async def back_to_main_callback(
    callback: CallbackQuery,
):

    await callback.message.edit_text(
        START_TEXT,
        parse_mode="HTML",
        reply_markup=main_keyboard(),
    )

    await callback.answer()


# =========================================================
# ADMIN USERS
# =========================================================

async def admin_users_callback(
    callback: CallbackQuery,
):

    if not is_owner(callback.from_user.id):

        await callback.answer(
            "⛔ Доступ запрещён.",
            show_alert=True,
        )

        return

    users = await get_all_users()

    lines = [
        "👥 <b>Пользователи</b>",
        "",
    ]

    for user in users[:50]:

        telegram_id = user[0]
        username = user[1]
        full_name = user[2]
        role = user[3]

        name = escape(
            full_name or "Без имени"
        )

        if username:
            name += f" (@{escape(username)})"

        icon = (
            "👑"
            if role == "owner"
            else "👤"
        )

        lines.append(
            f"{icon} <code>{telegram_id}</code> — {name}"
        )

    text = (
        "\n".join(lines)
        if users
        else "👥 <b>Пользователей пока нет.</b>"
    )

    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=admin_keyboard(
            await get_owner_notifications_enabled()
        ),
    )

    await callback.answer()


# =========================================================
# ADMIN STATS
# =========================================================

async def admin_stats_callback(
    callback: CallbackQuery,
):

    if not is_owner(callback.from_user.id):

        await callback.answer(
            "⛔ Доступ запрещён.",
            show_alert=True,
        )

        return

    stats = await get_user_statistics()

    text = (
        "📊 <b>Статистика</b>\n\n"
        f"👥 Пользователей: "
        f"<b>{stats['total_users']}</b>\n"
        f"🟢 Активных: "
        f"<b>{stats['active_users']}</b>\n"
        f"🎁 Trial: "
        f"<b>{stats['trials']}</b>\n"
        f"⭐ Подписок: "
        f"<b>{stats['subscriptions']}</b>"
    )

    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=admin_keyboard(
            await get_owner_notifications_enabled()
        ),
    )

    await callback.answer()


# =========================================================
# ADMIN GRANT
# =========================================================

async def admin_grant_callback(
    callback: CallbackQuery,
    state: FSMContext,
):

    if not is_owner(callback.from_user.id):

        await callback.answer(
            "⛔ Доступ запрещён.",
            show_alert=True,
        )

        return

    await state.set_state(
        AdminGrantStates.waiting_user_id
    )

    await callback.message.edit_text(
        "⭐ <b>Выдача подписки</b>\n\n"
        "Отправь Telegram ID пользователя.\n\n"
        "Для отмены:\n"
        "<code>/cancel</code>",
        parse_mode="HTML",
    )

    await callback.answer()


async def admin_grant_user_id(
    message: Message,
    state: FSMContext,
):

    if not message.from_user:
        return

    if not is_owner(message.from_user.id):

        await state.clear()

        return

    if not message.text:
        return

    text = message.text.strip()

    if text == "/cancel":

        await state.clear()

        await message.answer(
            "❌ Выдача отменена.",
            reply_markup=admin_keyboard(
                await get_owner_notifications_enabled()
            ),
        )

        return

    try:

        target_user_id = int(text)

    except ValueError:

        await message.answer(
            "❌ Telegram ID должен быть числом."
        )

        return

    target_user = await get_user(
        target_user_id
    )

    if not target_user:

        await state.clear()

        await message.answer(
            "❌ Пользователь не найден.\n\n"
            "Попроси его сначала отправить "
            "<code>/start</code>.",
            parse_mode="HTML",
        )

        return

    await state.update_data(
        target_user_id=target_user_id
    )

    await state.set_state(None)

    await message.answer(
        "⭐ <b>Выдача подписки</b>\n\n"
        f"Пользователь: <code>{target_user_id}</code>\n\n"
        "Выбери срок:",
        parse_mode="HTML",
        reply_markup=grant_keyboard(),
    )


async def grant_subscription_callback(
    callback: CallbackQuery,
    state: FSMContext,
):

    if not is_owner(callback.from_user.id):

        await callback.answer(
            "⛔ Доступ запрещён.",
            show_alert=True,
        )

        return

    data = await state.get_data()

    target_user_id = data.get(
        "target_user_id"
    )

    if not target_user_id:

        await callback.answer(
            "❌ Пользователь не выбран.",
            show_alert=True,
        )

        return

    try:

        days = int(
            callback.data.replace(
                "grant_",
                "",
            )
        )

    except ValueError:

        await callback.answer(
            "❌ Некорректный срок.",
            show_alert=True,
        )

        return

    try:

        expiration = await grant_subscription(
            telegram_id=target_user_id,
            days=days,
        )

    except ValueError:

        await callback.answer(
            "❌ Пользователь не найден.",
            show_alert=True,
        )

        return

    await state.clear()

    await callback.message.edit_text(
        "✅ <b>Подписка выдана!</b>\n\n"
        f"👤 ID:\n"
        f"<code>{target_user_id}</code>\n\n"
        f"⭐ Срок: <b>{days} дней</b>\n\n"
        f"📅 До:\n"
        f"<code>{escape(str(expiration))}</code>",
        parse_mode="HTML",
        reply_markup=admin_keyboard(
            await get_owner_notifications_enabled()
        ),
    )

    if bot:

        try:

            await bot.send_message(
                chat_id=target_user_id,
                text=(
                    "🎉 <b>Тебе выдана подписка!</b>\n\n"
                    f"⭐ Срок: <b>{days} дней</b>\n"
                    f"📅 До:\n"
                    f"<code>{escape(str(expiration))}</code>"
                ),
                parse_mode="HTML",
            )

        except Exception as e:

            print(
                f"⚠️ Не удалось уведомить "
                f"{target_user_id}: {e}"
            )

    await callback.answer(
        "✅ Подписка выдана."
    )


async def admin_cancel_grant_callback(
    callback: CallbackQuery,
    state: FSMContext,
):

    if not is_owner(callback.from_user.id):

        await callback.answer(
            "⛔ Доступ запрещён.",
            show_alert=True,
        )

        return

    await state.clear()

    await callback.message.edit_text(
        "👑 <b>Админ-панель</b>\n\n"
        "Выбери действие:",
        parse_mode="HTML",
        reply_markup=admin_keyboard(
            await get_owner_notifications_enabled()
        ),
    )

    await callback.answer()


# =========================================================
# ADMIN TOGGLE USER NOTIFICATIONS
# =========================================================

async def admin_toggle_notifications_callback(
    callback: CallbackQuery,
):

    if not is_owner(callback.from_user.id):

        await callback.answer(
            "⛔ Доступ запрещён.",
            show_alert=True,
        )

        return

    current = (
        await get_owner_notifications_enabled()
    )

    new_value = not current

    await set_owner_notifications_enabled(
        new_value
    )

    status = (
        "🟢 ВКЛ"
        if new_value
        else "🔴 ВЫКЛ"
    )

    await callback.message.edit_text(
        "👑 <b>Админ-панель</b>\n\n"
        "🔔 Уведомления от других пользователей: "
        f"<b>{status}</b>",
        parse_mode="HTML",
        reply_markup=admin_keyboard(
            new_value
        ),
    )

    await callback.answer(
        "🔔 Уведомления включены."
        if new_value
        else
        "🔕 Уведомления выключены."
    )


# =========================================================
# ADMIN TOGGLE ONE-TIME MEDIA
# =========================================================

async def admin_toggle_one_time_callback(
    callback: CallbackQuery,
):

    global one_time_media_notifications_enabled

    if not is_owner(callback.from_user.id):

        await callback.answer(
            "⛔ Доступ запрещён.",
            show_alert=True,
        )

        return

    one_time_media_notifications_enabled = (
        not one_time_media_notifications_enabled
    )

    status = (
        "🟢 ВКЛ"
        if one_time_media_notifications_enabled
        else "🔴 ВЫКЛ"
    )

    notifications_enabled = (
        await get_owner_notifications_enabled()
    )

    await callback.message.edit_text(
        "👑 <b>Админ-панель</b>\n\n"
        "📦 Получение одноразовых медиа OWNER: "
        f"<b>{status}</b>\n\n"
        "Поддерживаются:\n"
        "📸 Фото\n"
        "🎥 Видео\n"
        "🔵 Кружки\n"
        "🎞 GIF",
        parse_mode="HTML",
        reply_markup=admin_keyboard(
            notifications_enabled
        ),
    )

    await callback.answer(
        "📦 Настройка изменена."
    )

    print(
        "📦 Настройка одноразовых медиа OWNER: "
        f"{status}"
    )


# =========================================================
# EXTRACT MEDIA
# =========================================================

def extract_message_data(
    message: Message,
):

    media_type = None
    file_id = None
    file_name = None
    mime_type = None
    file_size = None
    duration = None
    width = None
    height = None

    text = message.text
    caption = message.caption

    if message.photo:

        media_type = "photo"

        item = message.photo[-1]

        file_id = item.file_id
        width = item.width
        height = item.height
        file_size = item.file_size

    elif message.voice:

        media_type = "voice"

        item = message.voice

        file_id = item.file_id
        duration = item.duration
        mime_type = item.mime_type
        file_size = item.file_size

    elif message.document:

        media_type = "document"

        item = message.document

        file_id = item.file_id
        file_name = item.file_name
        mime_type = item.mime_type
        file_size = item.file_size

    elif message.video_note:

        media_type = "video_note"

        item = message.video_note

        file_id = item.file_id
        duration = item.duration
        width = item.length
        height = item.length
        file_size = item.file_size

    elif message.video:

        media_type = "video"

        item = message.video

        file_id = item.file_id
        file_name = item.file_name
        mime_type = item.mime_type
        file_size = item.file_size
        duration = item.duration
        width = item.width
        height = item.height

    elif message.audio:

        media_type = "audio"

        item = message.audio

        file_id = item.file_id
        file_name = item.file_name
        mime_type = item.mime_type
        file_size = item.file_size
        duration = item.duration

    elif message.animation:

        media_type = "animation"

        item = message.animation

        file_id = item.file_id
        file_name = item.file_name
        mime_type = item.mime_type
        file_size = item.file_size
        duration = item.duration
        width = item.width
        height = item.height

    elif message.sticker:

        media_type = "sticker"

        item = message.sticker

        file_id = item.file_id
        width = item.width
        height = item.height

    elif message.text:

        media_type = "text"

    return {
        "text": text,
        "media_type": media_type,
        "file_id": file_id,
        "file_name": file_name,
        "mime_type": mime_type,
        "file_size": file_size,
        "duration": duration,
        "width": width,
        "height": height,
        "caption": caption,
    }


# =========================================================
# GET REPLY MEDIA
# =========================================================

def extract_reply_media(
    reply: Message,
):

    print()
    print("=" * 60)
    print("🔍 ПРОВЕРКА REPLY MEDIA")
    print("=" * 60)

    print(
        f"Reply Message ID: "
        f"{reply.message_id}"
    )

    print(
        f"has_protected_content: "
        f"{reply.has_protected_content}"
    )

    if not reply.has_protected_content:

        print(
            "🚫 Это обычное сообщение/медиа."
        )

        print(
            "🚫 Копия по Reply НЕ отправляется."
        )

        print("=" * 60)

        return None

    print(
        "✅ Обнаружено защищённое / одноразовое медиа."
    )

    if reply.photo:

        item = reply.photo[-1]

        print("📸 Тип: PHOTO")
        print("=" * 60)

        return {
            "type": "photo",
            "file_id": item.file_id,
            "filename": (
                f"one_time_photo_"
                f"{reply.message_id}.jpg"
            ),
        }

    if reply.video:

        item = reply.video

        print("🎥 Тип: VIDEO")
        print("=" * 60)

        return {
            "type": "video",
            "file_id": item.file_id,
            "filename": (
                item.file_name
                or
                f"one_time_video_{reply.message_id}.mp4"
            ),
        }

    if reply.video_note:

        item = reply.video_note

        print("🔵 Тип: VIDEO NOTE")
        print("=" * 60)

        return {
            "type": "video_note",
            "file_id": item.file_id,
            "filename": (
                f"one_time_video_note_"
                f"{reply.message_id}.mp4"
            ),
        }

    if reply.animation:

        item = reply.animation

        print("🎞 Тип: ANIMATION")
        print("=" * 60)

        return {
            "type": "animation",
            "file_id": item.file_id,
            "filename": (
                item.file_name
                or
                f"one_time_animation_{reply.message_id}.mp4"
            ),
        }

    print(
        "ℹ️ Защищённое сообщение найдено, "
        "но поддерживаемого медиа в нём нет."
    )

    print("=" * 60)

    return None


# =========================================================
# DOWNLOAD FILE TO RAM
# =========================================================

async def download_file_to_ram(
    file_id: str,
):

    if bot is None:
        return None

    try:

        telegram_file = await bot.get_file(
            file_id
        )

        if not telegram_file.file_path:

            print(
                "❌ Telegram не вернул file_path."
            )

            return None

        print(
            f"📥 GET_FILE: "
            f"{telegram_file.file_path}"
        )

        buffer = BytesIO()

        await bot.download_file(
            file_path=telegram_file.file_path,
            destination=buffer,
        )

        buffer.seek(0)

        data = buffer.getvalue()

        buffer.close()

        if not data:

            print(
                "❌ Telegram вернул пустые данные."
            )

            return None

        print(
            f"✅ Файл загружен в RAM: "
            f"{len(data)} байт"
        )

        return data

    except Exception as e:

        print(
            "❌ Ошибка скачивания файла:"
        )

        print(
            repr(e)
        )

        return None


# =========================================================
# SEND MEDIA FROM RAM
# =========================================================

async def send_media_from_ram(
    recipient_id: int,
    media_type: str,
    media_bytes: bytes,
    filename: str,
    caption: str | None = None,
):

    if bot is None:
        return False

    try:

        input_file = BufferedInputFile(
            media_bytes,
            filename=filename,
        )

        if media_type == "photo":

            await bot.send_photo(
                chat_id=recipient_id,
                photo=input_file,
                caption=caption,
                parse_mode="HTML",
            )

        elif media_type == "video":

            await bot.send_video(
                chat_id=recipient_id,
                video=input_file,
                caption=caption,
                parse_mode="HTML",
            )

        elif media_type == "video_note":

            await bot.send_video_note(
                chat_id=recipient_id,
                video_note=input_file,
            )

            if caption:

                await bot.send_message(
                    chat_id=recipient_id,
                    text=caption,
                    parse_mode="HTML",
                )

        elif media_type == "animation":

            await bot.send_animation(
                chat_id=recipient_id,
                animation=input_file,
                caption=caption,
                parse_mode="HTML",
            )

        else:

            print(
                f"❌ Неподдерживаемый тип: "
                f"{media_type}"
            )

            return False

        return True

    except Exception as e:

        print(
            f"❌ Ошибка отправки "
            f"{media_type} из RAM:"
        )

        print(
            repr(e)
        )

        return False


# =========================================================
# ONE-TIME MEDIA FROM REPLY
# =========================================================

async def save_media_from_reply(
    message: Message,
    owner_id: int,
):

    if bot is None:
        return False

    reply = message.reply_to_message

    if reply is None:
        return False

    if not message.from_user:

        print(
            "⚠️ У Reply нет from_user."
        )

        return False

    recipient_user_id = message.from_user.id

    recipient_user_name = (
        message.from_user.full_name
        or
        "Неизвестный пользователь"
    )

    reply_media = extract_reply_media(
        reply
    )

    if reply_media is None:

        print(
            "ℹ️ Reply не является Reply на "
            "одноразовое/защищённое медиа."
        )

        return False

    media_type = reply_media["type"]
    file_id = reply_media["file_id"]
    filename = reply_media["filename"]

    if is_duplicate_one_time_reply(
        user_id=recipient_user_id,
        file_id=file_id,
    ):
        return False

    type_names = {
        "photo": "📸 Фото",
        "video": "🎥 Видео",
        "video_note": "🔵 Кружок",
        "animation": "🎞 GIF / Анимация",
    }

    type_name = type_names.get(
        media_type,
        "📦 Медиа",
    )

    print()
    print("=" * 60)
    print("📦 НАЙДЕНО ОДНОРАЗОВОЕ МЕДИА ЧЕРЕЗ REPLY")
    print("=" * 60)

    print(f"Тип: {type_name}")

    print(
        f"Reply Message ID: "
        f"{reply.message_id}"
    )

    print(f"File ID: {file_id}")

    print(
        f"Получатель: "
        f"{recipient_user_name}"
    )

    print(
        f"User ID: "
        f"{recipient_user_id}"
    )

    print(
        f"Business Owner ID: "
        f"{owner_id}"
    )

    print(
        "Protected: "
        f"{reply.has_protected_content}"
    )

    print("💾 Сохранение на диск: OFF")

    print(
        "🎯 Отправка: ТОЛЬКО пользователю, "
        "который ответил"
    )

    media_bytes = await download_file_to_ram(
        file_id
    )

    if not media_bytes:

        print(
            "❌ Не удалось получить медиа."
        )

        return False

    user_caption = (
        f"{type_name} "
        "<b>из одноразового сообщения</b>\n\n"
        "Ты ответил на это сообщение, "
        "поэтому бот отправил тебе копию."
    )

    user_sent = await send_media_from_ram(
        recipient_id=recipient_user_id,
        media_type=media_type,
        media_bytes=media_bytes,
        filename=filename,
        caption=user_caption,
    )

    if not user_sent:

        print(
            "❌ Не удалось отправить одноразовое медиа."
        )

        del media_bytes

        return False

    print(
        f"✅ {type_name} отправлено "
        f"только пользователю "
        f"{recipient_user_id}"
    )

    print(
        "🚫 OWNER-копия одноразового медиа отключена."
    )

    del media_bytes

    print(
        "🧹 Данные медиа удалены из RAM."
    )

    print("=" * 60)

    return True


# =========================================================
# BUSINESS CHAT / MUTE HELPERS
# =========================================================

MUTE_COMMAND_RE = re.compile(
    r"^\.mute\s+(\d+)\s*$",
    re.IGNORECASE,
)

UNMUTE_COMMAND_RE = re.compile(
    r"^\.unmute\s*$",
    re.IGNORECASE,
)

WARN_COMMAND_RE = re.compile(
    r"^\.warn\s+(\d+)\s*$",
    re.IGNORECASE,
)

SPAM_COMMAND_RE = re.compile(
    r"^\.spam\s+(\d+)\s+(.+)$",
    re.IGNORECASE | re.DOTALL,
)


PANIC_COMMAND_RE = re.compile(
    r"^\.panic\s+(all|\d+)$",
    re.IGNORECASE,
)

PANIC_MAX_DELETE_BATCH = 100

SPAM_MAX_MESSAGES = 100


async def delete_business_message(
    message: Message,
    connection_id: str,
    reason: str,
):

    if bot is None:
        print("❌ Bot ещё не инициализирован.")
        return False

    if not connection_id:
        print("❌ Нет Business Connection ID.")
        return False

    if not message.chat:
        print("❌ У сообщения нет chat.")
        return False

    if not message.message_id:
        print("❌ У сообщения нет message_id.")
        return False

    try:

        await mark_service_deleted_message(
            business_connection_id=connection_id,
            chat_id=message.chat.id,
            message_id=message.message_id,
            reason=reason,
        )

        print(
            f"🛡 Сообщение {message.message_id} "
            "помечено как служебное удаление."
        )

    except Exception as e:

        print(
            f"⚠️ Не удалось записать служебное удаление "
            f"{message.message_id}: {e}"
        )

    try:

        print()
        print("=" * 60)
        print("🗑 ПОПЫТКА УДАЛЕНИЯ BUSINESS-СООБЩЕНИЯ")
        print("=" * 60)

        print(
            f"Connection ID: {connection_id}"
        )

        print(
            f"Chat ID: {message.chat.id}"
        )

        print(
            f"Message ID: {message.message_id}"
        )

        print(
            f"Reason: {reason}"
        )

        print("=" * 60)

        result = await bot.delete_business_messages(
            business_connection_id=connection_id,
            message_ids=[
                message.message_id
            ],
        )

        print()
        print("=" * 60)
        print("✅ BUSINESS-СООБЩЕНИЕ УДАЛЕНО")
        print("=" * 60)

        print(
            f"Message ID: {message.message_id}"
        )

        print(
            f"Result: {result}"
        )

        print("=" * 60)

        return True

    except Exception as e:

        print()
        print("=" * 60)
        print("❌ ОШИБКА УДАЛЕНИЯ BUSINESS-СООБЩЕНИЯ")
        print("=" * 60)

        print(
            f"Connection ID: {connection_id}"
        )

        print(
            f"Chat ID: {message.chat.id}"
        )

        print(
            f"Message ID: {message.message_id}"
        )

        print(
            f"Reason: {reason}"
        )

        print(
            f"Ошибка: {e}"
        )

        print(
            f"Тип ошибки: {type(e).__name__}"
        )

        print("=" * 60)

        return False


async def send_business_chat_message(
    chat_id: int,
    connection_id: str,
    text: str,
):
    if bot is None or not connection_id:
        return False

    try:
        sent_message = await bot.send_message(
            chat_id=chat_id,
            business_connection_id=connection_id,
            text=text,
            parse_mode="HTML",
        )
        if sent_message and sent_message.message_id:
            await _remember_bot_sent_message(
                chat_id, connection_id, sent_message.message_id
            )
        return True
    except Exception as e:
        print(
            f"❌ Не удалось отправить Business-сообщение "
            f"в чат {chat_id}: {e}"
        )
        return False


async def send_business_chat_message_with_result(
    chat_id: int,
    connection_id: str,
    text: str,
):
    """Отправляет Business-сообщение и возвращает объект Message."""
    if bot is None or not connection_id:
        return None
    try:
        sent_message = await bot.send_message(
            chat_id=chat_id,
            business_connection_id=connection_id,
            text=text,
            parse_mode="HTML",
        )
        if sent_message and sent_message.message_id:
            await _remember_bot_sent_message(
                chat_id, connection_id, sent_message.message_id
            )
        return sent_message
    except Exception as e:
        print(
            f"❌ Не удалось отправить Business-сообщение "
            f"в чат {chat_id}: {e}"
        )
        return None


async def delete_business_message_by_id(
    chat_id: int,
    connection_id: str,
    message_id: int,
    reason: str,
):
    """Удаляет конкретное Business-сообщение по ID."""

    if bot is None or not connection_id or not message_id:
        return False

    try:
        await mark_service_deleted_message(
            business_connection_id=connection_id,
            chat_id=chat_id,
            message_id=message_id,
            reason=reason,
        )
    except Exception as e:
        print(
            f"⚠️ Не удалось пометить сообщение {message_id} "
            f"как служебное удаление: {e}"
        )

    try:
        await bot.delete_business_messages(
            business_connection_id=connection_id,
            message_ids=[message_id],
        )
        await _forget_bot_sent_message(
            chat_id, connection_id, message_id
        )
        print(
            f"🗑 Business-сообщение {message_id} удалено "
            f"({reason})."
        )
        return True

    except Exception as e:
        print(
            f"❌ Не удалось удалить Business-сообщение "
            f"{message_id}: {e}"
        )
        return False


async def edit_business_chat_message(
    chat_id: int,
    connection_id: str,
    message_id: int,
    text: str,
):
    """Изменяет ранее отправленное Business-сообщение."""

    if bot is None or not connection_id or not message_id:
        return False

    try:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            business_connection_id=connection_id,
            text=text,
            parse_mode="HTML",
        )
        return True

    except Exception as e:
        print(
            f"❌ Не удалось изменить Business-сообщение "
            f"{message_id}: {e}"
        )
        return False


# =========================================================
# SEND MUTE CONTROL TO OWNER
# =========================================================

async def send_mute_control_to_owner(
    owner_id: int,
    chat_id: int,
    connection_id: str,
    minutes: int,
    muted_until,
):
    """
    Отправляет сообщение с кнопкой Снять мут
    именно владельцу конкретного Business Connection.

    ВАЖНО:
    owner_id здесь НЕ заменяется на OWNER_ID.
    """

    if bot is None:
        return False

    keyboard = mute_control_keyboard(
        owner_id=owner_id,
        chat_id=chat_id,
        business_connection_id=connection_id,
    )

    if keyboard is None:

        print(
            "❌ Не удалось создать клавиатуру "
            "управления мутом."
        )

        return False

    text = (
        "🔇 <b>Пользователь замучен</b>\n\n"
        f"💬 <b>Chat ID:</b> "
        f"<code>{chat_id}</code>\n"
        f"🔗 <b>Business Connection:</b> "
        f"<code>{escape(str(connection_id))}</code>\n\n"
        f"⏱ <b>Мут:</b> {minutes} минут\n"
        f"📅 <b>До:</b>\n"
        f"<code>{escape(format_mute_datetime(muted_until))}</code>\n\n"
        "Для досрочного снятия мута нажми кнопку:"
    )

    try:

        print()
        print("=" * 60)
        print("🔘 ОТПРАВКА КНОПКИ СНЯТИЯ МУТА")
        print("=" * 60)

        print(
            f"Получатель: {owner_id}"
        )

        print(
            f"Chat ID: {chat_id}"
        )

        print(
            f"Connection ID: {connection_id}"
        )

        print(
            f"Minutes: {minutes}"
        )

        print(
            f"Until: {muted_until}"
        )

        print("=" * 60)

        await bot.send_message(
            chat_id=owner_id,
            text=text,
            parse_mode="HTML",
            reply_markup=keyboard,
        )

        print(
            f"✅ Кнопка снятия мута отправлена "
            f"владельцу {owner_id}"
        )

        return True

    except Exception as e:

        print()
        print("=" * 60)
        print("❌ НЕ УДАЛОСЬ ОТПРАВИТЬ КНОПКУ МУТА")
        print("=" * 60)

        print(
            f"Owner ID: {owner_id}"
        )

        print(
            f"Chat ID: {chat_id}"
        )

        print(
            f"Connection ID: {connection_id}"
        )

        print(
            f"Ошибка: {e}"
        )

        print(
            f"Тип: {type(e).__name__}"
        )

        print("=" * 60)

        return False


# =========================================================
# PANIC COMMAND
# =========================================================

async def _get_panic_message_ids(
    chat_id: int,
    connection_id: str,
    before_message_id: int | None = None,
    limit: int | None = None,
):
    """
    Берёт ID сообщений, которые бот ранее сохранил в SQLite.

    Telegram Bot API не предоставляет Business Bot отдельный метод
    для получения произвольной старой истории чата, поэтому .panic
    работает по сообщениям, которые уже были получены и сохранены ботом.
    """
    query = (
        "SELECT message_id "
        "FROM messages "
        "WHERE chat_id = ? AND business_connection_id = ?"
    )
    params = [chat_id, connection_id]

    if before_message_id is not None:
        query += " AND message_id < ?"
        params.append(before_message_id)

    query += " ORDER BY message_id DESC"

    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    try:
        async with aiosqlite.connect(PANIC_DB_PATH) as db:
            cursor = await db.execute(query, tuple(params))
            rows = await cursor.fetchall()
            await cursor.close()
        database_ids = [int(row[0]) for row in rows if row and row[0] is not None]
        bot_ids = await _get_bot_sent_message_ids(
            chat_id, connection_id, before_message_id
        )
        merged = sorted(set(database_ids + bot_ids), reverse=True)
        if limit is not None:
            merged = merged[:limit]
        return merged
    except Exception as e:
        print(f"❌ Ошибка получения истории для .panic: {e}")
        return []


async def _panic_delete_ids(
    chat_id: int,
    connection_id: str,
    message_ids: list[int],
    reason: str,
):
    """Удаляет Business-сообщения пачками максимум по 100 ID."""
    if bot is None or not connection_id or not message_ids:
        return 0

    unique_ids = list(dict.fromkeys(int(x) for x in message_ids))
    deleted_count = 0

    for start in range(0, len(unique_ids), PANIC_MAX_DELETE_BATCH):
        batch = unique_ids[start:start + PANIC_MAX_DELETE_BATCH]

        # Помечаем сообщения как служебно удаляемые, чтобы
        # BusinessMessagesDeleted не отправил ложные уведомления OWNER.
        for message_id in batch:
            try:
                await mark_service_deleted_message(
                    business_connection_id=connection_id,
                    chat_id=chat_id,
                    message_id=message_id,
                    reason=reason,
                )
            except Exception as e:
                print(
                    f"⚠️ Не удалось пометить {message_id} как служебное удаление: {e}"
                )

        try:
            result = await bot.delete_business_messages(
                business_connection_id=connection_id,
                message_ids=batch,
            )

            if result:
                for deleted_id in batch:
                    await _forget_bot_sent_message(
                        chat_id, connection_id, deleted_id
                    )
                deleted_count += len(batch)
                print(
                    f"🧹 .panic: удалена пачка {len(batch)} сообщений "
                    f"из чата {chat_id}."
                )
            else:
                print(
                    f"⚠️ .panic: Telegram не подтвердил удаление пачки. "
                    f"chat={chat_id}"
                )

        except Exception as e:
            print(
                f"❌ Ошибка удаления пачки .panic: "
                f"chat={chat_id}, count={len(batch)}, error={e}"
            )

    return deleted_count


async def handle_panic_command(
    message: Message,
    owner_id: int,
    connection_id: str,
):
    """
    .panic N   — удаляет N сообщений перед самой командой + саму команду.
    .panic all — удаляет всю сохранённую историю этого Business-чата + команду.

    Ограничение Telegram Bot API: за один запрос можно передать максимум
    100 message_id. Функция автоматически разбивает большие удаления.
    """
    if not message.text or not message.from_user:
        return False

    match = PANIC_COMMAND_RE.fullmatch(message.text.strip())
    if not match:
        return False

    if message.from_user.id != owner_id:
        print(
            f"⛔ .panic отклонён: from_user={message.from_user.id}, owner={owner_id}"
        )
        return True

    value = match.group(1).lower()

    if value == "all":
        message_ids = await _get_panic_message_ids(
            chat_id=message.chat.id,
            connection_id=connection_id,
            before_message_id=message.message_id,
        )
        mode_text = "всю сохранённую историю"
    else:
        count = int(value)
        if count <= 0:
            await send_business_chat_message(
                chat_id=message.chat.id,
                connection_id=connection_id,
                text="❌ Используй <code>.panic 1</code>, <code>.panic 10</code> или <code>.panic all</code>.",
            )
            return True

        message_ids = await _get_panic_message_ids(
            chat_id=message.chat.id,
            connection_id=connection_id,
            before_message_id=message.message_id,
            limit=count,
        )
        mode_text = f"{count} сообщений перед командой"

    # Сама команда тоже удаляется.
    message_ids.append(message.message_id)

    deleted_count = await _panic_delete_ids(
        chat_id=message.chat.id,
        connection_id=connection_id,
        message_ids=message_ids,
        reason="panic_command",
    )

    await send_business_chat_message(
        chat_id=message.chat.id,
        connection_id=connection_id,
        text=(
            "🧹 <b>PANIC выполнен.</b>\n\n"
            f"Удалено: <b>{deleted_count}</b> сообщений.\n"
            f"Режим: <b>{escape(mode_text)}</b>\n\n"
            "⚠️ Удаляются сообщения, которые бот ранее получил и сохранил."
        ),
    )

    print()
    print("=" * 60)
    print("🧹 PANIC COMMAND")
    print("=" * 60)
    print(f"Owner ID: {owner_id}")
    print(f"Chat ID: {message.chat.id}")
    print(f"Connection ID: {connection_id}")
    print(f"Mode: {value}")
    print(f"Selected IDs: {len(message_ids)}")
    print(f"Deleted: {deleted_count}")
    print("=" * 60)

    return True


# =========================================================
# SPAM COMMAND
# =========================================================

async def handle_spam_command(
    message: Message,
    owner_id: int,
    connection_id: str,
):
    """
    .spam N TEXT отправляет TEXT в Business-чат N раз.

    Ограничение намеренно простое:
    максимум 3 сообщения за один вызов команды.
    Задержек и ограничений по чатам нет.
    """

    if not message.text or not message.from_user:
        return False

    match = SPAM_COMMAND_RE.fullmatch(
        message.text.strip()
    )

    if not match:
        return False

    if message.from_user.id != owner_id:
        print(
            f"⛔ .spam отклонён: "
            f"from_user={message.from_user.id}, "
            f"owner={owner_id}"
        )
        return True

    try:
        count = int(match.group(1))
    except ValueError:
        return True

    text = match.group(2).strip()

    if count <= 0:
        await send_business_chat_message(
            chat_id=message.chat.id,
            connection_id=connection_id,
            text="❌ Количество сообщений должно быть больше 0.",
        )
        return True

    if count > SPAM_MAX_MESSAGES:
        await send_business_chat_message(
            chat_id=message.chat.id,
            connection_id=connection_id,
            text=(
                "❌ Максимум за один вызов .spam: "
                f"<b>{SPAM_MAX_MESSAGES}</b> сообщения.\n\n"
                "Используй, например: <code>.spam 3 текст</code>"
            ),
        )
        return True

    print()
    print("=" * 60)
    print("📨 SPAM COMMAND")
    print("=" * 60)
    print(f"Owner ID: {owner_id}")
    print(f"Chat ID: {message.chat.id}")
    print(f"Connection ID: {connection_id}")
    print(f"Count: {count}")
    print(f"Text: {text}")
    print("=" * 60)

    sent = 0

    for _ in range(count):
        if await send_business_chat_message(
            chat_id=message.chat.id,
            connection_id=connection_id,
            text=text,
        ):
            sent += 1

    print(
        f"✅ .spam завершён: отправлено {sent}/{count} сообщений."
    )

    # Удаляем саму команду .spam, чтобы она не оставалась в чате.
    await delete_business_message(
        message=message,
        connection_id=connection_id,
        reason="spam_command",
    )

    return True


# =========================================================
# MUTE COMMAND
# =========================================================

async def handle_mute_command(
    message: Message,
    owner_id: int,
    connection_id: str,
):

    if not message.text or not message.from_user:
        return False

    match = MUTE_COMMAND_RE.fullmatch(
        message.text.strip()
    )

    if not match:
        return False

    # =====================================================
    # ТОЛЬКО ВЛАДЕЛЕЦ ЭТОГО BUSINESS CONNECTION
    # =====================================================

    if message.from_user.id != owner_id:

        print(
            f"⛔ .mute отклонён: "
            f"from_user={message.from_user.id}, "
            f"owner={owner_id}"
        )

        return False

    minutes = int(
        match.group(1)
    )

    if minutes <= 0:
        return False

    # =====================================================
    # УДАЛЯЕМ КОМАНДУ
    # =====================================================

    await delete_business_message(
        message=message,
        connection_id=connection_id,
        reason="mute_command",
    )

    # =====================================================
    # УСТАНАВЛИВАЕМ МУТ
    # =====================================================

    muted_until = await set_chat_mute(
        owner_id=owner_id,
        chat_id=message.chat.id,
        business_connection_id=connection_id,
        minutes=minutes,
    )

    print()
    print("=" * 60)
    print("🔇 MUTE УСТАНОВЛЕН")
    print("=" * 60)

    print(
        f"Owner ID: {owner_id}"
    )

    print(
        f"Chat ID: {message.chat.id}"
    )

    print(
        f"Connection ID: {connection_id}"
    )

    print(
        f"Minutes: {minutes}"
    )

    print(
        f"Until: {muted_until}"
    )

    print("=" * 60)

    # =====================================================
    # УВЕДОМЛЕНИЕ СОБЕСЕДНИКУ
    # =====================================================

    await send_business_chat_message(
        chat_id=message.chat.id,
        connection_id=connection_id,
        text=(
            f"🔇 <b>Вам выдан мут на {minutes} минут.</b>\n\n"
        ),
    )

    # =====================================================
    # КНОПКА ВЛАДЕЛЬЦУ
    # =====================================================

    await send_mute_control_to_owner(
        owner_id=owner_id,
        chat_id=message.chat.id,
        connection_id=connection_id,
        minutes=minutes,
        muted_until=muted_until,
    )

    return True


# =========================================================
# WARN COMMAND
# =========================================================

async def handle_warn_command(
    message: Message,
    owner_id: int,
    connection_id: str,
):
    """
    .warn N выдаёт собеседнику N предупреждений.

    Каждое следующее сообщение собеседника уменьшает
    счётчик на единицу. После последнего сообщения
    предупреждение удаляется, а собеседнику автоматически
    выдаётся мут на 30 минут.
    """

    if not message.text or not message.from_user:
        return False

    match = WARN_COMMAND_RE.fullmatch(message.text.strip())

    if not match:
        return False

    if message.from_user.id != owner_id:
        print(
            f"⛔ .warn отклонён: "
            f"from_user={message.from_user.id}, owner={owner_id}"
        )
        return False

    warnings = int(match.group(1))

    if warnings <= 0:
        return False

    # Удаляем саму команду владельца.
    await delete_business_message(
        message=message,
        connection_id=connection_id,
        reason="warn_command",
    )

    # Если в этом чате уже был активный варн, удаляем
    # старое сообщение и заменяем состояние новым.
    previous_warning = await get_chat_warning(
        chat_id=message.chat.id,
        business_connection_id=connection_id,
    )

    if previous_warning:
        previous_message_id = previous_warning.get("warning_message_id")

        await clear_chat_warning(
            chat_id=message.chat.id,
            business_connection_id=connection_id,
        )

        if previous_message_id:
            await delete_business_message_by_id(
                chat_id=message.chat.id,
                connection_id=connection_id,
                message_id=previous_message_id,
                reason="replace_warning",
            )

    warning_text = (
        "⚠️ <b>Вам выдан варн.</b>\n\n"
        f"Число предупреждений: <b>{warnings}</b>"
    )

    warning_message = await send_business_chat_message_with_result(
        chat_id=message.chat.id,
        connection_id=connection_id,
        text=warning_text,
    )

    if warning_message is None:
        print(
            "❌ Не удалось отправить сообщение о варне. "
            "Состояние варна не сохранено."
        )
        return True

    await set_chat_warning(
        owner_id=owner_id,
        chat_id=message.chat.id,
        business_connection_id=connection_id,
        remaining=warnings,
        warning_message_id=warning_message.message_id,
    )

    print()
    print("=" * 60)
    print("⚠️ WARN УСТАНОВЛЕН")
    print("=" * 60)
    print(f"Owner ID: {owner_id}")
    print(f"Chat ID: {message.chat.id}")
    print(f"Connection ID: {connection_id}")
    print(f"Warnings: {warnings}")
    print(f"Warning message ID: {warning_message.message_id}")
    print("=" * 60)

    return True


async def process_chat_warning(
    message: Message,
    owner_id: int,
    connection_id: str,
):
    """Обрабатывает одно новое сообщение собеседника при активном варне."""

    warning = await get_chat_warning(
        chat_id=message.chat.id,
        business_connection_id=connection_id,
    )

    if not warning:
        return False

    result = await decrement_chat_warning(
        chat_id=message.chat.id,
        business_connection_id=connection_id,
    )

    if result is None:
        return False

    remaining = result["remaining"]
    warning_message_id = result["warning_message_id"]

    if remaining > 0:
        updated_text = (
            "⚠️ <b>Вам выдан варн.</b>\n\n"
            f"Число предупреждений: <b>{remaining}</b>"
        )

        if warning_message_id:
            edited = await edit_business_chat_message(
                chat_id=message.chat.id,
                connection_id=connection_id,
                message_id=warning_message_id,
                text=updated_text,
            )

            print(
                f"⚠️ WARN уменьшен: "
                f"chat={message.chat.id}, "
                f"remaining={remaining}, "
                f"edited={edited}"
            )

        return True

    # Последнее сообщение собеседника: удаляем карточку варна.
    if warning_message_id:
        await delete_business_message_by_id(
            chat_id=message.chat.id,
            connection_id=connection_id,
            message_id=warning_message_id,
            reason="warn_finished",
        )

    # После исчерпания предупреждений — фиксированный мут 30 минут.
    muted_until = await set_chat_mute(
        owner_id=owner_id,
        chat_id=message.chat.id,
        business_connection_id=connection_id,
        minutes=30,
    )

    await send_business_chat_message(
        chat_id=message.chat.id,
        connection_id=connection_id,
        text="🔇 <b>Вам выдан мут на 30 минут.</b>",
    )

    await send_mute_control_to_owner(
        owner_id=owner_id,
        chat_id=message.chat.id,
        connection_id=connection_id,
        minutes=30,
        muted_until=muted_until,
    )

    print()
    print("=" * 60)
    print("⚠️ WARN ЗАКОНЧИЛСЯ — ВЫДАН МУТ")
    print("=" * 60)
    print(f"Owner ID: {owner_id}")
    print(f"Chat ID: {message.chat.id}")
    print(f"Connection ID: {connection_id}")
    print("Mute: 30 minutes")
    print(f"Until: {muted_until}")
    print("=" * 60)

    return True


# =========================================================
# UNMUTE COMMAND
# =========================================================

async def handle_unmute_command(
    message: Message,
    owner_id: int,
    connection_id: str,
):

    if not message.text or not message.from_user:
        return False

    if not UNMUTE_COMMAND_RE.fullmatch(
        message.text.strip()
    ):
        return False

    if message.from_user.id != owner_id:

        print(
            f"⛔ .unmute отклонён: "
            f"from_user={message.from_user.id}, "
            f"owner={owner_id}"
        )

        return False

    await delete_business_message(
        message=message,
        connection_id=connection_id,
        reason="unmute_command",
    )

    previous_until = await clear_chat_mute(
        chat_id=message.chat.id,
        business_connection_id=connection_id,
    )

    if previous_until is None:

        text = (
            "🔊 <b>Мут сейчас не установлен.</b>"
        )

    else:

        text = (
            "🔊 <b>Мут снят. "
            "Вы снова можете отправлять сообщения.</b>"
        )

    await send_business_chat_message(
        chat_id=message.chat.id,
        connection_id=connection_id,
        text=text,
    )

    print(
        f"🔊 UNMUTE: owner={owner_id}, "
        f"chat={message.chat.id}, "
        f"connection={connection_id}"
    )

    return True


# =========================================================
# REMOVE MUTE BUTTON CALLBACK
# =========================================================

async def remove_mute_callback(
    callback: CallbackQuery,
):
    """
    Обработчик кнопки 🔊 Снять мут.

    Проверяет:
        1. что callback принадлежит OWNER конкретного Business;
        2. что owner_id совпадает с пользователем,
           который нажал кнопку;
        3. снимает мут именно в переданном chat_id;
        4. отправляет уведомление через Business Connection.
    """

    if not callback.from_user:

        await callback.answer(
            "❌ Не удалось определить пользователя.",
            show_alert=True,
        )

        return

    if not callback.data:

        await callback.answer(
            "❌ Некорректная кнопка.",
            show_alert=True,
        )

        return

    try:

        parts = callback.data.split(
            ":",
            3,
        )

        if len(parts) != 4:

            raise ValueError(
                "Неверный формат callback_data"
            )

        prefix = parts[0]
        owner_id = int(parts[1])
        chat_id = int(parts[2])
        connection_id = parts[3]

        if prefix != "remove_mute":

            raise ValueError(
                "Неверный callback prefix"
            )

    except Exception as e:

        print(
            f"❌ Ошибка разбора кнопки мута: {e}"
        )

        await callback.answer(
            "❌ Некорректная кнопка.",
            show_alert=True,
        )

        return

    # =====================================================
    # ГЛАВНАЯ ПРОВЕРКА
    # =====================================================

    if callback.from_user.id != owner_id:

        print()
        print("=" * 60)
        print("🚨 ПОПЫТКА СНЯТИЯ ЧУЖОГО МУТА")
        print("=" * 60)

        print(
            f"Нажал: {callback.from_user.id}"
        )

        print(
            f"Разрешённый владелец: {owner_id}"
        )

        print(
            f"Chat ID: {chat_id}"
        )

        print(
            f"Connection ID: {connection_id}"
        )

        print("=" * 60)

        await callback.answer(
            "⛔ Эта кнопка принадлежит другому владельцу.",
            show_alert=True,
        )

        return

    # =====================================================
    # ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА ДОСТУПА
    # =====================================================

    if not await user_has_access(owner_id):

        await callback.answer(
            "⛔ У владельца нет активного доступа.",
            show_alert=True,
        )

        return

    # =====================================================
    # ПРОВЕРЯЕМ МУТ
    # =====================================================

    current_mute = await get_chat_mute(
        chat_id=chat_id,
        business_connection_id=connection_id,
    )

    if not current_mute:

        try:

            await callback.message.edit_text(
                "🔊 <b>Мут уже снят.</b>\n\n"
                f"💬 Chat ID: <code>{chat_id}</code>",
                parse_mode="HTML",
            )

        except Exception:
            pass

        await callback.answer(
            "🔊 Мут уже снят.",
            show_alert=False,
        )

        return

    # =====================================================
    # СНЯТИЕ МУТА
    # =====================================================

    try:

        previous_until = await clear_chat_mute(
            chat_id=chat_id,
            business_connection_id=connection_id,
        )

    except Exception as e:

        print()
        print("=" * 60)
        print("❌ ОШИБКА СНЯТИЯ МУТА КНОПКОЙ")
        print("=" * 60)
        print(
            f"Owner: {owner_id}"
        )
        print(
            f"Chat: {chat_id}"
        )
        print(
            f"Connection: {connection_id}"
        )
        print(
            f"Ошибка: {e}"
        )
        print("=" * 60)

        await callback.answer(
            "❌ Не удалось снять мут.",
            show_alert=True,
        )

        return

    if previous_until is None:

        print(
            "ℹ️ clear_chat_mute вернул None: "
            "мут уже отсутствует."
        )

    # =====================================================
    # ОБНОВЛЯЕМ СООБЩЕНИЕ С КНОПКОЙ
    # =====================================================

    try:

        await callback.message.edit_text(
            "🔊 <b>Мут снят</b>\n\n"
            f"💬 <b>Chat ID:</b> "
            f"<code>{chat_id}</code>\n"
            f"🔗 <b>Business Connection:</b> "
            f"<code>{escape(str(connection_id))}</code>\n\n"
            "Пользователь снова может отправлять "
            "сообщения.",
            parse_mode="HTML",
        )

    except Exception as e:

        print(
            f"⚠️ Не удалось изменить сообщение "
            f"с кнопкой: {e}"
        )

    # =====================================================
    # УВЕДОМЛЕНИЕ СОБЕСЕДНИКУ
    # =====================================================

    notification_sent = await send_business_chat_message(
        chat_id=chat_id,
        connection_id=connection_id,
        text=(
            "🔊 <b>Мут снят.</b>\n\n"
            "Вы снова можете отправлять сообщения."
        ),
    )

    print()
    print("=" * 60)
    print("🔊 MUTЕ СНЯТ КНОПКОЙ")
    print("=" * 60)

    print(
        f"Owner ID: {owner_id}"
    )

    print(
        f"Chat ID: {chat_id}"
    )

    print(
        f"Connection ID: {connection_id}"
    )

    print(
        f"Previous until: {previous_until}"
    )

    print(
        f"Уведомление пользователю: "
        f"{notification_sent}"
    )

    print("=" * 60)

    await callback.answer(
        "🔊 Мут снят.",
        show_alert=False,
    )


# =========================================================
# MUTE EXPIRATION WORKER
# =========================================================

async def mute_expiration_worker():

    while True:

        try:

            expired_mutes = await get_expired_mutes()

            for mute in expired_mutes:

                await send_business_chat_message(
                    chat_id=mute["chat_id"],
                    connection_id=mute["business_connection_id"],
                    text=(
                        "🔊 <b>Мут закончился.</b>\n\n"
                        "Вы снова можете отправлять сообщения."
                    ),
                )

                print(
                    f"🔊 Мут автоматически завершён: "
                    f"owner={mute['owner_id']}, "
                    f"chat={mute['chat_id']}, "
                    f"connection={mute['business_connection_id']}"
                )

        except asyncio.CancelledError:
            raise

        except Exception as e:

            print(
                f"❌ Ошибка worker мута: {e}"
            )

        await asyncio.sleep(5)


# =========================================================
# NEW BUSINESS MESSAGE
# =========================================================

async def handle_auto_reply_command(message: Message, owner_id: int, connection_id: str):
    """Команды управления автоответами в конкретном Business-чате."""
    if not message.text or not message.from_user or message.from_user.id != owner_id:
        return False

    command = message.text.strip().casefold()
    if command not in {".autoreply_off", ".autoreply_on", ".autoreply_status"}:
        return False

    chat_id = message.chat.id

    if command == ".autoreply_off":
        await set_auto_reply_chat_disabled(connection_id, chat_id, True)
        response = "🔕 Автоответы отключены только в этом чате."
    elif command == ".autoreply_on":
        await set_auto_reply_chat_disabled(connection_id, chat_id, False)
        settings = await get_auto_reply_settings(connection_id)
        if settings.get("enabled"):
            response = "🔔 Автоответы снова разрешены в этом чате."
        else:
            response = (
                "🔔 Исключение для этого чата снято. "
                "Но общие автоответы сейчас выключены, поэтому ответы не отправляются."
            )
    else:
        settings = await get_auto_reply_settings(connection_id)
        chat_disabled = await is_auto_reply_chat_disabled(connection_id, chat_id)
        if chat_disabled:
            status = "🔴 Отключены в этом чате командой .autoreply_off"
        elif not settings.get("enabled"):
            status = "🔴 Общие автоответы выключены"
        else:
            status = "🟢 Разрешены в этом чате (при соблюдении расписания и интервала)"
        response = f"📊 <b>Статус автоответов</b>\\n\\n{status}"

    await delete_business_message(
        message=message,
        connection_id=connection_id,
        reason="auto_reply_command",
    )
    await send_business_chat_message(
        chat_id=chat_id,
        connection_id=connection_id,
        text=response,
    )
    return True


async def handle_custom_command(message: Message, owner_id: int, connection_id: str):
    if not message.text or not message.from_user or message.from_user.id != owner_id:
        return False
    trigger = message.text.strip()
    commands = await get_custom_commands(owner_id)
    matched_trigger = next(
        (key for key in commands if key.casefold() == trigger.casefold()),
        None,
    )
    if matched_trigger is None:
        return False
    actions = commands.get(matched_trigger) or []
    if not actions:
        return False
    print("📝 Выполнение заготовки:", matched_trigger)
    command_handled = False
    for action in actions[:CUSTOM_ACTIONS_MAX]:
        action = str(action).strip()
        if not action:
            continue
        if action.startswith("."):
            proxy = message.model_copy(update={"text": action, "caption": None})
            if await handle_spam_command(proxy, owner_id, connection_id):
                command_handled = True
                continue
            if await handle_mute_command(proxy, owner_id, connection_id):
                command_handled = True
                continue
            if await handle_unmute_command(proxy, owner_id, connection_id):
                command_handled = True
                continue
            if await handle_warn_command(proxy, owner_id, connection_id):
                command_handled = True
                continue
            if await handle_panic_command(proxy, owner_id, connection_id):
                command_handled = True
                continue
            print(f"⚠️ Неизвестная команда в заготовке: {action}")
            continue
        await send_business_chat_message(
            chat_id=message.chat.id,
            connection_id=connection_id,
            text=action,
        )
    if not command_handled:
        await delete_business_message(
            message=message,
            connection_id=connection_id,
            reason="custom_command_trigger",
        )
    else:
        # Встроенные обработчики могли уже удалить этот ID; повторная
        # попытка не обязательна. Если были только текстовые действия
        # вместе с командой, команда всё равно удаляет триггер.
        pass
    return True


async def handle_message(
    message: Message,
):

    connection_id = message.business_connection_id

    if not connection_id:

        print(
            "⚠️ Нет Business Connection ID."
        )

        return

    if is_duplicate_event(
        event_type="business_message",
        connection_id=connection_id,
        chat_id=message.chat.id,
        message_id=message.message_id,
    ):

        return

    # =====================================================
    # ОПРЕДЕЛЯЕМ ВЛАДЕЛЬЦА
    # =====================================================

    owner_id = await get_user_by_connection(
        business_connection_id=connection_id,
    )

    if owner_id is None:

        print(
            "⚠️ Владелец Business Connection "
            "не найден."
        )

        return

    print()
    print("=" * 60)
    print("🔗 BUSINESS MESSAGE OWNER RESOLUTION")
    print("=" * 60)

    print(
        f"Connection ID: {connection_id}"
    )

    print(
        f"Owner ID: {owner_id}"
    )

    print(
        f"Message from: "
        f"{message.from_user.id if message.from_user else None}"
    )

    print(
        f"Chat ID: {message.chat.id}"
    )

    print("=" * 60)

    if not await user_has_access(owner_id):

        print(
            f"⛔ У пользователя "
            f"{owner_id} нет доступа."
        )

        return

    # =====================================================
    # AUTO REPLY COMMANDS (PER CHAT)
    # =====================================================

    if await handle_auto_reply_command(
        message=message,
        owner_id=owner_id,
        connection_id=connection_id,
    ):
        return

    # =====================================================
    # PANIC COMMAND
    # =====================================================

    if await handle_panic_command(
        message=message,
        owner_id=owner_id,
        connection_id=connection_id,
    ):
        return

    # =====================================================
    # CUSTOM COMMAND PRESET
    # =====================================================

    if await handle_custom_command(
        message=message,
        owner_id=owner_id,
        connection_id=connection_id,
    ):
        return

    # =====================================================
    # SPAM COMMAND
    # =====================================================

    if await handle_spam_command(
        message=message,
        owner_id=owner_id,
        connection_id=connection_id,
    ):

        return

    # =====================================================
    # PANIC COMMAND
    # =====================================================

    if await handle_panic_command(
        message=message,
        owner_id=owner_id,
        connection_id=connection_id,
    ):

        return

    # =====================================================
    # MUTE / UNMUTE
    # =====================================================

    if await handle_mute_command(
        message=message,
        owner_id=owner_id,
        connection_id=connection_id,
    ):

        return

    if await handle_unmute_command(
        message=message,
        owner_id=owner_id,
        connection_id=connection_id,
    ):

        return

    # =====================================================
    # WARN COMMAND
    # =====================================================

    if await handle_warn_command(
        message=message,
        owner_id=owner_id,
        connection_id=connection_id,
    ):

        return

    # =====================================================
    # ACTIVE MUTE
    # =====================================================

    sender_id = (
        message.from_user.id
        if message.from_user
        else None
    )

    if (
        sender_id is not None
        and sender_id != owner_id
    ):

        mute = await get_chat_mute(
            chat_id=message.chat.id,
            business_connection_id=connection_id,
        )

        if mute:

            print()
            print("=" * 60)
            print("🔇 АКТИВНЫЙ МУТ")
            print("=" * 60)

            print(
                f"Owner: {owner_id}"
            )

            print(
                f"Sender: {sender_id}"
            )

            print(
                f"Chat: {message.chat.id}"
            )

            print(
                f"Message: {message.message_id}"
            )

            print(
                f"Connection: {connection_id}"
            )

            print("=" * 60)

            data = extract_message_data(
                message
            )

            user_name = (
                message.from_user.full_name
                if message.from_user
                else None
            )

            try:

                await save_message(
                    message_id=message.message_id,
                    chat_id=message.chat.id,
                    user_id=sender_id,
                    owner_id=owner_id,
                    business_connection_id=connection_id,
                    user_name=user_name,
                    text=data["text"],
                    media_type=data["media_type"],
                    file_id=data["file_id"],
                    file_name=data["file_name"],
                    mime_type=data["mime_type"],
                    file_size=data["file_size"],
                    duration=data["duration"],
                    width=data["width"],
                    height=data["height"],
                    caption=data["caption"],
                )

            except Exception as e:

                print(
                    f"⚠️ Ошибка сохранения сообщения "
                    f"при муте: {e}"
                )

            deleted = await delete_business_message(
                message=message,
                connection_id=connection_id,
                reason="active_mute",
            )

            if deleted:

                print(
                    "🔇 Сообщение собеседника "
                    "успешно удалено."
                )

            else:

                print(
                    "❌ Сообщение собеседника "
                    "НЕ удалось удалить."
                )

            return

    # =====================================================
    # ACTIVE WARN
    # =====================================================

    if (
        sender_id is not None
        and sender_id != owner_id
    ):
        warning_processed = await process_chat_warning(
            message=message,
            owner_id=owner_id,
            connection_id=connection_id,
        )

        # Если варн был активен, текущее сообщение уже
        # учтено в его счётчике. Само сообщение собеседника
        # не удаляем. После последнего сообщения мут уже
        # установлен, а текущее сообщение остаётся сохранённым.
        if warning_processed:
            print(
                f"⚠️ Сообщение {message.message_id} "
                "учтено в системе WARN."
            )

    # =====================================================
    # ONE-TIME MEDIA VIA REPLY
    # =====================================================

    if message.reply_to_message:

        try:

            await save_media_from_reply(
                message=message,
                owner_id=owner_id,
            )

        except Exception as e:

            print(
                "❌ Ошибка обработки "
                "одноразового медиа:"
            )

            print(
                repr(e)
            )

    # =====================================================
    # SAVE NORMAL MESSAGE
    # =====================================================

    user_id = (
        message.from_user.id
        if message.from_user
        else None
    )

    user_name = (
        message.from_user.full_name
        if message.from_user
        else None
    )

    data = extract_message_data(
        message
    )

    try:

        await save_message(
            message_id=message.message_id,
            chat_id=message.chat.id,
            user_id=user_id,
            owner_id=owner_id,
            business_connection_id=connection_id,
            user_name=user_name,
            text=data["text"],
            media_type=data["media_type"],
            file_id=data["file_id"],
            file_name=data["file_name"],
            mime_type=data["mime_type"],
            file_size=data["file_size"],
            duration=data["duration"],
            width=data["width"],
            height=data["height"],
            caption=data["caption"],
        )

    except Exception as e:

        print(
            f"❌ Ошибка сохранения сообщения "
            f"{message.message_id}: {e}"
        )

    # =====================================================
    # AUTO REPLY
    # =====================================================

    await handle_auto_reply(
        message=message,
        owner_id=owner_id,
        connection_id=connection_id,
    )

    print()
    print("=" * 50)
    print("НОВОЕ BUSINESS-СООБЩЕНИЕ")
    print("=" * 50)

    print(
        f"Тип: {data['media_type']}"
    )

    print(
        f"Message ID: {message.message_id}"
    )

    print(
        f"Автор: {user_name}"
    )

    print(
        f"User ID: {user_id}"
    )

    print(
        f"Owner ID: {owner_id}"
    )

    print(
        f"Connection: {connection_id}"
    )

    print("=" * 50)


# =========================================================
# EDITED BUSINESS MESSAGE
# =========================================================

async def handle_edited_message(
    message: Message,
):

    connection_id = message.business_connection_id

    if not connection_id:
        return

    if is_duplicate_event(
        event_type="edited_business_message",
        connection_id=connection_id,
        chat_id=message.chat.id,
        message_id=message.message_id,
    ):

        return

    owner_id = await get_user_by_connection(
        business_connection_id=connection_id,
    )

    if owner_id is None:
        return

    if not await user_has_access(owner_id):
        return

    data = extract_message_data(
        message
    )

    new_text = (
        data["text"]
        or
        data["caption"]
    )

    previous_text, version = (
        await save_edited_message(
            message_id=message.message_id,
            chat_id=message.chat.id,
            business_connection_id=connection_id,
            text=new_text,
        )
    )

    client_name = (
        message.from_user.full_name
        if message.from_user
        else "Неизвестно"
    )

    client_id = (
        message.from_user.id
        if message.from_user
        else None
    )

    client_id_display = (
        client_id
        if client_id is not None
        else "Неизвестно"
    )

    notification = (
        "✏️ <b>СООБЩЕНИЕ ИЗМЕНЕНО</b>\n\n"

        f"👤 <b>Клиент:</b> "
        f"{escape(str(client_name))}\n"

        f"🆔 <b>User ID:</b> "
        f"<code>{client_id_display}</code>\n"

        f"💬 <b>Chat ID:</b> "
        f"<code>{message.chat.id}</code>\n"

        f"📝 <b>Message ID:</b> "
        f"<code>{message.message_id}</code>\n"

        f"🔢 <b>Версия:</b> "
        f"<code>{version}</code>\n\n"

        f"<b>Было:</b>\n"
        f"{escape(previous_text or '[нет текста]')}\n\n"

        f"<b>Стало:</b>\n"
        f"{escape(new_text or '[нет текста]')}"
    )

    await send_notification(
        notification=notification,
        owner_id=owner_id,
        source_user_id=client_id,
    )


# =========================================================
# DELETED BUSINESS MESSAGES
# =========================================================

async def handle_deleted_messages(
    deleted: BusinessMessagesDeleted,
):

    connection_id = deleted.business_connection_id

    if not connection_id:
        return

    owner_id = await get_user_by_connection(
        business_connection_id=connection_id
    )

    if owner_id is None:

        print(
            "⚠️ Удаление: владелец Business Connection "
            "не найден."
        )

        return

    if not await user_has_access(owner_id):
        return

    deleted_messages = deleted.message_ids

    if not deleted_messages:
        return

    print()
    print("=" * 60)
    print("🗑 BUSINESS-СООБЩЕНИЯ УДАЛЕНЫ")
    print("=" * 60)

    print(
        f"Connection: {connection_id}"
    )

    print(
        f"Owner ID: {owner_id}"
    )

    print(
        f"Message IDs: {deleted_messages}"
    )

    print("=" * 60)

    for message_id in deleted_messages:

        # =================================================
        # GET MESSAGE INFO
        # =================================================

        message_info = None

        try:

            message_info = await get_message_info(
                message_id=message_id,
                business_connection_id=connection_id,
            )

        except Exception as e:

            print(
                f"⚠️ Не удалось получить информацию "
                f"о сообщении {message_id}: {e}"
            )

        # =================================================
        # DEBUG MESSAGE INFO
        # =================================================

        print()
        print("=" * 60)
        print("🔎 ДАННЫЕ УДАЛЁННОГО СООБЩЕНИЯ")
        print("=" * 60)

        print(
            f"Message ID: {message_id}"
        )

        print(
            f"message_info type: "
            f"{type(message_info).__name__}"
        )

        print(
            f"message_info: "
            f"{message_info!r}"
        )

        print("=" * 60)

        # =================================================
        # CHAT ID
        # =================================================

        chat_id = None

        if isinstance(message_info, dict):

            chat_id = message_info.get(
                "chat_id"
            )

        elif isinstance(message_info, (tuple, list)):

            if len(message_info) > 1:
                chat_id = message_info[1]

        # =================================================
        # SERVICE DELETE
        # =================================================

        service_reason = None

        try:

            service_reason = (
                await consume_service_deleted_message(
                    business_connection_id=connection_id,
                    chat_id=chat_id,
                    message_id=message_id,
                )
            )

        except Exception as e:

            print(
                f"⚠️ Ошибка проверки служебного удаления "
                f"{message_id}: {e}"
            )

        if service_reason:

            print()
            print("🛡 СЛУЖЕБНОЕ УДАЛЕНИЕ")
            print(
                f"Message ID: {message_id}"
            )
            print(
                f"Chat ID: {chat_id}"
            )
            print(
                f"Причина: {service_reason}"
            )
            print(
                "🚫 Уведомление OWNER НЕ отправляется."
            )

            continue

        # =================================================
        # NORMAL DELETE
        # =================================================

        try:

            await mark_message_deleted(
                message_id=message_id,
                business_connection_id=connection_id,
            )

        except Exception as e:

            print(
                f"⚠️ Не удалось отметить сообщение "
                f"{message_id} как удалённое: {e}"
            )

        # =================================================
        # AUTHOR + TEXT
        # =================================================

        client_name = "Неизвестно"
        client_id = None

        deleted_text = None
        deleted_caption = None

        if isinstance(message_info, dict):

            client_name = (
                message_info.get("user_name")
                or "Неизвестно"
            )

            client_id = (
                message_info.get("user_id")
            )

            if chat_id is None:

                chat_id = message_info.get(
                    "chat_id"
                )

            deleted_text = (
                message_info.get("text")
                or message_info.get("message_text")
                or message_info.get("content")
            )

            deleted_caption = (
                message_info.get("caption")
                or message_info.get("message_caption")
            )

        elif isinstance(message_info, (tuple, list)):

            if len(message_info) > 2:
                client_id = message_info[2]

            if len(message_info) > 3:
                client_name = (
                    message_info[3]
                    or "Неизвестно"
                )

            string_values = []

            for value in message_info:

                if not isinstance(value, str):
                    continue

                value = value.strip()

                if not value:
                    continue

                if value == str(client_name).strip():
                    continue

                string_values.append(value)

            if string_values:

                for value in string_values:

                    if value in {
                        "active",
                        "deleted",
                        "text",
                        "photo",
                        "video",
                        "voice",
                        "document",
                        "audio",
                        "animation",
                        "video_note",
                        "sticker",
                    }:
                        continue

                    deleted_text = value
                    break

        # =================================================
        # DISPLAY IDS
        # =================================================

        chat_id_display = (
            chat_id
            if chat_id is not None
            else "Неизвестно"
        )

        client_id_display = (
            client_id
            if client_id is not None
            else "Неизвестно"
        )

        # =================================================
        # MESSAGE CONTENT
        # =================================================

        message_content = (
            deleted_text
            or deleted_caption
            or "[текст отсутствует]"
        )

        message_content = escape(
            str(message_content)
        )

        if len(message_content) > 3000:

            message_content = (
                message_content[:3000]
                + "\n\n<i>…текст обрезан.</i>"
            )

        # =================================================
        # NORMAL DELETE NOTIFICATION
        # =================================================

        notification = (
            "🗑 <b>СООБЩЕНИЕ УДАЛЕНО</b>\n\n"

            f"👤 <b>Клиент:</b> "
            f"{escape(str(client_name))}\n"

            f"🆔 <b>User ID:</b> "
            f"<code>{client_id_display}</code>\n"

            f"💬 <b>Chat ID:</b> "
            f"<code>{chat_id_display}</code>\n"

            f"📝 <b>Message ID:</b> "
            f"<code>{message_id}</code>\n\n"

            f"📄 <b>Текст:</b>\n"
            f"{message_content}"
        )

        await send_notification(
            notification=notification,
            owner_id=owner_id,
            source_user_id=client_id,
        )

        print(
            f"🔔 Уведомление об обычном удалении "
            f"обработано: {message_id}"
        )

        print(
            f"📄 Содержимое: "
            f"{deleted_text or deleted_caption or '[нет текста]'}"
        )

    print(
        "✅ Обработка удаления завершена."
    )


# =========================================================
# MAIN
# =========================================================

async def main():

    global bot

    await init_db()

    session = AiohttpSession(
        proxy=PROXY
    )

    bot = Bot(
        token=BOT_TOKEN,
        session=session,
    )

    dp = Dispatcher()

    dp.message.middleware(
        ForceSubscriptionMiddleware()
    )

    dp.callback_query.middleware(
        ForceSubscriptionMiddleware()
    )

    mute_worker = asyncio.create_task(
        mute_expiration_worker()
    )

    # =====================================================
    # COMMANDS
    # =====================================================

    dp.message.register(
        start_command,
        lambda message:
            message.text == "/start",
    )

    dp.message.register(
        root_command,
        lambda message:
            message.text == "/root",
    )

    # =====================================================
    # FSM
    # =====================================================

    dp.message.register(
        admin_grant_user_id,
        AdminGrantStates.waiting_user_id,
    )

    dp.message.register(
        custom_trigger_message,
        CustomCommandStates.waiting_trigger,
    )

    dp.message.register(
        custom_action_message,
        CustomCommandStates.waiting_action,
    )

    # aiogram handlers must be async callables. Do not use ordinary
    # lambdas here: a lambda returning a coroutine is not awaited by
    # Dispatcher and causes "coroutine was never awaited".
    async def auto_reply_day_text_handler(message: Message, state: FSMContext):
        await _save_auto_reply_text_message(message, state, "day_text")

    async def auto_reply_night_text_handler(message: Message, state: FSMContext):
        await _save_auto_reply_text_message(message, state, "night_text")

    async def auto_reply_interval_handler(message: Message, state: FSMContext):
        await _save_auto_reply_number_message(
            message, state, "interval_minutes"
        )

    async def auto_reply_day_start_handler(message: Message, state: FSMContext):
        await _save_auto_reply_number_message(
            message, state, "day_start"
        )

    async def auto_reply_day_end_handler(message: Message, state: FSMContext):
        await _save_auto_reply_number_message(
            message, state, "day_end"
        )

    async def auto_reply_night_start_handler(message: Message, state: FSMContext):
        await _save_auto_reply_number_message(
            message, state, "night_start"
        )

    async def auto_reply_night_end_handler(message: Message, state: FSMContext):
        await _save_auto_reply_number_message(
            message, state, "night_end"
        )

    dp.message.register(
        auto_reply_day_text_handler,
        AutoReplyStates.waiting_day_text,
    )

    dp.message.register(
        auto_reply_night_text_handler,
        AutoReplyStates.waiting_night_text,
    )

    dp.message.register(
        auto_reply_interval_handler,
        AutoReplyStates.waiting_interval,
    )

    dp.message.register(
        auto_reply_day_start_handler,
        AutoReplyStates.waiting_day_start,
    )

    dp.message.register(
        auto_reply_day_end_handler,
        AutoReplyStates.waiting_day_end,
    )

    dp.message.register(
        auto_reply_night_start_handler,
        AutoReplyStates.waiting_night_start,
    )

    dp.message.register(
        auto_reply_night_end_handler,
        AutoReplyStates.waiting_night_end,
    )

    # =====================================================
    # PAYMENTS
    # =====================================================

    dp.pre_checkout_query.register(
        pre_checkout_handler
    )

    dp.message.register(
        successful_payment_handler,
        lambda message: message.successful_payment is not None,
    )

    # =====================================================
    # BUSINESS CONNECTION
    # =====================================================

    dp.business_connection.register(
        handle_business_connection
    )

    # =====================================================
    # MAIN CALLBACKS
    # =====================================================

    dp.callback_query.register(
        check_channel_subscription_callback,
        lambda callback:
            callback.data == "check_channel_subscription",
    )

    dp.callback_query.register(
        bot_features_callback,
        lambda callback:
            callback.data == "bot_features",
    )
    dp.callback_query.register(
        connection_tutorial_callback,
        lambda callback:
            callback.data == "connection_tutorial",
    )

    dp.callback_query.register(
        show_auto_replies,
        lambda callback:
            callback.data == "auto_replies",
    )

    dp.callback_query.register(
        auto_reply_select_callback,
        lambda callback:
            bool(callback.data)
            and callback.data.startswith("auto_reply_select:"),
    )

    dp.callback_query.register(
        auto_reply_toggle_callback,
        lambda callback:
            bool(callback.data) and callback.data.startswith("auto_reply_toggle:"),
    )

    dp.callback_query.register(
        auto_reply_day_text_callback,
        lambda callback:
            bool(callback.data) and callback.data.startswith("auto_reply_day_text:"),
    )

    dp.callback_query.register(
        auto_reply_night_text_callback,
        lambda callback:
            bool(callback.data) and callback.data.startswith("auto_reply_night_text:"),
    )

    dp.callback_query.register(
        auto_reply_interval_callback,
        lambda callback:
            bool(callback.data) and callback.data.startswith("auto_reply_interval:"),
    )

    dp.callback_query.register(
        auto_reply_custom_interval_callback,
        lambda callback:
            bool(callback.data) and callback.data.startswith("auto_reply_custom_interval:"),
    )

    dp.callback_query.register(
        auto_reply_set_interval_callback,
        lambda callback:
            bool(callback.data)
            and callback.data.startswith("auto_reply_set_interval:"),
    )

    dp.callback_query.register(
        lambda callback, state: auto_reply_time_callback(
            callback, state, "day_start"
        ),
        lambda callback:
            bool(callback.data) and callback.data.startswith("auto_reply_day_start:") ,
    )

    dp.callback_query.register(
        lambda callback, state: auto_reply_time_callback(
            callback, state, "day_end"
        ),
        lambda callback:
            bool(callback.data) and callback.data.startswith("auto_reply_day_end:") ,
    )

    dp.callback_query.register(
        lambda callback, state: auto_reply_time_callback(
            callback, state, "night_start"
        ),
        lambda callback:
            bool(callback.data) and callback.data.startswith("auto_reply_night_start:") ,
    )

    dp.callback_query.register(
        lambda callback, state: auto_reply_time_callback(
            callback, state, "night_end"
        ),
        lambda callback:
            bool(callback.data) and callback.data.startswith("auto_reply_night_end:") ,
    )

    dp.callback_query.register(
        features_passive_callback,
        lambda callback:
            callback.data == "features_passive",
    )

    dp.callback_query.register(
        features_commands_callback,
        lambda callback:
            callback.data == "features_commands",
    )

    dp.callback_query.register(
        feature_detail_callback,
        lambda callback:
            bool(callback.data)
            and callback.data.startswith("feature_"),
    )

    dp.callback_query.register(
        custom_commands_callback,
        lambda callback:
            callback.data == "custom_commands",
    )

    dp.callback_query.register(
        custom_create_callback,
        lambda callback:
            callback.data == "custom_create",
    )

    dp.callback_query.register(
        custom_save_callback,
        lambda callback:
            callback.data == "custom_save",
    )

    dp.callback_query.register(
        custom_cancel_callback,
        lambda callback:
            callback.data == "custom_cancel",
    )

    dp.callback_query.register(
        custom_delete_callback,
        lambda callback:
            bool(callback.data)
            and callback.data.startswith("custom_delete:"),
    )

    dp.callback_query.register(
        subscription_callback,
        lambda callback:
            callback.data == "subscription",
    )

    dp.callback_query.register(
        create_stars_invoice_callback,
        lambda callback: bool(callback.data) and callback.data.startswith("pay_stars:"),
    )

    dp.callback_query.register(
        back_to_main_callback,
        lambda callback:
            callback.data == "back_to_main",
    )

    # =====================================================
    # ADMIN CALLBACKS
    # =====================================================

    dp.callback_query.register(
        admin_users_callback,
        lambda callback:
            callback.data == "admin_users",
    )

    dp.callback_query.register(
        admin_grant_callback,
        lambda callback:
            callback.data == "admin_grant",
    )

    dp.callback_query.register(
        admin_stats_callback,
        lambda callback:
            callback.data == "admin_stats",
    )

    dp.callback_query.register(
        admin_cancel_grant_callback,
        lambda callback:
            callback.data == "admin_cancel_grant",
    )

    dp.callback_query.register(
        admin_toggle_notifications_callback,
        lambda callback:
            callback.data == "admin_toggle_notifications",
    )

    dp.callback_query.register(
        admin_toggle_one_time_callback,
        lambda callback:
            callback.data == "admin_toggle_one_time",
    )

    # =====================================================
    # GRANT
    # =====================================================

    dp.callback_query.register(
        grant_subscription_callback,
        lambda callback:
            callback.data in {
                "grant_7",
                "grant_30",
                "grant_90",
                "grant_365",
            },
    )

    # =====================================================
    # MUTE BUTTON
    # =====================================================

    dp.callback_query.register(
        remove_mute_callback,
        lambda callback:
            bool(callback.data)
            and callback.data.startswith(
                "remove_mute:"
            ),
    )

    # =====================================================
    # BUSINESS EVENTS
    # =====================================================

    dp.business_message.register(
        handle_message
    )

    dp.edited_business_message.register(
        handle_edited_message
    )

    dp.deleted_business_messages.register(
        handle_deleted_messages
    )

    # =====================================================
    # START
    # =====================================================

    print()
    print("=" * 60)
    print("🚀 REEFER CHAT MANAGER")
    print("=" * 60)

    print(
        f"👑 OWNER_ID: {OWNER_ID}"
    )

    print(
        f"🌐 PROXY: {PROXY}"
    )

    print(
        f"📢 FORCE SUBSCRIPTION: {FORCE_SUB_CHANNEL}"
    )

    print("🗄 SQLite: ON")
    print("📩 Business messages: ON")
    print("✏️ Edited messages: ON")
    print("🗑 Deleted messages: ON")
    print("🛡 Anti-duplicate events: ON")
    print("🛡 Anti-duplicate one-time replies: ON")
    print("📦 One-time media via Reply: ON")
    print("🔐 Проверка protected_content: ON")
    print("💾 One-time media saved to disk: OFF")

    print(
        "📦 One-time media OWNER notifications: "
        + (
            "ON"
            if one_time_media_notifications_enabled
            else "OFF"
        )
    )

    print("📝 Custom command presets: ON")
    print(f"🔢 Custom presets maximum: {CUSTOM_COMMAND_MAX} per user")
    print(f"🔢 Custom actions maximum: {CUSTOM_ACTIONS_MAX} per preset")
    print("🧹 .panic command: ON")
    print("📨 .spam command: ON")
    print("🔢 .spam maximum: 3 messages per command")
    print("🔇 Mute system: ON")
    print("🔊 Unmute system: ON")
    print("🔘 Mute control button: ON")
    print("👤 Per-Business owner mute buttons: ON")
    print("⚠️ Warn system: ON")
    print("🔢 Warn counter: ON")
    print("🔇 Warn -> 30 minute mute: ON")

    print("📸 Фото: ON")
    print("🎤 Голосовые: ON")
    print("📎 Файлы: ON")
    print("🔵 Кружки: ON")
    print("🎥 Видео: ON")
    print("🎵 Аудио: ON")
    print("🎞 GIF: ON")
    print("🏷 Стикеры: ON")

    print("📝 Текст удалённых сообщений: ON")
    print("📝 Caption удалённых медиа: ON")

    print("=" * 60)

    try:

        await dp.start_polling(
            bot
        )

    finally:

        mute_worker.cancel()

        try:

            await mute_worker

        except asyncio.CancelledError:
            pass

        await bot.session.close()


# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        print(
            "🛑 Бот остановлен."
        )