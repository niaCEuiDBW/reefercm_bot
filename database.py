import aiosqlite

from datetime import datetime, timedelta, timezone


# =========================================================
# CONFIG
# =========================================================

DB_PATH = "business_bot.db"


# =========================================================
# TIME
# =========================================================

def utc_now():
    return datetime.now(timezone.utc)


def utc_now_str():
    return utc_now().isoformat()


def parse_datetime(value):
    if not value:
        return None

    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None


# =========================================================
# DATABASE HELPERS
# =========================================================

async def column_exists(
    db,
    table_name,
    column_name,
):
    cursor = await db.execute(
        f"PRAGMA table_info({table_name})"
    )

    columns = await cursor.fetchall()

    return any(
        column[1] == column_name
        for column in columns
    )


async def table_exists(
    db,
    table_name,
):
    cursor = await db.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name = ?
        LIMIT 1
        """,
        (table_name,),
    )

    row = await cursor.fetchone()

    return row is not None


async def add_column_if_missing(
    db,
    table_name,
    column_name,
    column_definition,
):
    if not await table_exists(
        db,
        table_name,
    ):
        return

    exists = await column_exists(
        db,
        table_name,
        column_name,
    )

    if not exists:

        print(
            f"🛠 MIGRATION: добавляю "
            f"{table_name}.{column_name}"
        )

        await db.execute(
            f"""
            ALTER TABLE {table_name}
            ADD COLUMN {column_name}
            {column_definition}
            """
        )


# =========================================================
# DATABASE INIT
# =========================================================

async def init_db():

    async with aiosqlite.connect(DB_PATH) as db:

        # =================================================
        # USERS
        # =================================================

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                role TEXT NOT NULL DEFAULT 'user',
                created_at TEXT NOT NULL,
                trial_started_at TEXT,
                trial_expires_at TEXT,
                subscription_expires_at TEXT,
                subscription_forever INTEGER NOT NULL DEFAULT 0
            )
            """
        )

        await add_column_if_missing(
            db,
            "users",
            "subscription_forever",
            "INTEGER NOT NULL DEFAULT 0",
        )

        # =================================================
        # PAYMENTS
        # =================================================

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                plan_id TEXT NOT NULL,
                plan_name TEXT NOT NULL,
                payment_method TEXT NOT NULL,
                amount INTEGER NOT NULL,
                currency TEXT NOT NULL,
                provider_payment_id TEXT,
                telegram_payment_charge_id TEXT,
                invoice_payload TEXT UNIQUE,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT,
                FOREIGN KEY (telegram_id)
                    REFERENCES users(telegram_id)
                    ON DELETE CASCADE
            )
            """
        )

        await db.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_provider_id
            ON payments(provider_payment_id)
            WHERE provider_payment_id IS NOT NULL
            """
        )

        await db.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_telegram_charge
            ON payments(telegram_payment_charge_id)
            WHERE telegram_payment_charge_id IS NOT NULL
            """
        )

        await db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_payments_user
            ON payments(telegram_id, created_at)
            """
        )

        # =================================================
        # BUSINESS CONNECTIONS
        # =================================================

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS business_connections (
                business_connection_id TEXT PRIMARY KEY,
                telegram_id INTEGER NOT NULL,
                is_enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,

                FOREIGN KEY (telegram_id)
                REFERENCES users(telegram_id)
                ON DELETE CASCADE
            )
            """
        )

        # =================================================
        # MESSAGES
        # =================================================

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                message_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                user_id INTEGER,

                owner_id INTEGER NOT NULL,

                business_connection_id TEXT NOT NULL,

                user_name TEXT,

                text TEXT,

                media_type TEXT,
                file_id TEXT,
                file_name TEXT,
                mime_type TEXT,
                file_size INTEGER,

                duration INTEGER,
                width INTEGER,
                height INTEGER,

                caption TEXT,

                status TEXT NOT NULL DEFAULT 'active',

                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,

                UNIQUE(
                    message_id,
                    business_connection_id
                )
            )
            """
        )

        # =================================================
        # MESSAGES MIGRATION
        #
        # Сначала добавляем ВСЕ потенциально отсутствующие
        # колонки.
        # =================================================

        await add_column_if_missing(
            db,
            "messages",
            "message_id",
            "INTEGER",
        )

        await add_column_if_missing(
            db,
            "messages",
            "chat_id",
            "INTEGER",
        )

        await add_column_if_missing(
            db,
            "messages",
            "user_id",
            "INTEGER",
        )

        await add_column_if_missing(
            db,
            "messages",
            "owner_id",
            "INTEGER",
        )

        await add_column_if_missing(
            db,
            "messages",
            "business_connection_id",
            "TEXT",
        )

        await add_column_if_missing(
            db,
            "messages",
            "user_name",
            "TEXT",
        )

        await add_column_if_missing(
            db,
            "messages",
            "text",
            "TEXT",
        )

        await add_column_if_missing(
            db,
            "messages",
            "media_type",
            "TEXT",
        )

        await add_column_if_missing(
            db,
            "messages",
            "file_id",
            "TEXT",
        )

        await add_column_if_missing(
            db,
            "messages",
            "file_name",
            "TEXT",
        )

        await add_column_if_missing(
            db,
            "messages",
            "mime_type",
            "TEXT",
        )

        await add_column_if_missing(
            db,
            "messages",
            "file_size",
            "INTEGER",
        )

        await add_column_if_missing(
            db,
            "messages",
            "duration",
            "INTEGER",
        )

        await add_column_if_missing(
            db,
            "messages",
            "width",
            "INTEGER",
        )

        await add_column_if_missing(
            db,
            "messages",
            "height",
            "INTEGER",
        )

        await add_column_if_missing(
            db,
            "messages",
            "caption",
            "TEXT",
        )

        await add_column_if_missing(
            db,
            "messages",
            "status",
            "TEXT",
        )

        await add_column_if_missing(
            db,
            "messages",
            "created_at",
            "TEXT",
        )

        await add_column_if_missing(
            db,
            "messages",
            "updated_at",
            "TEXT",
        )

        # =================================================
        # FIX OLD MESSAGE ROWS
        # =================================================

        now = utc_now_str()

        await db.execute(
            """
            UPDATE messages
            SET created_at = ?
            WHERE created_at IS NULL
               OR created_at = ''
            """,
            (now,),
        )

        await db.execute(
            """
            UPDATE messages
            SET updated_at = created_at
            WHERE updated_at IS NULL
               OR updated_at = ''
            """
        )

        await db.execute(
            """
            UPDATE messages
            SET status = 'active'
            WHERE status IS NULL
               OR status = ''
            """
        )

        # =================================================
        # MESSAGE VERSIONS
        # =================================================

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS message_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                message_id INTEGER NOT NULL,

                business_connection_id TEXT NOT NULL,

                version INTEGER NOT NULL,

                text TEXT,

                created_at TEXT NOT NULL,

                UNIQUE(
                    message_id,
                    business_connection_id,
                    version
                )
            )
            """
        )

        await add_column_if_missing(
            db,
            "message_versions",
            "message_id",
            "INTEGER",
        )

        await add_column_if_missing(
            db,
            "message_versions",
            "business_connection_id",
            "TEXT",
        )

        await add_column_if_missing(
            db,
            "message_versions",
            "version",
            "INTEGER",
        )

        await add_column_if_missing(
            db,
            "message_versions",
            "text",
            "TEXT",
        )

        await add_column_if_missing(
            db,
            "message_versions",
            "created_at",
            "TEXT",
        )

        # =================================================
        # OWNER SETTINGS
        # =================================================

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS owner_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),

                owner_notifications_enabled
                INTEGER NOT NULL DEFAULT 1,

                one_time_media_notifications_enabled
                INTEGER NOT NULL DEFAULT 1,

                updated_at TEXT
            )
            """
        )

        await add_column_if_missing(
            db,
            "owner_settings",
            "owner_notifications_enabled",
            "INTEGER NOT NULL DEFAULT 1",
        )

        await add_column_if_missing(
            db,
            "owner_settings",
            "one_time_media_notifications_enabled",
            "INTEGER NOT NULL DEFAULT 1",
        )

        await add_column_if_missing(
            db,
            "owner_settings",
            "updated_at",
            "TEXT",
        )

        await db.execute(
            """
            INSERT OR IGNORE INTO owner_settings (
                id,
                owner_notifications_enabled,
                one_time_media_notifications_enabled,
                updated_at
            )
            VALUES (1, 1, 1, ?)
            """,
            (now,),
        )

        await db.execute(
            """
            UPDATE owner_settings
            SET updated_at = ?
            WHERE id = 1
              AND (
                  updated_at IS NULL
                  OR updated_at = ''
              )
            """,
            (now,),
        )

        # =================================================
        # CHAT MUTES
        # =================================================

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_mutes (
                owner_id INTEGER NOT NULL,

                chat_id INTEGER NOT NULL,

                business_connection_id TEXT NOT NULL,

                muted_until TEXT NOT NULL,

                created_at TEXT NOT NULL,

                updated_at TEXT NOT NULL,

                PRIMARY KEY (
                    business_connection_id,
                    chat_id
                )
            )
            """
        )

        # =================================================
        # WARNINGS
        # =================================================

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_warnings (
                owner_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                business_connection_id TEXT NOT NULL,
                remaining INTEGER NOT NULL,
                warning_message_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,

                PRIMARY KEY (
                    business_connection_id,
                    chat_id
                )
            )
            """
        )

        # =================================================
        # SERVICE DELETED
        # =================================================

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS service_deleted_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                business_connection_id TEXT NOT NULL,

                chat_id INTEGER,

                message_id INTEGER NOT NULL,

                reason TEXT,

                created_at TEXT NOT NULL,

                UNIQUE(
                    business_connection_id,
                    chat_id,
                    message_id
                )
            )
            """
        )


        # =================================================
        # AUTO REPLIES
        # =================================================

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS auto_reply_settings (
                business_connection_id TEXT PRIMARY KEY,
                owner_id INTEGER NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 0,
                day_text TEXT,
                night_text TEXT,
                interval_minutes INTEGER NOT NULL DEFAULT 30,
                day_start TEXT NOT NULL DEFAULT '08:00',
                day_end TEXT NOT NULL DEFAULT '23:00',
                night_start TEXT NOT NULL DEFAULT '23:00',
                night_end TEXT NOT NULL DEFAULT '08:00',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,

                FOREIGN KEY (business_connection_id)
                REFERENCES business_connections(business_connection_id)
                ON DELETE CASCADE
            )
            """
        )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS auto_reply_cooldowns (
                business_connection_id TEXT NOT NULL,
                chat_id INTEGER NOT NULL,
                last_sent_at TEXT NOT NULL,

                PRIMARY KEY (
                    business_connection_id,
                    chat_id
                ),

                FOREIGN KEY (business_connection_id)
                REFERENCES business_connections(business_connection_id)
                ON DELETE CASCADE
            )
            """
        )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS auto_reply_chat_exclusions (
                business_connection_id TEXT NOT NULL,
                chat_id INTEGER NOT NULL,
                disabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (business_connection_id, chat_id),
                FOREIGN KEY (business_connection_id)
                    REFERENCES business_connections(business_connection_id)
                    ON DELETE CASCADE
            )
            """
        )

        await db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_auto_reply_cooldowns_time
            ON auto_reply_cooldowns(last_sent_at)
            """
        )

        # =================================================
        # CUSTOM TEMPLATES
        # =================================================

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS custom_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                trigger TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(owner_id, trigger)
            )
            """
        )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS custom_template_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                template_id INTEGER NOT NULL,
                position INTEGER NOT NULL,
                action_type TEXT NOT NULL DEFAULT 'text',
                content TEXT NOT NULL,
                UNIQUE(template_id, position),
                FOREIGN KEY(template_id) REFERENCES custom_templates(id)
                    ON DELETE CASCADE
            )
            """
        )

        await db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_custom_templates_owner
            ON custom_templates(owner_id)
            """
        )

        await db.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_custom_templates_owner_trigger_nocase
            ON custom_templates(owner_id, trigger COLLATE NOCASE)
            """
        )

        await db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_custom_template_actions_template
            ON custom_template_actions(template_id)
            """
        )

        # =================================================
        # INDEXES
        # =================================================

        await db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_messages_connection
            ON messages(business_connection_id)
            """
        )

        await db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_messages_chat
            ON messages(chat_id)
            """
        )

        await db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_messages_message
            ON messages(message_id)
            """
        )

        await db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_mutes_until
            ON chat_mutes(muted_until)
            """
        )

        await db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_service_deleted
            ON service_deleted_messages(
                business_connection_id,
                chat_id,
                message_id
            )
            """
        )

        await db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_warnings_chat
            ON chat_warnings(
                business_connection_id,
                chat_id
            )
            """
        )

        await db.commit()

    print("🗄 DATABASE: OK")
    print("🛠 DATABASE MIGRATION: OK")
    print("✏️ MESSAGE VERSIONS: OK")
    print("🗑 MESSAGE DELETE HISTORY: OK")


# =========================================================
# USERS
# =========================================================

async def create_or_update_user(
    telegram_id: int,
    username: str | None = None,
    full_name: str | None = None,
    is_owner: bool = False,
):

    now = utc_now_str()

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            SELECT
                telegram_id,
                trial_started_at,
                trial_expires_at,
                subscription_expires_at,
                role
            FROM users
            WHERE telegram_id = ?
            """,
            (telegram_id,),
        )

        existing = await cursor.fetchone()

        if existing is None:

            trial_started = utc_now()

            trial_expires = (
                trial_started
                + timedelta(days=7)
            )

            role = (
                "owner"
                if is_owner
                else "user"
            )

            await db.execute(
                """
                INSERT INTO users (
                    telegram_id,
                    username,
                    full_name,
                    role,
                    created_at,
                    trial_started_at,
                    trial_expires_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    telegram_id,
                    username,
                    full_name,
                    role,
                    now,
                    trial_started.isoformat(),
                    trial_expires.isoformat(),
                ),
            )

        else:

            role = (
                "owner"
                if is_owner
                else existing[4]
            )

            await db.execute(
                """
                UPDATE users
                SET
                    username = ?,
                    full_name = ?,
                    role = ?
                WHERE telegram_id = ?
                """,
                (
                    username,
                    full_name,
                    role,
                    telegram_id,
                ),
            )

        await db.commit()


async def get_user(
    telegram_id: int,
):

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            SELECT
                telegram_id,
                username,
                full_name,
                role,
                created_at,
                trial_started_at,
                trial_expires_at,
                subscription_expires_at,
                subscription_forever
            FROM users
            WHERE telegram_id = ?
            """,
            (telegram_id,),
        )

        row = await cursor.fetchone()

        if row is None:
            return None

        return row


async def get_all_users():

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            SELECT
                telegram_id,
                username,
                full_name,
                role,
                created_at,
                trial_started_at,
                trial_expires_at,
                subscription_expires_at,
                subscription_forever
            FROM users
            ORDER BY created_at DESC
            """
        )

        return await cursor.fetchall()


# =========================================================
# DELETE USER
# =========================================================

async def delete_user(
    telegram_id: int,
):
    """
    Полностью удаляет пользователя из базы вместе
    с его Business Connections и связанными данными.

    Возвращает True, если пользователь существовал
    и был удалён, иначе False.
    """

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute("PRAGMA foreign_keys = ON")

        cursor = await db.execute(
            "SELECT role FROM users WHERE telegram_id = ?",
            (telegram_id,),
        )

        user = await cursor.fetchone()

        if user is None:
            return False

        cursor = await db.execute(
            """
            SELECT business_connection_id
            FROM business_connections
            WHERE telegram_id = ?
            """,
            (telegram_id,),
        )

        connection_rows = await cursor.fetchall()
        connection_ids = [row[0] for row in connection_rows]

        # Удаляем данные, связанные с Business Connections.
        if connection_ids:
            placeholders = ",".join("?" for _ in connection_ids)

            await db.execute(
                f"DELETE FROM message_versions WHERE business_connection_id IN ({placeholders})",
                connection_ids,
            )

            await db.execute(
                f"DELETE FROM messages WHERE business_connection_id IN ({placeholders})",
                connection_ids,
            )

            await db.execute(
                f"DELETE FROM service_deleted_messages WHERE business_connection_id IN ({placeholders})",
                connection_ids,
            )

            await db.execute(
                f"DELETE FROM chat_mutes WHERE business_connection_id IN ({placeholders})",
                connection_ids,
            )

            await db.execute(
                f"DELETE FROM chat_warnings WHERE business_connection_id IN ({placeholders})",
                connection_ids,
            )

        # На случай старых/несогласованных записей.
        await db.execute(
            "DELETE FROM messages WHERE owner_id = ?",
            (telegram_id,),
        )

        await db.execute(
            "DELETE FROM chat_mutes WHERE owner_id = ?",
            (telegram_id,),
        )

        await db.execute(
            "DELETE FROM chat_warnings WHERE owner_id = ?",
            (telegram_id,),
        )

        await db.execute(
            "DELETE FROM business_connections WHERE telegram_id = ?",
            (telegram_id,),
        )

        await db.execute(
            "DELETE FROM users WHERE telegram_id = ?",
            (telegram_id,),
        )

        await db.commit()

        return True


async def get_user_by_connection(
    business_connection_id: str,
):

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            SELECT telegram_id
            FROM business_connections
            WHERE business_connection_id = ?
              AND is_enabled = 1
            LIMIT 1
            """,
            (business_connection_id,),
        )

        row = await cursor.fetchone()

        if row is None:
            return None

        return row[0]


# =========================================================
# ACCESS / SUBSCRIPTION
# =========================================================

async def has_access(
    telegram_id: int,
):

    user = await get_user(
        telegram_id
    )

    if user is None:
        return False

    role = user[3]

    if role == "owner":
        return True

    now = utc_now()

    trial_expires_at = parse_datetime(
        user[6]
    )

    if bool(user[8]):
        return True

    subscription_expires_at = parse_datetime(
        user[7]
    )

    if (
        subscription_expires_at
        and
        subscription_expires_at > now
    ):
        return True

    if (
        trial_expires_at
        and
        trial_expires_at > now
    ):
        return True

    return False


async def get_subscription_status(
    telegram_id: int,
):

    user = await get_user(
        telegram_id
    )

    if user is None:

        return {
            "status": "expired",
            "expires_at": None,
        }

    role = user[3]

    if role == "owner":

        return {
            "status": "owner",
            "expires_at": None,
        }

    now = utc_now()

    if bool(user[8]):
        return {
            "status": "lifetime",
            "expires_at": None,
        }

    subscription_expires_at = parse_datetime(
        user[7]
    )

    if (
        subscription_expires_at
        and
        subscription_expires_at > now
    ):

        return {
            "status": "active",
            "expires_at": subscription_expires_at.isoformat(),
        }

    trial_expires_at = parse_datetime(
        user[6]
    )

    if (
        trial_expires_at
        and
        trial_expires_at > now
    ):

        return {
            "status": "trial",
            "expires_at": trial_expires_at.isoformat(),
        }

    return {
        "status": "expired",
        "expires_at": None,
    }


async def grant_subscription(
    telegram_id: int,
    days: int,
):

    if days <= 0:
        raise ValueError(
            "days должен быть больше 0"
        )

    user = await get_user(
        telegram_id
    )

    if user is None:
        raise ValueError(
            "Пользователь не найден"
        )

    now = utc_now()

    current_expiration = parse_datetime(
        user[7]
    )

    if (
        current_expiration
        and
        current_expiration > now
    ):
        base_date = current_expiration
    else:
        base_date = now

    expiration = (
        base_date
        + timedelta(days=days)
    )

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            """
            UPDATE users
            SET subscription_expires_at = ?
            WHERE telegram_id = ?
            """,
            (
                expiration.isoformat(),
                telegram_id,
            ),
        )

        await db.commit()

    return expiration.isoformat()


# =========================================================
# PAYMENTS / SUBSCRIPTIONS
# =========================================================

async def create_payment(
    telegram_id: int,
    plan_id: str,
    plan_name: str,
    payment_method: str,
    amount: int,
    currency: str,
    invoice_payload: str | None = None,
):
    now = utc_now_str()

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            INSERT INTO payments (
                telegram_id,
                plan_id,
                plan_name,
                payment_method,
                amount,
                currency,
                invoice_payload,
                status,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
            """,
            (
                telegram_id,
                plan_id,
                plan_name,
                payment_method,
                amount,
                currency,
                invoice_payload,
                now,
                now,
            ),
        )
        await db.commit()
        return cursor.lastrowid


async def get_payment_by_payload(payload: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT
                id, telegram_id, plan_id, plan_name,
                payment_method, amount, currency,
                provider_payment_id,
                telegram_payment_charge_id,
                invoice_payload, status,
                created_at, updated_at, completed_at
            FROM payments
            WHERE invoice_payload = ?
            LIMIT 1
            """,
            (payload,),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return {
            "id": row[0],
            "telegram_id": row[1],
            "plan_id": row[2],
            "plan_name": row[3],
            "payment_method": row[4],
            "amount": row[5],
            "currency": row[6],
            "provider_payment_id": row[7],
            "telegram_payment_charge_id": row[8],
            "invoice_payload": row[9],
            "status": row[10],
            "created_at": row[11],
            "updated_at": row[12],
            "completed_at": row[13],
        }


async def set_payment_provider_id(
    payment_id: int,
    provider_payment_id: str,
):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE payments
            SET provider_payment_id = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (provider_payment_id, utc_now_str(), payment_id),
        )
        await db.commit()



async def get_payment_by_provider_id(provider_payment_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT
                id, telegram_id, plan_id, plan_name,
                payment_method, amount, currency,
                provider_payment_id,
                telegram_payment_charge_id,
                invoice_payload, status,
                created_at, updated_at, completed_at
            FROM payments
            WHERE provider_payment_id = ?
            LIMIT 1
            """,
            (provider_payment_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return {
            "id": row[0],
            "telegram_id": row[1],
            "plan_id": row[2],
            "plan_name": row[3],
            "payment_method": row[4],
            "amount": row[5],
            "currency": row[6],
            "provider_payment_id": row[7],
            "telegram_payment_charge_id": row[8],
            "invoice_payload": row[9],
            "status": row[10],
            "created_at": row[11],
            "updated_at": row[12],
            "completed_at": row[13],
        }


async def complete_payment(
    payment_id: int,
    provider_payment_id: str | None = None,
    telegram_payment_charge_id: str | None = None,
):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("BEGIN IMMEDIATE")
        cursor = await db.execute(
            """
            SELECT
                telegram_id, plan_id, status
            FROM payments
            WHERE id = ?
            LIMIT 1
            """,
            (payment_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            await db.rollback()
            return {"ok": False, "reason": "not_found"}

        telegram_id, plan_id, status = row
        if status == "succeeded":
            await db.commit()
            return {"ok": True, "already_completed": True, "telegram_id": telegram_id, "plan_id": plan_id}

        now = utc_now()
        cursor = await db.execute(
            """
            SELECT subscription_expires_at, subscription_forever,
                   trial_expires_at
            FROM users
            WHERE telegram_id = ?
            LIMIT 1
            """,
            (telegram_id,),
        )
        user = await cursor.fetchone()
        if user is None:
            await db.rollback()
            return {"ok": False, "reason": "user_not_found"}

        subscription_expires = parse_datetime(user[0])
        trial_expires = parse_datetime(user[2])

        if plan_id == "lifetime":
            await db.execute(
                """
                UPDATE users
                SET subscription_forever = 1,
                    subscription_expires_at = NULL
                WHERE telegram_id = ?
                """,
                (telegram_id,),
            )
        else:
            days_map = {
                "month": 30,
                "3months": 90,
                "year": 365,
            }
            days = days_map.get(plan_id)
            if not days:
                await db.rollback()
                return {"ok": False, "reason": "invalid_plan"}

            candidates = [now]
            if subscription_expires and subscription_expires > now:
                candidates.append(subscription_expires)
            if trial_expires and trial_expires > now:
                candidates.append(trial_expires)

            base_date = max(candidates)
            expiration = base_date + timedelta(days=days)

            await db.execute(
                """
                UPDATE users
                SET subscription_expires_at = ?,
                    subscription_forever = 0
                WHERE telegram_id = ?
                """,
                (expiration.isoformat(), telegram_id),
            )

        await db.execute(
            """
            UPDATE payments
            SET status = 'succeeded',
                provider_payment_id = COALESCE(?, provider_payment_id),
                telegram_payment_charge_id = COALESCE(?, telegram_payment_charge_id),
                updated_at = ?,
                completed_at = ?
            WHERE id = ?
            """,
            (
                provider_payment_id,
                telegram_payment_charge_id,
                now.isoformat(),
                now.isoformat(),
                payment_id,
            ),
        )
        await db.commit()

        return {"ok": True, "already_completed": False, "telegram_id": telegram_id, "plan_id": plan_id}


async def fail_payment(payment_id: int, status: str = "canceled"):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE payments
            SET status = ?, updated_at = ?
            WHERE id = ?
              AND status = 'pending'
            """,
            (status, utc_now_str(), payment_id),
        )
        await db.commit()


async def get_payment_statistics():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT
                COUNT(*),
                COALESCE(SUM(CASE WHEN status = 'succeeded' THEN amount ELSE 0 END), 0)
            FROM payments
            WHERE currency = 'RUB'
            """
        )
        rub = await cursor.fetchone()

        cursor = await db.execute(
            """
            SELECT
                COUNT(*),
                COALESCE(SUM(CASE WHEN status = 'succeeded' THEN amount ELSE 0 END), 0)
            FROM payments
            WHERE currency = 'XTR'
            """
        )
        stars = await cursor.fetchone()

        return {
            "rub_count": rub[0],
            "rub_amount": rub[1],
            "stars_count": stars[0],
            "stars_amount": stars[1],
        }


# =========================================================
# USER STATISTICS
# =========================================================

async def get_user_statistics():

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            SELECT COUNT(*)
            FROM users
            """
        )

        total_users = (
            await cursor.fetchone()
        )[0]

        now = utc_now_str()

        cursor = await db.execute(
            """
            SELECT COUNT(*)
            FROM users
            WHERE
                subscription_expires_at > ?
                OR
                trial_expires_at > ?
                OR
                role = 'owner'
            """,
            (
                now,
                now,
            ),
        )

        active_users = (
            await cursor.fetchone()
        )[0]

        cursor = await db.execute(
            """
            SELECT COUNT(*)
            FROM users
            WHERE
                trial_expires_at > ?
                AND
                (
                    subscription_expires_at IS NULL
                    OR
                    subscription_expires_at <= ?
                )
            """,
            (
                now,
                now,
            ),
        )

        trials = (
            await cursor.fetchone()
        )[0]

        cursor = await db.execute(
            """
            SELECT COUNT(*)
            FROM users
            WHERE
                subscription_expires_at > ?
            """,
            (now,),
        )

        subscriptions = (
            await cursor.fetchone()
        )[0]

        return {
            "total_users": total_users,
            "active_users": active_users,
            "trials": trials,
            "subscriptions": subscriptions,
        }


# =========================================================
# BUSINESS CONNECTION
# =========================================================

async def save_business_connection(
    telegram_id: int,
    business_connection_id: str,
    is_enabled: bool,
):

    now = utc_now_str()

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            """
            INSERT INTO business_connections (
                business_connection_id,
                telegram_id,
                is_enabled,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(business_connection_id)
            DO UPDATE SET
                telegram_id = excluded.telegram_id,
                is_enabled = excluded.is_enabled,
                updated_at = excluded.updated_at
            """,
            (
                business_connection_id,
                telegram_id,
                1 if is_enabled else 0,
                now,
                now,
            ),
        )

        await db.commit()


# =========================================================
# SAVE MESSAGE
# =========================================================

async def save_message(
    message_id: int,
    chat_id: int,
    user_id: int | None,
    owner_id: int,
    business_connection_id: str,
    user_name: str | None,
    text: str | None,
    media_type: str | None,
    file_id: str | None,
    file_name: str | None,
    mime_type: str | None,
    file_size: int | None,
    duration: int | None,
    width: int | None,
    height: int | None,
    caption: str | None,
):

    now = utc_now_str()

    async with aiosqlite.connect(DB_PATH) as db:

        # =================================================
        # НЕ полагаемся на ON CONFLICT старой БД.
        # Сначала ищем существующее сообщение.
        # =================================================

        cursor = await db.execute(
            """
            SELECT id
            FROM messages
            WHERE
                message_id = ?
                AND
                business_connection_id = ?
            LIMIT 1
            """,
            (
                message_id,
                business_connection_id,
            ),
        )

        existing = await cursor.fetchone()

        if existing:

            await db.execute(
                """
                UPDATE messages
                SET
                    chat_id = ?,
                    user_id = ?,
                    owner_id = ?,
                    user_name = ?,
                    text = ?,
                    media_type = ?,
                    file_id = ?,
                    file_name = ?,
                    mime_type = ?,
                    file_size = ?,
                    duration = ?,
                    width = ?,
                    height = ?,
                    caption = ?,
                    status = 'active',
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    chat_id,
                    user_id,
                    owner_id,
                    user_name,
                    text,
                    media_type,
                    file_id,
                    file_name,
                    mime_type,
                    file_size,
                    duration,
                    width,
                    height,
                    caption,
                    now,
                    existing[0],
                ),
            )

        else:

            await db.execute(
                """
                INSERT INTO messages (
                    message_id,
                    chat_id,
                    user_id,
                    owner_id,
                    business_connection_id,
                    user_name,
                    text,
                    media_type,
                    file_id,
                    file_name,
                    mime_type,
                    file_size,
                    duration,
                    width,
                    height,
                    caption,
                    status,
                    created_at,
                    updated_at
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, 'active',
                    ?, ?
                )
                """,
                (
                    message_id,
                    chat_id,
                    user_id,
                    owner_id,
                    business_connection_id,
                    user_name,
                    text,
                    media_type,
                    file_id,
                    file_name,
                    mime_type,
                    file_size,
                    duration,
                    width,
                    height,
                    caption,
                    now,
                    now,
                ),
            )

        await db.commit()


# =========================================================
# GET MESSAGE INFO
# =========================================================

async def get_message_info(
    message_id: int,
    business_connection_id: str,
):

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            SELECT
                message_id,
                chat_id,
                user_id,
                user_name,
                owner_id,
                business_connection_id,
                text,
                media_type,
                file_id,
                file_name,
                mime_type,
                file_size,
                duration,
                width,
                height,
                caption,
                status,
                created_at,
                updated_at
            FROM messages
            WHERE
                message_id = ?
                AND
                business_connection_id = ?
            LIMIT 1
            """,
            (
                message_id,
                business_connection_id,
            ),
        )

        row = await cursor.fetchone()

        if row is None:
            return None

        return {
            "message_id": row[0],
            "chat_id": row[1],
            "user_id": row[2],
            "user_name": row[3],
            "owner_id": row[4],
            "business_connection_id": row[5],
            "text": row[6],
            "media_type": row[7],
            "file_id": row[8],
            "file_name": row[9],
            "mime_type": row[10],
            "file_size": row[11],
            "duration": row[12],
            "width": row[13],
            "height": row[14],
            "caption": row[15],
            "status": row[16],
            "created_at": row[17],
            "updated_at": row[18],
        }


# =========================================================
# EDITED MESSAGE
# =========================================================

async def save_edited_message(
    message_id: int,
    chat_id: int,
    business_connection_id: str,
    text: str | None,
):

    async with aiosqlite.connect(DB_PATH) as db:

        # =================================================
        # Получаем СТАРУЮ версию
        # =================================================

        cursor = await db.execute(
            """
            SELECT
                text,
                caption
            FROM messages
            WHERE
                message_id = ?
                AND
                business_connection_id = ?
            LIMIT 1
            """,
            (
                message_id,
                business_connection_id,
            ),
        )

        row = await cursor.fetchone()

        if row:

            previous_text = (
                row[0]
                if row[0] is not None
                else row[1]
            )

        else:

            previous_text = None

        # =================================================
        # Текущая версия
        # =================================================

        cursor = await db.execute(
            """
            SELECT
                COALESCE(
                    MAX(version),
                    0
                )
            FROM message_versions
            WHERE
                message_id = ?
                AND
                business_connection_id = ?
            """,
            (
                message_id,
                business_connection_id,
            ),
        )

        row_version = await cursor.fetchone()

        current_version = (
            row_version[0]
            if row_version
            else 0
        )

        version = current_version + 1

        now = utc_now_str()

        # =================================================
        # Сохраняем НОВУЮ версию
        # =================================================

        await db.execute(
            """
            INSERT INTO message_versions (
                message_id,
                business_connection_id,
                version,
                text,
                created_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                message_id,
                business_connection_id,
                version,
                text,
                now,
            ),
        )

        # =================================================
        # Обновляем основное сообщение
        # =================================================

        cursor = await db.execute(
            """
            SELECT id
            FROM messages
            WHERE
                message_id = ?
                AND
                business_connection_id = ?
            LIMIT 1
            """,
            (
                message_id,
                business_connection_id,
            ),
        )

        message_row = await cursor.fetchone()

        if message_row:

            await db.execute(
                """
                UPDATE messages
                SET
                    chat_id = ?,
                    text = ?,
                    caption = ?,
                    status = 'active',
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    chat_id,
                    text,
                    text,
                    now,
                    message_row[0],
                ),
            )

        else:

            # -------------------------------------------------
            # Если исходное сообщение не было сохранено,
            # создаём минимальную запись.
            # Это предотвращает потерю автора при следующих
            # событиях.
            # -------------------------------------------------

            await db.execute(
                """
                INSERT INTO messages (
                    message_id,
                    chat_id,
                    user_id,
                    owner_id,
                    business_connection_id,
                    user_name,
                    text,
                    media_type,
                    file_id,
                    file_name,
                    mime_type,
                    file_size,
                    duration,
                    width,
                    height,
                    caption,
                    status,
                    created_at,
                    updated_at
                )
                VALUES (
                    ?, ?, NULL, 0, ?,
                    NULL, ?, NULL, NULL,
                    NULL, NULL, NULL,
                    NULL, NULL, NULL,
                    ?, 'active', ?, ?
                )
                """,
                (
                    message_id,
                    chat_id,
                    business_connection_id,
                    text,
                    text,
                    now,
                    now,
                ),
            )

        await db.commit()

        return previous_text, version


# =========================================================
# DELETE MESSAGE
# =========================================================

async def mark_message_deleted(
    message_id: int,
    business_connection_id: str,
):

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            """
            UPDATE messages
            SET
                status = 'deleted',
                updated_at = ?
            WHERE
                message_id = ?
                AND
                business_connection_id = ?
            """,
            (
                utc_now_str(),
                message_id,
                business_connection_id,
            ),
        )

        await db.commit()


# =========================================================
# OWNER NOTIFICATIONS
# =========================================================

async def get_owner_notifications_enabled():

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            SELECT
                owner_notifications_enabled
            FROM owner_settings
            WHERE id = 1
            """
        )

        row = await cursor.fetchone()

        if row is None:
            return True

        return bool(row[0])


async def set_owner_notifications_enabled(
    enabled: bool,
):

    now = utc_now_str()

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            """
            INSERT INTO owner_settings (
                id,
                owner_notifications_enabled,
                one_time_media_notifications_enabled,
                updated_at
            )
            VALUES (1, ?, 1, ?)
            ON CONFLICT(id)
            DO UPDATE SET
                owner_notifications_enabled =
                    excluded.owner_notifications_enabled,
                updated_at =
                    excluded.updated_at
            """,
            (
                1 if enabled else 0,
                now,
            ),
        )

        await db.commit()


# =========================================================
# ONE-TIME MEDIA NOTIFICATIONS
# =========================================================

async def get_one_time_media_notifications_enabled():

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            SELECT
                one_time_media_notifications_enabled
            FROM owner_settings
            WHERE id = 1
            """
        )

        row = await cursor.fetchone()

        if row is None:
            return True

        return bool(row[0])


async def set_one_time_media_notifications_enabled(
    enabled: bool,
):

    now = utc_now_str()

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            """
            INSERT INTO owner_settings (
                id,
                owner_notifications_enabled,
                one_time_media_notifications_enabled,
                updated_at
            )
            VALUES (1, 1, ?, ?)
            ON CONFLICT(id)
            DO UPDATE SET
                one_time_media_notifications_enabled =
                    excluded.one_time_media_notifications_enabled,
                updated_at =
                    excluded.updated_at
            """,
            (
                1 if enabled else 0,
                now,
            ),
        )

        await db.commit()


# =========================================================
# CHAT MUTE
# =========================================================

async def set_chat_mute(
    owner_id: int,
    chat_id: int,
    business_connection_id: str,
    minutes: int,
):

    if minutes <= 0:
        raise ValueError(
            "minutes должен быть больше 0"
        )

    now = utc_now()

    muted_until = (
        now
        + timedelta(minutes=minutes)
    )

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            """
            INSERT INTO chat_mutes (
                owner_id,
                chat_id,
                business_connection_id,
                muted_until,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(
                business_connection_id,
                chat_id
            )
            DO UPDATE SET
                owner_id = excluded.owner_id,
                muted_until = excluded.muted_until,
                updated_at = excluded.updated_at
            """,
            (
                owner_id,
                chat_id,
                business_connection_id,
                muted_until.isoformat(),
                now.isoformat(),
                now.isoformat(),
            ),
        )

        await db.commit()

    return muted_until.isoformat()


async def get_chat_mute(
    chat_id: int,
    business_connection_id: str,
):

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            SELECT
                owner_id,
                chat_id,
                business_connection_id,
                muted_until,
                created_at,
                updated_at
            FROM chat_mutes
            WHERE
                chat_id = ?
                AND
                business_connection_id = ?
            LIMIT 1
            """,
            (
                chat_id,
                business_connection_id,
            ),
        )

        row = await cursor.fetchone()

        if row is None:
            return None

        muted_until = parse_datetime(
            row[3]
        )

        if muted_until is None:
            return None

        if muted_until <= utc_now():

            await db.execute(
                """
                DELETE FROM chat_mutes
                WHERE
                    chat_id = ?
                    AND
                    business_connection_id = ?
                """,
                (
                    chat_id,
                    business_connection_id,
                ),
            )

            await db.commit()

            return None

        return {
            "owner_id": row[0],
            "chat_id": row[1],
            "business_connection_id": row[2],
            "muted_until": row[3],
            "created_at": row[4],
            "updated_at": row[5],
        }


async def clear_chat_mute(
    chat_id: int,
    business_connection_id: str,
):

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            SELECT muted_until
            FROM chat_mutes
            WHERE
                chat_id = ?
                AND
                business_connection_id = ?
            LIMIT 1
            """,
            (
                chat_id,
                business_connection_id,
            ),
        )

        row = await cursor.fetchone()

        previous_until = (
            row[0]
            if row
            else None
        )

        await db.execute(
            """
            DELETE FROM chat_mutes
            WHERE
                chat_id = ?
                AND
                business_connection_id = ?
            """,
            (
                chat_id,
                business_connection_id,
            ),
        )

        await db.commit()

        return previous_until


async def get_expired_mutes():

    now = utc_now_str()

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            SELECT
                owner_id,
                chat_id,
                business_connection_id,
                muted_until,
                created_at,
                updated_at
            FROM chat_mutes
            WHERE muted_until <= ?
            ORDER BY muted_until ASC
            """,
            (now,),
        )

        rows = await cursor.fetchall()

        if not rows:
            return []

        result = []

        for row in rows:

            result.append(
                {
                    "owner_id": row[0],
                    "chat_id": row[1],
                    "business_connection_id": row[2],
                    "muted_until": row[3],
                    "created_at": row[4],
                    "updated_at": row[5],
                }
            )

        await db.executemany(
            """
            DELETE FROM chat_mutes
            WHERE
                business_connection_id = ?
                AND
                chat_id = ?
            """,
            [
                (
                    row[2],
                    row[1],
                )
                for row in rows
            ],
        )

        await db.commit()

        return result


# =========================================================
# CHAT WARNINGS
# =========================================================

async def set_chat_warning(
    owner_id: int,
    chat_id: int,
    business_connection_id: str,
    remaining: int,
    warning_message_id: int | None,
):
    if remaining <= 0:
        raise ValueError("remaining должен быть больше 0")

    now = utc_now_str()

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO chat_warnings (
                owner_id,
                chat_id,
                business_connection_id,
                remaining,
                warning_message_id,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(
                business_connection_id,
                chat_id
            )
            DO UPDATE SET
                owner_id = excluded.owner_id,
                remaining = excluded.remaining,
                warning_message_id = excluded.warning_message_id,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at
            """,
            (
                owner_id,
                chat_id,
                business_connection_id,
                remaining,
                warning_message_id,
                now,
                now,
            ),
        )
        await db.commit()


async def get_chat_warning(
    chat_id: int,
    business_connection_id: str,
):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT
                owner_id,
                chat_id,
                business_connection_id,
                remaining,
                warning_message_id,
                created_at,
                updated_at
            FROM chat_warnings
            WHERE chat_id = ?
              AND business_connection_id = ?
            LIMIT 1
            """,
            (chat_id, business_connection_id),
        )
        row = await cursor.fetchone()

        if row is None:
            return None

        return {
            "owner_id": row[0],
            "chat_id": row[1],
            "business_connection_id": row[2],
            "remaining": row[3],
            "warning_message_id": row[4],
            "created_at": row[5],
            "updated_at": row[6],
        }


async def decrement_chat_warning(
    chat_id: int,
    business_connection_id: str,
):
    now = utc_now_str()

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT
                owner_id,
                chat_id,
                business_connection_id,
                remaining,
                warning_message_id,
                created_at,
                updated_at
            FROM chat_warnings
            WHERE chat_id = ?
              AND business_connection_id = ?
            LIMIT 1
            """,
            (chat_id, business_connection_id),
        )
        row = await cursor.fetchone()

        if row is None:
            return None

        new_remaining = max(0, int(row[3]) - 1)

        if new_remaining <= 0:
            await db.execute(
                """
                DELETE FROM chat_warnings
                WHERE chat_id = ?
                  AND business_connection_id = ?
                """,
                (chat_id, business_connection_id),
            )
        else:
            await db.execute(
                """
                UPDATE chat_warnings
                SET remaining = ?,
                    updated_at = ?
                WHERE chat_id = ?
                  AND business_connection_id = ?
                """,
                (
                    new_remaining,
                    now,
                    chat_id,
                    business_connection_id,
                ),
            )

        await db.commit()

        return {
            "owner_id": row[0],
            "chat_id": row[1],
            "business_connection_id": row[2],
            "previous_remaining": row[3],
            "remaining": new_remaining,
            "warning_message_id": row[4],
            "created_at": row[5],
            "updated_at": now,
        }


async def clear_chat_warning(
    chat_id: int,
    business_connection_id: str,
):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT warning_message_id
            FROM chat_warnings
            WHERE chat_id = ?
              AND business_connection_id = ?
            LIMIT 1
            """,
            (chat_id, business_connection_id),
        )
        row = await cursor.fetchone()

        await db.execute(
            """
            DELETE FROM chat_warnings
            WHERE chat_id = ?
              AND business_connection_id = ?
            """,
            (chat_id, business_connection_id),
        )
        await db.commit()

        return row[0] if row else None


# =========================================================
# SERVICE-DELETED MESSAGES
# =========================================================

async def mark_service_deleted_message(
    business_connection_id: str,
    chat_id: int,
    message_id: int,
    reason: str,
):

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            """
            INSERT INTO service_deleted_messages (
                business_connection_id,
                chat_id,
                message_id,
                reason,
                created_at
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(
                business_connection_id,
                chat_id,
                message_id
            )
            DO UPDATE SET
                reason = excluded.reason,
                created_at = excluded.created_at
            """,
            (
                business_connection_id,
                chat_id,
                message_id,
                reason,
                utc_now_str(),
            ),
        )

        await db.commit()


async def consume_service_deleted_message(
    business_connection_id: str,
    chat_id: int | None,
    message_id: int,
):

    async with aiosqlite.connect(DB_PATH) as db:

        if chat_id is not None:

            cursor = await db.execute(
                """
                SELECT reason
                FROM service_deleted_messages
                WHERE
                    business_connection_id = ?
                    AND
                    chat_id = ?
                    AND
                    message_id = ?
                LIMIT 1
                """,
                (
                    business_connection_id,
                    chat_id,
                    message_id,
                ),
            )

            row = await cursor.fetchone()

        else:

            cursor = await db.execute(
                """
                SELECT
                    reason,
                    chat_id
                FROM service_deleted_messages
                WHERE
                    business_connection_id = ?
                    AND
                    message_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (
                    business_connection_id,
                    message_id,
                ),
            )

            row = await cursor.fetchone()

        if row is None:
            return None

        reason = row[0]

        if chat_id is not None:

            await db.execute(
                """
                DELETE FROM service_deleted_messages
                WHERE
                    business_connection_id = ?
                    AND
                    chat_id = ?
                    AND
                    message_id = ?
                """,
                (
                    business_connection_id,
                    chat_id,
                    message_id,
                ),
            )

        else:

            await db.execute(
                """
                DELETE FROM service_deleted_messages
                WHERE
                    business_connection_id = ?
                    AND
                    message_id = ?
                """,
                (
                    business_connection_id,
                    message_id,
                ),
            )

        await db.commit()

        return reason


# =========================================================
# CUSTOM TEMPLATES
# =========================================================

async def create_custom_template(
    owner_id: int,
    trigger: str,
    actions: list[str],
):
    trigger = trigger.strip()
    actions = [str(action).strip() for action in actions if str(action).strip()]

    if not trigger or not trigger.startswith('.'):
        raise ValueError("❌ Триггер должен начинаться с точки.")

    if not actions or len(actions) > 3:
        raise ValueError("❌ В заготовке должно быть от 1 до 3 действий.")

    now = utc_now_str()

    async with aiosqlite.connect(DB_PATH) as db:
        try:
            cursor = await db.execute(
                """
                INSERT INTO custom_templates (owner_id, trigger, created_at)
                VALUES (?, ?, ?)
                """,
                (owner_id, trigger, now),
            )
            template_id = cursor.lastrowid

            for position, content in enumerate(actions, 1):
                action_type = "command" if content.startswith('.') else "text"
                await db.execute(
                    """
                    INSERT INTO custom_template_actions
                    (template_id, position, action_type, content)
                    VALUES (?, ?, ?, ?)
                    """,
                    (template_id, position, action_type, content),
                )

            await db.commit()
            return template_id

        except aiosqlite.IntegrityError:
            await db.rollback()
            raise ValueError("❌ Такая заготовка уже существует.")


async def get_custom_templates(owner_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT id, trigger, created_at
            FROM custom_templates
            WHERE owner_id = ?
            ORDER BY id DESC
            """,
            (owner_id,),
        )
        rows = await cursor.fetchall()
        return [
            {"id": row[0], "trigger": row[1], "created_at": row[2]}
            for row in rows
        ]


async def get_custom_template(owner_id: int, template_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT id, trigger, created_at
            FROM custom_templates
            WHERE id = ? AND owner_id = ?
            LIMIT 1
            """,
            (template_id, owner_id),
        )
        row = await cursor.fetchone()
        if row is None:
            return None

        cursor = await db.execute(
            """
            SELECT position, action_type, content
            FROM custom_template_actions
            WHERE template_id = ?
            ORDER BY position ASC
            """,
            (template_id,),
        )
        action_rows = await cursor.fetchall()

        return {
            "id": row[0],
            "trigger": row[1],
            "created_at": row[2],
            "actions": [row[2] for row in action_rows],
            "action_details": [
                {"position": r[0], "action_type": r[1], "content": r[2]}
                for r in action_rows
            ],
        }


async def get_custom_template_by_trigger(owner_id: int, trigger: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT id
            FROM custom_templates
            WHERE owner_id = ? AND lower(trigger) = lower(?)
            LIMIT 1
            """,
            (owner_id, trigger.strip()),
        )
        row = await cursor.fetchone()

    if row is None:
        return None

    return await get_custom_template(owner_id, row[0])


async def delete_custom_template(owner_id: int, template_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys = ON")
        cursor = await db.execute(
            """
            DELETE FROM custom_templates
            WHERE id = ? AND owner_id = ?
            """,
            (template_id, owner_id),
        )
        await db.commit()
        return cursor.rowcount > 0



# =========================================================
# AUTO REPLIES — SETTINGS
# =========================================================

AUTO_REPLY_DEFAULTS = {
    "enabled": 0,
    "day_text": None,
    "night_text": None,
    "interval_minutes": 30,
    "day_start": "08:00",
    "day_end": "23:00",
    "night_start": "23:00",
    "night_end": "08:00",
}


async def get_auto_reply_settings(business_connection_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT
                business_connection_id,
                owner_id,
                enabled,
                day_text,
                night_text,
                interval_minutes,
                day_start,
                day_end,
                night_start,
                night_end,
                created_at,
                updated_at
            FROM auto_reply_settings
            WHERE business_connection_id = ?
            LIMIT 1
            """,
            (business_connection_id,),
        )
        row = await cursor.fetchone()

        if row is None:
            cursor = await db.execute(
                """
                SELECT telegram_id
                FROM business_connections
                WHERE business_connection_id = ?
                LIMIT 1
                """,
                (business_connection_id,),
            )
            owner_row = await cursor.fetchone()

            if owner_row is None:
                return dict(AUTO_REPLY_DEFAULTS)

            now = utc_now_str()
            await db.execute(
                """
                INSERT OR IGNORE INTO auto_reply_settings (
                    business_connection_id, owner_id, enabled, day_text, night_text,
                    interval_minutes, day_start, day_end, night_start, night_end,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    business_connection_id, owner_row[0], 0, None, None,
                    30, "08:00", "23:00", "23:00", "08:00", now, now,
                ),
            )
            await db.commit()

            cursor = await db.execute(
                """
                SELECT
                    business_connection_id, owner_id, enabled, day_text, night_text,
                    interval_minutes, day_start, day_end, night_start, night_end,
                    created_at, updated_at
                FROM auto_reply_settings
                WHERE business_connection_id = ?
                LIMIT 1
                """,
                (business_connection_id,),
            )
            row = await cursor.fetchone()

        return {
            "business_connection_id": row[0],
            "owner_id": row[1],
            "enabled": row[2],
            "day_text": row[3],
            "night_text": row[4],
            "interval_minutes": row[5],
            "day_start": row[6],
            "day_end": row[7],
            "night_start": row[8],
            "night_end": row[9],
            "created_at": row[10],
            "updated_at": row[11],
        }


async def update_auto_reply_settings(
    business_connection_id: str,
    **fields,
):
    allowed = {
        "enabled",
        "day_text",
        "night_text",
        "interval_minutes",
        "day_start",
        "day_end",
        "night_start",
        "night_end",
    }

    values = {
        key: value
        for key, value in fields.items()
        if key in allowed
    }

    if not values:
        return await get_auto_reply_settings(business_connection_id)

    current = await get_auto_reply_settings(business_connection_id)
    now = utc_now_str()

    set_parts = []
    params = []

    for key, value in values.items():
        set_parts.append(f"{key} = ?")
        params.append(value)

    set_parts.append("updated_at = ?")
    params.append(now)
    params.append(business_connection_id)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            f"""
            UPDATE auto_reply_settings
            SET {", ".join(set_parts)}
            WHERE business_connection_id = ?
            """,
            params,
        )
        await db.commit()

    return await get_auto_reply_settings(business_connection_id)


async def claim_auto_reply_cooldown(
    business_connection_id: str,
    chat_id: int,
    interval_minutes: int,
):
    """
    Атомарно резервирует отправку автоответа для конкретного чата.

    Это защищает от ситуации, когда два входящих сообщения приходят
    одновременно и оба пытаются отправить автоответ.
    """

    interval_minutes = max(1, int(interval_minutes))
    now = utc_now()
    now_str = now.isoformat()
    threshold = (
        now - timedelta(minutes=interval_minutes)
    ).isoformat()

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("BEGIN IMMEDIATE")

        cursor = await db.execute(
            """
            SELECT last_sent_at
            FROM auto_reply_cooldowns
            WHERE business_connection_id = ?
              AND chat_id = ?
            LIMIT 1
            """,
            (business_connection_id, chat_id),
        )
        row = await cursor.fetchone()

        if row is not None:
            try:
                last_sent = datetime.fromisoformat(row[0])
            except Exception:
                last_sent = None

            if last_sent is not None and last_sent > now - timedelta(
                minutes=interval_minutes
            ):
                await db.rollback()
                return False

            await db.execute(
                """
                UPDATE auto_reply_cooldowns
                SET last_sent_at = ?
                WHERE business_connection_id = ?
                  AND chat_id = ?
                """,
                (
                    now_str,
                    business_connection_id,
                    chat_id,
                ),
            )
        else:
            await db.execute(
                """
                INSERT INTO auto_reply_cooldowns (
                    business_connection_id,
                    chat_id,
                    last_sent_at
                )
                VALUES (?, ?, ?)
                """,
                (
                    business_connection_id,
                    chat_id,
                    now_str,
                ),
            )

        await db.commit()
        return True


async def reset_auto_reply_cooldown(
    business_connection_id: str,
    chat_id: int,
):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            DELETE FROM auto_reply_cooldowns
            WHERE business_connection_id = ?
              AND chat_id = ?
            """,
            (
                business_connection_id,
                chat_id,
            ),
        )
        await db.commit()


# =========================================================
# AUTO REPLY — PER-CHAT EXCLUSION
# =========================================================

async def set_auto_reply_chat_disabled(
    business_connection_id: str,
    chat_id: int,
    disabled: bool,
):
    now = utc_now_str()
    async with aiosqlite.connect(DB_PATH) as db:
        if disabled:
            await db.execute(
                """
                INSERT INTO auto_reply_chat_exclusions (
                    business_connection_id, chat_id, disabled,
                    created_at, updated_at
                )
                VALUES (?, ?, 1, ?, ?)
                ON CONFLICT(business_connection_id, chat_id)
                DO UPDATE SET disabled = 1, updated_at = excluded.updated_at
                """,
                (business_connection_id, chat_id, now, now),
            )
        else:
            await db.execute(
                """
                DELETE FROM auto_reply_chat_exclusions
                WHERE business_connection_id = ? AND chat_id = ?
                """,
                (business_connection_id, chat_id),
            )
        await db.commit()


async def is_auto_reply_chat_disabled(
    business_connection_id: str,
    chat_id: int,
) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT disabled FROM auto_reply_chat_exclusions
            WHERE business_connection_id = ? AND chat_id = ?
            LIMIT 1
            """,
            (business_connection_id, chat_id),
        )
        row = await cursor.fetchone()
        return bool(row and row[0])


# =========================================================
# CLEANUP
# =========================================================

async def cleanup_old_service_deleted_messages(
    hours: int = 24,
):

    if hours <= 0:
        return

    limit = (
        utc_now()
        - timedelta(hours=hours)
    ).isoformat()

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            """
            DELETE FROM service_deleted_messages
            WHERE created_at < ?
            """,
            (limit,),
        )

        await db.commit()


async def cleanup_old_event_data():

    await cleanup_old_service_deleted_messages(
        hours=24
    )