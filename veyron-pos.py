from __future__ import annotations

import os
import re
import smtplib
import shutil
import sqlite3
from contextlib import suppress
from datetime import datetime
from datetime import timedelta
from email.message import EmailMessage
from functools import wraps
from pathlib import Path
from typing import Any

from flask import Flask, flash, redirect, render_template, request, send_file, session, url_for
from dotenv import load_dotenv
try:
    from psycopg import connect as pg_connect
    from psycopg.rows import dict_row
    from psycopg.errors import IntegrityError as PostgresIntegrityError
except ImportError:
    pg_connect = None
    dict_row = None
    PostgresIntegrityError = None
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
INSTANCE_DIR = BASE_DIR / "instance"
DEFAULT_SQLITE_DATABASE = BASE_DIR / "pos.db"
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DATABASE = Path(os.getenv("SQLITE_DATABASE_PATH", str(DEFAULT_SQLITE_DATABASE)))
BACKUP_DIR = Path(os.getenv("BACKUP_DIR", str(BASE_DIR / "backups")))
DATABASE_ENGINE = "postgres" if DATABASE_URL and not DATABASE_URL.startswith("sqlite") else "sqlite"
PUBLIC_LOGO = BASE_DIR / "static" / "public" / "logo.png"
FALLBACK_LOGO = "images/logo.png"
PRODUCT_IMAGES_DIR = Path(os.getenv("PRODUCT_IMAGES_DIR", str(BASE_DIR / "static" / "images" / "products")))
ALLOWED_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}
PLACEHOLDER_MAP = {
    "cakes": "images/products/placeholder-cake.svg",
    "pastries": "images/products/placeholder-pastry.svg",
    "pick-a pika": "images/products/placeholder-pika.svg",
    "beverages": "images/products/placeholder-beverage.svg",
}
DEFAULT_PLACEHOLDER = "images/products/placeholder-default.svg"
BUSINESS_NAME = "Veyron's Cakes and Pastries"
CURRENCY_CODE = "PHP"
VAT_RATE = 0.00  # Set to 0.12 to re-enable 12% VAT
APP_ENV = os.getenv("APP_ENV", "development").strip().lower()
IS_PRODUCTION = APP_ENV == "production"

DEFAULT_CATEGORIES = [
    "Cakes",
    "Pastries",
    "Pick-a Pika",
    "Beverages",
    "Upcoming Specials",
]
DEFAULT_BRANDS = [
    "Veyron's Signature",
    "Pick-a Pika Line",
    "Cafe Kitchen",
    "Seasonal Test Kitchen",
]
DEFAULT_UNITS = ["Slice", "Box", "Piece", "Cup", "Order", "Pack"]
DEFAULT_UNITS_DATA = [
    # (name, symbol, type, conversion_to_base)  base: g for weight, ml for volume, 1 for count
    ("Kilogram", "kg", "weight", 1000),
    ("Gram", "g", "weight", 1),
    ("Pound", "lb", "weight", 453.592),
    ("Ounce", "oz", "weight", 28.3495),
    ("Liter", "L", "volume", 1000),
    ("Milliliter", "ml", "volume", 1),
    ("Cup", "cup", "volume", 240),
    ("Tablespoon", "tbsp", "volume", 15),
    ("Teaspoon", "tsp", "volume", 5),
    ("Piece", "pcs", "count", 1),
    ("Pack", "pack", "count", 1),
    ("Box", "box", "count", 1),
    ("Slice", "slice", "count", 1),
    ("Order", "order", "count", 1),
    ("Serving", "srv", "count", 1),
    ("Dozen", "doz", "count", 12),
    ("Tray", "tray", "count", 1),
]
ALLOWED_PRODUCT_STATUSES = {"active", "upcoming", "inactive"}
ALLOWED_INVENTORY_REASONS = {
    "opening_balance",
    "restock",
    "manual_count",
    "damaged",
    "wastage",
    "sale",
    "purchase_receive",
    "void",
    "refund",
}
ALLOWED_USER_ROLES = {"owner", "admin", "cashier"}
ALLOWED_SALE_STATUSES = {"completed", "voided", "refunded"}
DISCOUNT_PRESETS = {
    "none": 0.0,
    "senior": 0.20,
    "pwd": 0.20,
    "custom": None,
}
DEFAULT_APP_SETTINGS = {
    "auto_print_receipt": "0",
    "cash_drawer_enabled": "0",
    "printer_mode": "browser",
    "drawer_open_note": "Browser mode logs drawer opens but needs a local bridge for real ESC/POS drawer pulses.",
    "alert_low_stock_email": "1",
    "alert_void_refund_email": "1",
    "alert_variance_email": "1",
}
DEFAULT_USERS = [
    {"full_name": "Veyron Owner", "username": "owner", "role": "owner", "pin": "owner123"},
    {"full_name": "Veyron Admin", "username": "admin", "role": "admin", "pin": "admin123"},
    {"full_name": "Veyron Cashier", "username": "cashier", "role": "cashier", "pin": "cashier123"},
]


app = Flask(__name__, instance_path=str(INSTANCE_DIR), instance_relative_config=False)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
app.config.update(
    SECRET_KEY=os.getenv("SECRET_KEY", "veyron-pos-dev-key"),
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=IS_PRODUCTION,
    PERMANENT_SESSION_LIFETIME=timedelta(hours=12),
    MAX_CONTENT_LENGTH=5 * 1024 * 1024,
)

DBIntegrityError = (sqlite3.IntegrityError, PostgresIntegrityError)


def translate_sql(sql: str) -> str:
    if DATABASE_ENGINE != "postgres":
        return sql
    translated_sql = sql.replace("INSERT OR IGNORE", "INSERT")
    translated_sql = translated_sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY")
    translated_sql = translated_sql.replace("REAL", "DOUBLE PRECISION")
    translated_sql = re.sub(r"\?", "%s", translated_sql)
    return translated_sql


class PostgresCursorWrapper:
    def __init__(self, cursor, lastrowid: int | None = None):
        self.cursor = cursor
        self.lastrowid = lastrowid

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()


class PostgresConnectionWrapper:
    def __init__(self, dsn: str):
        self.connection = pg_connect(dsn, row_factory=dict_row)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.connection.commit()
        else:
            self.connection.rollback()
        self.connection.close()

    def execute(self, sql: str, params: tuple | list | None = None):
        cursor = self.connection.cursor()
        cursor.execute(translate_sql(sql), params or ())
        return PostgresCursorWrapper(cursor)

    def executemany(self, sql: str, seq_of_params):
        cursor = self.connection.cursor()
        cursor.executemany(translate_sql(sql), seq_of_params)
        return PostgresCursorWrapper(cursor)

    def executescript(self, script: str):
        cursor = self.connection.cursor()
        statements = [statement.strip() for statement in script.split(";") if statement.strip()]
        for statement in statements:
            cursor.execute(translate_sql(statement))
        return PostgresCursorWrapper(cursor)


def peso(value: float | int | None) -> str:
    amount = float(value or 0)
    return f"{CURRENCY_CODE} {amount:,.2f}"


app.jinja_env.filters["php"] = peso
app.jinja_env.globals["get_product_image_url"] = lambda img, cat=None: get_product_image_url(img, cat)


def get_logo_path() -> str:
    return "public/logo.png" if PUBLIC_LOGO.exists() else FALLBACK_LOGO


@app.context_processor
def inject_template_globals() -> dict[str, object]:
    return {
        "business_name": BUSINESS_NAME,
        "currency_code": CURRENCY_CODE,
        "logo_path": get_logo_path(),
        "vat_rate": VAT_RATE,
        "current_user": get_current_user(),
    }


def get_connection():
    if DATABASE_ENGINE == "postgres":
        return PostgresConnectionWrapper(DATABASE_URL)

    database_path = DATABASE
    if DATABASE_URL.startswith("sqlite:///"):
        database_path = Path(DATABASE_URL.removeprefix("sqlite:///"))
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    return connection


def sql_now() -> str:
    return "CURRENT_TIMESTAMP"


def sql_today() -> str:
    if DATABASE_ENGINE == "postgres":
        return "CURRENT_DATE"
    return "date('now', 'localtime')"


def sql_date_bucket(column: str) -> str:
    if DATABASE_ENGINE == "postgres":
        return f"TO_CHAR(({column})::timestamp, 'YYYY-MM-DD')"
    return f"date({column}, 'localtime')"


def sql_week_bucket(column: str) -> str:
    if DATABASE_ENGINE == "postgres":
        return f"TO_CHAR(({column})::timestamp, 'IYYY-\"W\"IW')"
    return f"strftime('%Y-W%W', datetime({column}, 'localtime'))"


def sql_month_bucket(column: str) -> str:
    if DATABASE_ENGINE == "postgres":
        return f"TO_CHAR(({column})::timestamp, 'YYYY-MM')"
    return f"strftime('%Y-%m', datetime({column}, 'localtime'))"


def sql_year_bucket(column: str) -> str:
    if DATABASE_ENGINE == "postgres":
        return f"TO_CHAR(({column})::timestamp, 'YYYY')"
    return f"strftime('%Y', datetime({column}, 'localtime'))"


def sql_label_for_bucket(bucket_sql: str) -> str:
    if DATABASE_ENGINE == "postgres":
        return bucket_sql
    return bucket_sql


def get_current_user() -> dict[str, object] | None:
    user_id = session.get("user_id")
    if not user_id:
        return None

    with get_connection() as connection:
        user = connection.execute(
            "SELECT id, full_name, username, role, is_active FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()

    if user is None or not user["is_active"]:
        session.clear()
        return None

    return dict(user)


def login_required(*roles: str):
    def decorator(view):
        @wraps(view)
        def wrapped_view(*args, **kwargs):
            user = get_current_user()
            if user is None:
                flash("Log in first to continue.", "error")
                return redirect(url_for("login", next=request.path))
            if roles and user["role"] not in roles:
                flash("You do not have permission to access that page.", "error")
                return redirect(url_for("pos"))
            return view(*args, **kwargs)

        return wrapped_view

    return decorator


def get_setting(key: str, fallback: str | None = None) -> str | None:
    with get_connection() as connection:
        row = connection.execute("SELECT value FROM app_settings WHERE key = ?", (key,)).fetchone()
    if row is None:
        return DEFAULT_APP_SETTINGS.get(key, fallback)
    return row["value"]


def fetch_app_settings() -> dict[str, str]:
    with get_connection() as connection:
        rows = connection.execute("SELECT key, value FROM app_settings").fetchall()
    settings = dict(DEFAULT_APP_SETTINGS)
    settings.update({row["key"]: row["value"] for row in rows})
    return settings


def session_user_id() -> int | None:
    user_id = session.get("user_id")
    return int(user_id) if user_id else None


def log_audit(
    connection: sqlite3.Connection,
    action: str,
    entity_type: str,
    entity_id: int | None = None,
    details: str = "",
) -> None:
    connection.execute(
        """
        INSERT INTO audit_logs (user_id, action, entity_type, entity_id, details)
        VALUES (?, ?, ?, ?, ?)
        """,
        (session_user_id(), action, entity_type, entity_id, details),
    )


def send_email_alert(subject: str, body: str) -> bool:
    smtp_host = os.getenv("SMTP_HOST", "").strip()
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_username = os.getenv("SMTP_USERNAME", "").strip()
    smtp_password = os.getenv("SMTP_PASSWORD", "").strip()
    alert_from_email = os.getenv("ALERT_FROM_EMAIL", smtp_username).strip()
    alert_to_email = os.getenv("ALERT_TO_EMAIL", "").strip()

    if not smtp_host or not alert_from_email or not alert_to_email:
        return False

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = alert_from_email
    message["To"] = alert_to_email
    message.set_content(body)

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as smtp:
            with suppress(Exception):
                smtp.starttls()
            if smtp_username and smtp_password:
                smtp.login(smtp_username, smtp_password)
            smtp.send_message(message)
    except Exception:
        return False
    return True


def should_email_alert(connection, setting_key: str) -> bool:
    row = connection.execute("SELECT value FROM app_settings WHERE key = ?", (setting_key,)).fetchone()
    value = row["value"] if row is not None else DEFAULT_APP_SETTINGS.get(setting_key, "0")
    return value == "1"


def create_owner_alert(
    connection,
    alert_type: str,
    severity: str,
    title: str,
    message: str,
    related_entity_type: str | None = None,
    related_entity_id: int | None = None,
    email_setting_key: str | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO owner_alerts (alert_type, severity, title, message, related_entity_type, related_entity_id)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (alert_type, severity, title, message, related_entity_type, related_entity_id),
    )
    if email_setting_key and should_email_alert(connection, email_setting_key):
        send_email_alert(f"[Veyron POS] {title}", message)


def maybe_create_low_stock_alert(connection, product_id: int, source: str) -> None:
    product = connection.execute(
        "SELECT id, name, stock, reorder_level FROM products WHERE id = ?",
        (product_id,),
    ).fetchone()
    if product is None or product["stock"] > product["reorder_level"]:
        return

    create_owner_alert(
        connection,
        "low_stock",
        "warning" if product["stock"] > 0 else "critical",
        f"Low stock: {product['name']}",
        f"{product['name']} is now at {product['stock']} unit(s), at or below its reorder level of {product['reorder_level']} after {source}.",
        "product",
        product["id"],
        "alert_low_stock_email",
    )


def maybe_create_adjustment_alert(connection, product, quantity_change: int, reason: str) -> None:
    threshold = max(5, int(product["reorder_level"] or 0), int(abs(product["stock"]) * 0.25))
    suspicious_reasons = {"manual_count", "damaged", "wastage"}
    if reason not in suspicious_reasons and abs(quantity_change) < threshold:
        return

    severity = "critical" if abs(quantity_change) >= max(10, threshold * 2) else "warning"
    create_owner_alert(
        connection,
        "inventory_adjustment",
        severity,
        f"Inventory adjustment: {product['name']}",
        f"{product['name']} was adjusted by {quantity_change} unit(s) for reason '{reason}'. New stock is {product['stock'] + quantity_change}.",
        "product",
        product["id"],
        "alert_variance_email",
    )


def copy_database(target_path: Path) -> None:
    if DATABASE_ENGINE == "postgres":
        raise RuntimeError("Use managed PostgreSQL backups or pg_dump for production database backups.")
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DATABASE) as source_connection:
        with sqlite3.connect(target_path) as target_connection:
            source_connection.backup(target_connection)


def fetch_backup_rows() -> list[dict[str, str]]:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    backups = []
    for file_path in sorted(BACKUP_DIR.glob("veyron-pos-backup-*.db"), reverse=True):
        backups.append(
            {
                "name": file_path.name,
                "size": f"{file_path.stat().st_size / 1024:.1f} KB",
                "modified": datetime.fromtimestamp(file_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M"),
            }
        )
    return backups


def fetch_users() -> list[sqlite3.Row]:
    with get_connection() as connection:
        return connection.execute(
            "SELECT id, full_name, username, role, is_active, last_login FROM users ORDER BY role, username"
        ).fetchall()


def fetch_suppliers() -> list[sqlite3.Row]:
    with get_connection() as connection:
        return connection.execute(
            "SELECT id, name, contact_person, phone, email, notes, is_active FROM suppliers ORDER BY is_active DESC, name ASC"
        ).fetchall()


def fetch_purchase_orders() -> list[sqlite3.Row]:
    with get_connection() as connection:
        return connection.execute(
            """
            SELECT
                po.id,
                po.status,
                po.notes,
                po.created_at,
                po.received_at,
                s.name AS supplier_name,
                p.name AS product_name,
                poi.ordered_quantity,
                poi.received_quantity,
                poi.unit_cost
            FROM purchase_orders po
            JOIN suppliers s ON s.id = po.supplier_id
            JOIN purchase_order_items poi ON poi.purchase_order_id = po.id
            JOIN products p ON p.id = poi.product_id
            ORDER BY po.created_at DESC, po.id DESC
            LIMIT 20
            """
        ).fetchall()


def fetch_open_stock_count() -> tuple[sqlite3.Row | None, list[sqlite3.Row]]:
    with get_connection() as connection:
        stock_count = connection.execute(
            "SELECT id, title, status, created_at, completed_at FROM stock_counts WHERE status = 'open' ORDER BY created_at DESC, id DESC LIMIT 1"
        ).fetchone()
        if stock_count is None:
            return None, []
        items = connection.execute(
            """
            SELECT
                sci.id,
                sci.product_id,
                sci.system_stock,
                sci.counted_stock,
                sci.variance,
                p.name,
                p.sku,
                c.name AS category_name
            FROM stock_count_items sci
            JOIN products p ON p.id = sci.product_id
            LEFT JOIN categories c ON c.id = p.category_id
            WHERE sci.stock_count_id = ?
            ORDER BY c.sort_order ASC, p.sort_order ASC, p.id ASC
            """,
            (stock_count["id"],),
        ).fetchall()
    return stock_count, items


def fetch_recent_audit_logs(limit: int = 20) -> list[sqlite3.Row]:
    with get_connection() as connection:
        return connection.execute(
            """
            SELECT
                al.created_at,
                al.action,
                al.entity_type,
                al.entity_id,
                al.details,
                COALESCE(u.full_name, 'System') AS actor_name,
                COALESCE(u.role, 'system') AS actor_role
            FROM audit_logs al
            LEFT JOIN users u ON u.id = al.user_id
            ORDER BY al.created_at DESC, al.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()


def fetch_sales_for_control(limit: int = 20) -> list[sqlite3.Row]:
    with get_connection() as connection:
        return connection.execute(
            """
            SELECT
                s.id,
                s.created_at,
                s.subtotal,
                s.discount_type,
                s.discount_amount,
                s.tax,
                s.total,
                s.payment_method,
                s.status,
                COALESCE(u.full_name, 'Unknown') AS cashier_name
            FROM sales s
            LEFT JOIN users u ON u.id = s.cashier_user_id
            ORDER BY s.created_at DESC, s.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()


def fetch_owner_alerts(limit: int = 20) -> list[sqlite3.Row]:
    with get_connection() as connection:
        return connection.execute(
            """
            SELECT id, alert_type, severity, title, message, related_entity_type, related_entity_id, created_at
            FROM owner_alerts
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()


def build_sale_stock_return(connection, sale_id: int, reason: str) -> None:
    sale_items = connection.execute(
        "SELECT product_id, variant_id, quantity FROM sale_items WHERE sale_id = ?",
        (sale_id,),
    ).fetchall()
    for item in sale_items:
        if item["variant_id"]:
            connection.execute(
                "UPDATE product_variants SET stock = stock + ? WHERE id = ?",
                (item["quantity"], item["variant_id"]),
            )
        else:
            connection.execute(
                "UPDATE products SET stock = stock + ? WHERE id = ?",
                (item["quantity"], item["product_id"]),
            )
        log_stock_movement(connection, item["product_id"], item["quantity"], reason, item["variant_id"])


def ensure_column(connection, table_name: str, column_name: str, definition: str) -> None:
    if DATABASE_ENGINE == "postgres":
        columns = {
            row["column_name"]
            for row in connection.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                """,
                (table_name,),
            ).fetchall()
        }
    else:
        columns = {row["name"] for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()}
    if column_name not in columns:
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def seed_lookup_table(connection, table_name: str, values: list[str]) -> None:
    if DATABASE_ENGINE == "postgres":
        connection.executemany(
            f"INSERT INTO {table_name} (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
            [(value,) for value in values],
        )
        return

    connection.executemany(
        f"INSERT INTO {table_name} (name) VALUES (?) ON CONFLICT(name) DO NOTHING",
        [(value,) for value in values],
    )


def seed_units_data(connection) -> None:
    """Seed extended unit data (symbol, type, conversion) for measurement system."""
    for name, symbol, utype, conversion in DEFAULT_UNITS_DATA:
        if DATABASE_ENGINE == "postgres":
            connection.execute(
                "INSERT INTO units (name, symbol, type, conversion) VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (name) DO UPDATE SET symbol=EXCLUDED.symbol, type=EXCLUDED.type, conversion=EXCLUDED.conversion",
                (name, symbol, utype, conversion),
            )
        else:
            connection.execute(
                "INSERT INTO units (name, symbol, type, conversion) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(name) DO UPDATE SET symbol=excluded.symbol, type=excluded.type, conversion=excluded.conversion",
                (name, symbol, utype, conversion),
            )


def fetch_lookup_ids(connection: sqlite3.Connection, table_name: str) -> dict[str, int]:
    return {row["name"]: row["id"] for row in connection.execute(f"SELECT id, name FROM {table_name}").fetchall()}


def resequence_category_order(connection: sqlite3.Connection) -> None:
    ordered_ids = [
        row["id"]
        for row in connection.execute(
            "SELECT id FROM categories ORDER BY sort_order ASC, id ASC"
        ).fetchall()
    ]
    connection.executemany(
        "UPDATE categories SET sort_order = ? WHERE id = ?",
        [(position, category_id) for position, category_id in enumerate(ordered_ids, start=1)],
    )


def move_category_to_position(connection: sqlite3.Connection, category_id: int, requested_position: int) -> int:
    ordered_ids = [
        row["id"]
        for row in connection.execute(
            "SELECT id FROM categories ORDER BY sort_order ASC, id ASC"
        ).fetchall()
    ]
    if category_id not in ordered_ids:
        raise ValueError("Category not found in sort order list")

    ordered_ids.remove(category_id)
    target_index = max(0, min(requested_position - 1, len(ordered_ids)))
    ordered_ids.insert(target_index, category_id)
    connection.executemany(
        "UPDATE categories SET sort_order = ? WHERE id = ?",
        [(position, row_category_id) for position, row_category_id in enumerate(ordered_ids, start=1)],
    )
    return target_index + 1


def resequence_product_order(connection: sqlite3.Connection) -> None:
    ordered_ids = [
        row["id"]
        for row in connection.execute(
            "SELECT id FROM products ORDER BY sort_order ASC, id ASC"
        ).fetchall()
    ]
    connection.executemany(
        "UPDATE products SET sort_order = ? WHERE id = ?",
        [(position, product_id) for position, product_id in enumerate(ordered_ids, start=1)],
    )


def move_product_to_position(connection: sqlite3.Connection, product_id: int, requested_position: int) -> int:
    ordered_ids = [
        row["id"]
        for row in connection.execute(
            "SELECT id FROM products ORDER BY sort_order ASC, id ASC"
        ).fetchall()
    ]
    if product_id not in ordered_ids:
        raise ValueError("Product not found in sort order list")

    ordered_ids.remove(product_id)
    target_index = max(0, min(requested_position - 1, len(ordered_ids)))
    ordered_ids.insert(target_index, product_id)
    connection.executemany(
        "UPDATE products SET sort_order = ? WHERE id = ?",
        [(position, row_product_id) for position, row_product_id in enumerate(ordered_ids, start=1)],
    )
    return target_index + 1


def log_stock_movement(
    connection: sqlite3.Connection,
    product_id: int,
    quantity_change: int,
    reason: str,
    variant_id: int | None = None,
) -> None:
    if quantity_change == 0 or reason not in ALLOWED_INVENTORY_REASONS:
        return

    connection.execute(
        "INSERT INTO stock_movements (product_id, variant_id, quantity_change, reason) VALUES (?, ?, ?, ?)",
        (product_id, variant_id, quantity_change, reason),
    )


def generate_sku(connection: sqlite3.Connection, product_name: str, category_name: str | None = None) -> str:
    prefix_source = category_name or product_name
    prefix = "".join(char for char in prefix_source.upper() if char.isalnum())[:3] or "PRD"
    attempt = 101
    while True:
        sku = f"{prefix}-{attempt}"
        exists = connection.execute("SELECT 1 FROM products WHERE sku = ?", (sku,)).fetchone()
        if exists is None:
            return sku
        attempt += 1


def redirect_to_admin(section: str):
    return redirect(f"{url_for('admin_dashboard')}#{section}")


def redirect_to_inventory(anchor: str | None = None):
    destination = url_for("inventory_dashboard")
    if anchor:
        destination = f"{destination}#{anchor}"
    return redirect(destination)


def normalize_lookup_name(raw_name: str) -> str:
    return " ".join(part for part in raw_name.strip().split() if part)


def init_db() -> None:
    INSTANCE_DIR.mkdir(parents=True, exist_ok=True)
    with get_connection() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_name TEXT NOT NULL,
                username TEXT NOT NULL UNIQUE,
                role TEXT NOT NULL,
                pin_hash TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                last_login TEXT
            );

            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                action TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id INTEGER,
                details TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id)
            );

            CREATE TABLE IF NOT EXISTS owner_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_type TEXT NOT NULL,
                severity TEXT NOT NULL DEFAULT 'info',
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                related_entity_type TEXT,
                related_entity_id INTEGER,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                sort_order INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS brands (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS units (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                symbol TEXT NOT NULL DEFAULT '',
                type TEXT NOT NULL DEFAULT 'count',
                conversion REAL NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                sku TEXT NOT NULL UNIQUE,
                price REAL NOT NULL,
                stock INTEGER NOT NULL DEFAULT 0,
                category_id INTEGER REFERENCES categories (id),
                brand_id INTEGER REFERENCES brands (id),
                unit_id INTEGER REFERENCES units (id),
                reorder_level INTEGER NOT NULL DEFAULT 5,
                cost REAL NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'active',
                last_restocked TEXT,
                sort_order INTEGER NOT NULL DEFAULT 0,
                image_path TEXT
            );

            CREATE TABLE IF NOT EXISTS product_variants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                sku_suffix TEXT NOT NULL DEFAULT '',
                price REAL NOT NULL,
                cost REAL NOT NULL DEFAULT 0,
                stock INTEGER NOT NULL DEFAULT 0,
                reorder_level INTEGER NOT NULL DEFAULT 5,
                sort_order INTEGER NOT NULL DEFAULT 0,
                is_active INTEGER NOT NULL DEFAULT 1,
                FOREIGN KEY (product_id) REFERENCES products (id)
            );

            CREATE TABLE IF NOT EXISTS sales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                subtotal REAL NOT NULL,
                discount_type TEXT NOT NULL DEFAULT 'none',
                discount_rate REAL NOT NULL DEFAULT 0,
                discount_amount REAL NOT NULL DEFAULT 0,
                discount_note TEXT NOT NULL DEFAULT '',
                tax REAL NOT NULL,
                total REAL NOT NULL,
                payment_method TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'completed',
                original_sale_id INTEGER,
                cashier_user_id INTEGER,
                FOREIGN KEY (original_sale_id) REFERENCES sales (id),
                FOREIGN KEY (cashier_user_id) REFERENCES users (id)
            );

            CREATE TABLE IF NOT EXISTS sale_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sale_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                variant_id INTEGER,
                quantity INTEGER NOT NULL,
                unit_price REAL NOT NULL,
                line_total REAL NOT NULL,
                FOREIGN KEY (sale_id) REFERENCES sales (id),
                FOREIGN KEY (product_id) REFERENCES products (id),
                FOREIGN KEY (variant_id) REFERENCES product_variants (id)
            );

            CREATE TABLE IF NOT EXISTS stock_movements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                variant_id INTEGER,
                quantity_change INTEGER NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products (id),
                FOREIGN KEY (variant_id) REFERENCES product_variants (id)
            );

            CREATE TABLE IF NOT EXISTS suppliers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                contact_person TEXT NOT NULL DEFAULT '',
                phone TEXT NOT NULL DEFAULT '',
                email TEXT NOT NULL DEFAULT '',
                notes TEXT NOT NULL DEFAULT '',
                is_active INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS purchase_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                supplier_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'draft',
                notes TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                received_at TEXT,
                created_by INTEGER,
                FOREIGN KEY (supplier_id) REFERENCES suppliers (id),
                FOREIGN KEY (created_by) REFERENCES users (id)
            );

            CREATE TABLE IF NOT EXISTS purchase_order_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                purchase_order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                ordered_quantity INTEGER NOT NULL,
                received_quantity INTEGER NOT NULL DEFAULT 0,
                unit_cost REAL NOT NULL DEFAULT 0,
                FOREIGN KEY (purchase_order_id) REFERENCES purchase_orders (id),
                FOREIGN KEY (product_id) REFERENCES products (id)
            );

            CREATE TABLE IF NOT EXISTS stock_counts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                completed_at TEXT,
                created_by INTEGER,
                FOREIGN KEY (created_by) REFERENCES users (id)
            );

            CREATE TABLE IF NOT EXISTS stock_count_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_count_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                system_stock INTEGER NOT NULL,
                counted_stock INTEGER,
                variance INTEGER,
                FOREIGN KEY (stock_count_id) REFERENCES stock_counts (id),
                FOREIGN KEY (product_id) REFERENCES products (id)
            );

            CREATE TABLE IF NOT EXISTS daily_inventory_shifts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shift_date TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                opened_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                closed_at TEXT,
                opened_by INTEGER,
                closed_by INTEGER,
                opening_units INTEGER NOT NULL DEFAULT 0,
                closing_units INTEGER,
                units_sold INTEGER DEFAULT 0,
                units_received INTEGER DEFAULT 0,
                units_adjusted INTEGER DEFAULT 0,
                notes TEXT NOT NULL DEFAULT '',
                FOREIGN KEY (opened_by) REFERENCES users (id),
                FOREIGN KEY (closed_by) REFERENCES users (id)
            );

            CREATE TABLE IF NOT EXISTS daily_shift_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shift_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                opening_stock INTEGER NOT NULL,
                closing_stock INTEGER,
                FOREIGN KEY (shift_id) REFERENCES daily_inventory_shifts (id),
                FOREIGN KEY (product_id) REFERENCES products (id)
            );
            """
        )

        ensure_column(connection, "products", "category_id", "INTEGER REFERENCES categories (id)")
        ensure_column(connection, "products", "brand_id", "INTEGER REFERENCES brands (id)")
        ensure_column(connection, "products", "unit_id", "INTEGER REFERENCES units (id)")
        ensure_column(connection, "products", "reorder_level", "INTEGER NOT NULL DEFAULT 5")
        ensure_column(connection, "products", "cost", "REAL NOT NULL DEFAULT 0")
        ensure_column(connection, "products", "status", "TEXT NOT NULL DEFAULT 'active'")
        ensure_column(connection, "products", "last_restocked", "TEXT")
        ensure_column(connection, "products", "sort_order", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(connection, "categories", "sort_order", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(connection, "sales", "discount_type", "TEXT NOT NULL DEFAULT 'none'")
        ensure_column(connection, "sales", "discount_rate", "REAL NOT NULL DEFAULT 0")
        ensure_column(connection, "sales", "discount_amount", "REAL NOT NULL DEFAULT 0")
        ensure_column(connection, "sales", "discount_note", "TEXT NOT NULL DEFAULT ''")
        ensure_column(connection, "sales", "status", "TEXT NOT NULL DEFAULT 'completed'")
        ensure_column(connection, "sales", "original_sale_id", "INTEGER")
        ensure_column(connection, "sales", "cashier_user_id", "INTEGER")

        ensure_column(connection, "units", "symbol", "TEXT NOT NULL DEFAULT ''")
        ensure_column(connection, "units", "type", "TEXT NOT NULL DEFAULT 'count'")
        ensure_column(connection, "units", "conversion", "REAL NOT NULL DEFAULT 1")

        for key, value in DEFAULT_APP_SETTINGS.items():
            connection.execute(
                "INSERT INTO app_settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO NOTHING",
                (key, value),
            )

        for user in DEFAULT_USERS:
            exists = connection.execute(
                "SELECT id FROM users WHERE username = ?",
                (user["username"],),
            ).fetchone()
            if exists is None:
                connection.execute(
                    "INSERT INTO users (full_name, username, role, pin_hash) VALUES (?, ?, ?, ?)",
                    (
                        user["full_name"],
                        user["username"],
                        user["role"],
                        generate_password_hash(user["pin"]),
                    ),
                )

        seed_lookup_table(connection, "categories", DEFAULT_CATEGORIES)
        seed_lookup_table(connection, "brands", DEFAULT_BRANDS)
        seed_lookup_table(connection, "units", DEFAULT_UNITS)
        seed_units_data(connection)
        resequence_category_order(connection)

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    PRODUCT_IMAGES_DIR.mkdir(parents=True, exist_ok=True)


def get_product_image_url(image_path: str | None, category_name: str | None = None) -> str:
    if image_path:
        return image_path
    if category_name:
        return PLACEHOLDER_MAP.get(category_name.lower(), DEFAULT_PLACEHOLDER)
    return DEFAULT_PLACEHOLDER


def save_product_image(file_storage) -> str | None:
    if not file_storage or not file_storage.filename:
        return None
    filename = secure_filename(file_storage.filename)
    if not filename:
        return None
    ext = Path(filename).suffix.lower()
    if ext not in ALLOWED_IMAGE_EXTENSIONS:
        return None
    unique_name = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{filename}"
    dest = PRODUCT_IMAGES_DIR / unique_name
    file_storage.save(str(dest))
    return f"images/products/{unique_name}"


def fetch_pos_products() -> list[sqlite3.Row]:
    with get_connection() as connection:
        return connection.execute(
            """
            SELECT
                p.id,
                p.sort_order,
                p.name,
                p.sku,
                p.price,
                p.stock,
                p.reorder_level,
                p.image_path,
                c.id AS category_id,
                c.sort_order AS category_sort_order,
                c.name AS category_name,
                u.name AS unit_name
            FROM products p
            LEFT JOIN categories c ON c.id = p.category_id
            LEFT JOIN units u ON u.id = p.unit_id
            WHERE p.status = 'active'
            ORDER BY c.sort_order ASC, c.id ASC, p.sort_order ASC, p.id ASC
            """
        ).fetchall()


def fetch_product_variants(product_id: int | None = None) -> list[sqlite3.Row]:
    with get_connection() as connection:
        if product_id is not None:
            return connection.execute(
                """
                SELECT id, product_id, name, sku_suffix, price, cost, stock, reorder_level, sort_order, is_active
                FROM product_variants
                WHERE product_id = ?
                ORDER BY sort_order ASC, id ASC
                """,
                (product_id,),
            ).fetchall()
        return connection.execute(
            """
            SELECT id, product_id, name, sku_suffix, price, cost, stock, reorder_level, sort_order, is_active
            FROM product_variants
            WHERE is_active = 1
            ORDER BY product_id ASC, sort_order ASC, id ASC
            """
        ).fetchall()


def fetch_variants_by_product() -> dict[int, list[dict]]:
    variants = fetch_product_variants()
    grouped: dict[int, list[dict]] = {}
    for v in variants:
        pid = v["product_id"]
        if pid not in grouped:
            grouped[pid] = []
        grouped[pid].append(dict(v))
    return grouped


def fetch_lookup_rows(table_name: str) -> list[sqlite3.Row]:
    if table_name not in {"categories", "brands", "units"}:
        raise ValueError(f"Unsupported lookup table: {table_name}")

    with get_connection() as connection:
        if table_name == "categories":
            return connection.execute(
                "SELECT id, name, sort_order FROM categories ORDER BY sort_order ASC, id ASC"
            ).fetchall()
        if table_name == "units":
            return connection.execute(
                "SELECT id, name, symbol, type, conversion FROM units ORDER BY type, name"
            ).fetchall()
        return connection.execute(f"SELECT id, name FROM {table_name} ORDER BY name").fetchall()


def fetch_admin_products() -> list[sqlite3.Row]:
    with get_connection() as connection:
        return connection.execute(
            """
            SELECT
                p.id,
                p.sort_order,
                p.name,
                p.sku,
                p.price,
                p.cost,
                p.stock,
                p.reorder_level,
                p.status,
                p.image_path,
                p.category_id,
                p.brand_id,
                p.unit_id,
                c.sort_order AS category_sort_order,
                COALESCE(c.name, 'Unassigned') AS category_name,
                COALESCE(b.name, 'Unassigned') AS brand_name,
                COALESCE(u.name, 'Unassigned') AS unit_name,
                COALESCE(u.symbol, '') AS unit_symbol,
                p.last_restocked,
                (p.stock * p.cost) AS stock_cost_value,
                (p.stock * p.price) AS stock_retail_value
            FROM products p
            LEFT JOIN categories c ON c.id = p.category_id
            LEFT JOIN brands b ON b.id = p.brand_id
            LEFT JOIN units u ON u.id = p.unit_id
            ORDER BY p.sort_order ASC, p.id ASC
            """
        ).fetchall()


def fetch_inventory_watchlist() -> list[sqlite3.Row]:
    with get_connection() as connection:
        return connection.execute(
            """
            SELECT
                p.id,
                p.name,
                p.sku,
                p.stock,
                p.reorder_level,
                p.status,
                c.name AS category_name,
                b.name AS brand_name
            FROM products p
            LEFT JOIN categories c ON c.id = p.category_id
            LEFT JOIN brands b ON b.id = p.brand_id
            WHERE p.status = 'active' AND p.stock <= p.reorder_level
            ORDER BY p.stock ASC, p.name ASC
            LIMIT 12
            """
        ).fetchall()


def fetch_recent_stock_movements() -> list[sqlite3.Row]:
    with get_connection() as connection:
        return connection.execute(
            """
            SELECT
                sm.created_at,
                sm.quantity_change,
                sm.reason,
                p.name,
                p.sku,
                p.stock
            FROM stock_movements sm
            JOIN products p ON p.id = sm.product_id
            ORDER BY sm.created_at DESC, sm.id DESC
            LIMIT 12
            """
        ).fetchall()


def fetch_upcoming_products() -> list[sqlite3.Row]:
    with get_connection() as connection:
        return connection.execute(
            """
            SELECT p.name, p.sku, c.name AS category_name
            FROM products p
            LEFT JOIN categories c ON c.id = p.category_id
            WHERE p.status = 'upcoming'
            ORDER BY p.name ASC
            LIMIT 6
            """
        ).fetchall()


def fetch_dashboard_metrics() -> dict[str, object]:
    with get_connection() as connection:
        sales_summary = connection.execute(
            """
            SELECT
                COUNT(*) AS receipt_count,
                COALESCE(SUM(total), 0) AS revenue,
                COALESCE(AVG(total), 0) AS average_ticket
            FROM sales
            WHERE status = 'completed'
            """
        ).fetchone()
        inventory_summary = connection.execute(
            """
            SELECT
                COUNT(*) AS item_count,
                COALESCE(SUM(CASE WHEN status = 'active' THEN stock ELSE 0 END), 0) AS units_in_stock,
                COALESCE(SUM(CASE WHEN status = 'active' AND stock <= reorder_level THEN 1 ELSE 0 END), 0) AS low_stock_count,
                COALESCE(SUM(CASE WHEN status = 'active' AND stock = 0 THEN 1 ELSE 0 END), 0) AS out_of_stock_count,
                COALESCE(SUM(CASE WHEN status = 'active' THEN stock * cost ELSE 0 END), 0) AS inventory_cost_value,
                COALESCE(SUM(CASE WHEN status = 'active' THEN stock * price ELSE 0 END), 0) AS inventory_retail_value,
                COALESCE(SUM(CASE WHEN status = 'upcoming' THEN 1 ELSE 0 END), 0) AS upcoming_count
            FROM products
            """
        ).fetchone()
        recent_sales = connection.execute(
            """
            SELECT id, created_at, total, payment_method, status, discount_amount
            FROM sales
            ORDER BY created_at DESC, id DESC
            LIMIT 8
            """
        ).fetchall()
        inventory = connection.execute(
            """
            SELECT
                p.name,
                p.sku,
                p.price,
                p.cost,
                p.stock,
                p.reorder_level,
                c.name AS category_name,
                p.status
            FROM products p
            LEFT JOIN categories c ON c.id = p.category_id
            ORDER BY CASE p.status WHEN 'active' THEN 0 WHEN 'upcoming' THEN 1 ELSE 2 END, p.stock ASC, p.name ASC
            LIMIT 14
            """
        ).fetchall()
        category_mix = connection.execute(
            """
            SELECT c.name, COUNT(p.id) AS product_count
            FROM categories c
            LEFT JOIN products p ON p.category_id = c.id
            GROUP BY c.id, c.name
            ORDER BY product_count DESC, c.name ASC
            """
        ).fetchall()

    return {
        "receipt_count": sales_summary["receipt_count"],
        "revenue": sales_summary["revenue"],
        "average_ticket": sales_summary["average_ticket"],
        "item_count": inventory_summary["item_count"],
        "units_in_stock": inventory_summary["units_in_stock"],
        "low_stock_count": inventory_summary["low_stock_count"],
        "out_of_stock_count": inventory_summary["out_of_stock_count"],
        "inventory_cost_value": inventory_summary["inventory_cost_value"],
        "inventory_retail_value": inventory_summary["inventory_retail_value"],
        "upcoming_count": inventory_summary["upcoming_count"],
        "recent_sales": recent_sales,
        "inventory": inventory,
        "category_mix": category_mix,
    }


def build_period_report_rows(
    connection: sqlite3.Connection,
    sales_group_sql: str,
    movement_group_sql: str,
    limit: int,
) -> list[dict[str, object]]:
    sales_rows = connection.execute(
        f"""
        SELECT
            {sales_group_sql} AS period_key,
            COUNT(DISTINCT s.id) AS receipt_count,
            COALESCE(SUM(si.quantity), 0) AS units_sold,
            COALESCE(SUM(si.line_total), 0) AS sales_value,
            COALESCE(SUM(si.quantity * p.cost), 0) AS cogs
        FROM sale_items si
        JOIN sales s ON s.id = si.sale_id
        JOIN products p ON p.id = si.product_id
        WHERE s.status = 'completed'
        GROUP BY period_key
        ORDER BY period_key DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    movement_rows = connection.execute(
        f"""
        SELECT
            {movement_group_sql} AS period_key,
            COALESCE(SUM(CASE WHEN reason IN ('restock', 'opening_balance', 'purchase_receive', 'refund', 'void') AND quantity_change > 0 THEN quantity_change ELSE 0 END), 0) AS stock_in,
            COALESCE(SUM(CASE WHEN reason = 'manual_count' AND quantity_change > 0 THEN quantity_change ELSE 0 END), 0) AS count_gain,
            COALESCE(SUM(CASE WHEN reason = 'manual_count' AND quantity_change < 0 THEN ABS(quantity_change) ELSE 0 END), 0) AS count_loss,
            COALESCE(SUM(CASE WHEN reason IN ('damaged', 'wastage') AND quantity_change < 0 THEN ABS(quantity_change) ELSE 0 END), 0) AS write_off_units
        FROM stock_movements
        GROUP BY period_key
        ORDER BY period_key DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    period_map: dict[str, dict[str, object]] = {}
    for row in sales_rows:
        period_map[row["period_key"]] = {
            "period_key": row["period_key"],
            "receipt_count": row["receipt_count"],
            "units_sold": row["units_sold"],
            "sales_value": row["sales_value"],
            "cogs": row["cogs"],
            "stock_in": 0,
            "count_gain": 0,
            "count_loss": 0,
            "write_off_units": 0,
        }

    for row in movement_rows:
        bucket = period_map.setdefault(
            row["period_key"],
            {
                "period_key": row["period_key"],
                "receipt_count": 0,
                "units_sold": 0,
                "sales_value": 0,
                "cogs": 0,
                "stock_in": 0,
                "count_gain": 0,
                "count_loss": 0,
                "write_off_units": 0,
            },
        )
        bucket["stock_in"] = row["stock_in"]
        bucket["count_gain"] = row["count_gain"]
        bucket["count_loss"] = row["count_loss"]
        bucket["write_off_units"] = row["write_off_units"]

    rows = sorted(period_map.values(), key=lambda item: item["period_key"], reverse=True)[:limit]
    for row in rows:
        row["gross_profit"] = row["sales_value"] - row["cogs"]
        row["stock_out_total"] = row["units_sold"] + row["count_loss"] + row["write_off_units"]
        row["net_stock_movement"] = row["stock_in"] + row["count_gain"] - row["stock_out_total"]
    return rows


def build_current_period_summary(
    connection: sqlite3.Connection,
    sales_where_sql: str,
    movement_where_sql: str,
    label_sql: str,
    title: str,
) -> dict[str, object]:
    sales_row = connection.execute(
        f"""
        SELECT
            COUNT(DISTINCT s.id) AS receipt_count,
            COALESCE(SUM(si.quantity), 0) AS units_sold,
            COALESCE(SUM(si.line_total), 0) AS sales_value,
            COALESCE(SUM(si.quantity * p.cost), 0) AS cogs
        FROM sale_items si
        JOIN sales s ON s.id = si.sale_id
        JOIN products p ON p.id = si.product_id
        WHERE s.status = 'completed' AND {sales_where_sql}
        """
    ).fetchone()
    movement_row = connection.execute(
        f"""
        SELECT
            COALESCE(SUM(CASE WHEN reason IN ('restock', 'opening_balance', 'purchase_receive', 'refund', 'void') AND quantity_change > 0 THEN quantity_change ELSE 0 END), 0) AS stock_in,
            COALESCE(SUM(CASE WHEN reason = 'manual_count' AND quantity_change > 0 THEN quantity_change ELSE 0 END), 0) AS count_gain,
            COALESCE(SUM(CASE WHEN reason = 'manual_count' AND quantity_change < 0 THEN ABS(quantity_change) ELSE 0 END), 0) AS count_loss,
            COALESCE(SUM(CASE WHEN reason IN ('damaged', 'wastage') AND quantity_change < 0 THEN ABS(quantity_change) ELSE 0 END), 0) AS write_off_units
        FROM stock_movements
        WHERE {movement_where_sql}
        """
    ).fetchone()
    label = connection.execute(f"SELECT {label_sql} AS label").fetchone()["label"]

    sales_value = sales_row["sales_value"] or 0
    cogs = sales_row["cogs"] or 0
    units_sold = sales_row["units_sold"] or 0
    stock_in = movement_row["stock_in"] or 0
    count_gain = movement_row["count_gain"] or 0
    count_loss = movement_row["count_loss"] or 0
    write_off_units = movement_row["write_off_units"] or 0

    return {
        "title": title,
        "label": label,
        "receipt_count": sales_row["receipt_count"] or 0,
        "units_sold": units_sold,
        "sales_value": sales_value,
        "cogs": cogs,
        "gross_profit": sales_value - cogs,
        "stock_in": stock_in,
        "count_gain": count_gain,
        "count_loss": count_loss,
        "write_off_units": write_off_units,
        "stock_out_total": units_sold + count_loss + write_off_units,
        "net_stock_movement": stock_in + count_gain - (units_sold + count_loss + write_off_units),
    }


def fetch_reports_context() -> dict[str, object]:
    sales_date_bucket = sql_date_bucket("created_at")
    sales_month_bucket = sql_month_bucket("created_at")
    today_value_sql = sql_today()
    current_month_bucket_sql = sql_month_bucket(sql_now())
    sales_day_group_sql = sql_date_bucket("s.created_at")
    movement_day_group_sql = sql_date_bucket("created_at")
    sales_week_group_sql = sql_week_bucket("s.created_at")
    movement_week_group_sql = sql_week_bucket("created_at")
    sales_month_group_sql = sql_month_bucket("s.created_at")
    movement_month_group_sql = sql_month_bucket("created_at")
    sales_year_group_sql = sql_year_bucket("s.created_at")
    movement_year_group_sql = sql_year_bucket("created_at")

    with get_connection() as connection:
        report_summary = connection.execute(
            f"""
            SELECT
                COALESCE(SUM(CASE WHEN {sales_date_bucket} = {today_value_sql} THEN total ELSE 0 END), 0) AS today_revenue,
                COALESCE(SUM(CASE WHEN {sales_date_bucket} = {today_value_sql} THEN 1 ELSE 0 END), 0) AS today_receipts,
                COALESCE(SUM(CASE WHEN {sales_month_bucket} = {current_month_bucket_sql} THEN total ELSE 0 END), 0) AS month_revenue,
                COALESCE(SUM(CASE WHEN {sales_month_bucket} = {current_month_bucket_sql} THEN 1 ELSE 0 END), 0) AS month_receipts
            FROM sales
            WHERE status = 'completed'
            """
        ).fetchone()
        daily_sales = connection.execute(
            f"""
            SELECT
                {sales_date_bucket} AS sale_date,
                COUNT(*) AS receipt_count,
                COALESCE(SUM(total), 0) AS revenue,
                COALESCE(AVG(total), 0) AS average_ticket
            FROM sales
            WHERE status = 'completed'
            GROUP BY sale_date
            ORDER BY sale_date DESC
            LIMIT 14
            """
        ).fetchall()
        top_sellers = connection.execute(
            """
            SELECT
                p.name,
                p.sku,
                COALESCE(SUM(si.quantity), 0) AS quantity_sold,
                COALESCE(SUM(si.line_total), 0) AS sales_value
            FROM sale_items si
            JOIN products p ON p.id = si.product_id
            JOIN sales s ON s.id = si.sale_id
            WHERE s.status = 'completed'
            GROUP BY p.id, p.name, p.sku
            ORDER BY quantity_sold DESC, sales_value DESC, p.name ASC
            LIMIT 10
            """
        ).fetchall()

        daily_inventory = build_period_report_rows(
            connection,
            sales_day_group_sql,
            movement_day_group_sql,
            14,
        )
        weekly_inventory = build_period_report_rows(
            connection,
            sales_week_group_sql,
            movement_week_group_sql,
            12,
        )
        monthly_inventory = build_period_report_rows(
            connection,
            sales_month_group_sql,
            movement_month_group_sql,
            12,
        )
        yearly_inventory = build_period_report_rows(
            connection,
            sales_year_group_sql,
            movement_year_group_sql,
            6,
        )

        current_periods = [
            build_current_period_summary(
                connection,
                f"{sales_day_group_sql} = {today_value_sql}",
                f"{movement_day_group_sql} = {today_value_sql}",
                today_value_sql,
                "Daily",
            ),
            build_current_period_summary(
                connection,
                f"{sales_week_group_sql} = {sql_week_bucket(sql_now())}",
                f"{movement_week_group_sql} = {sql_week_bucket(sql_now())}",
                sql_week_bucket(sql_now()),
                "Weekly",
            ),
            build_current_period_summary(
                connection,
                f"{sales_month_group_sql} = {sql_month_bucket(sql_now())}",
                f"{movement_month_group_sql} = {sql_month_bucket(sql_now())}",
                sql_month_bucket(sql_now()),
                "Monthly",
            ),
            build_current_period_summary(
                connection,
                f"{sales_year_group_sql} = {sql_year_bucket(sql_now())}",
                f"{movement_year_group_sql} = {sql_year_bucket(sql_now())}",
                sql_year_bucket(sql_now()),
                "Yearly",
            ),
        ]

    return {
        "summary": report_summary,
        "daily_sales": daily_sales,
        "top_sellers": top_sellers,
        "current_periods": current_periods,
        "daily_inventory": daily_inventory,
        "weekly_inventory": weekly_inventory,
        "monthly_inventory": monthly_inventory,
        "yearly_inventory": yearly_inventory,
    }


def fetch_admin_context() -> dict[str, object]:
    products = fetch_admin_products()
    return {
        "metrics": fetch_dashboard_metrics(),
        "products": products,
        "variants_map": fetch_variants_by_product(),
        "all_variants": fetch_product_variants(),
        "categories": fetch_lookup_rows("categories"),
        "brands": fetch_lookup_rows("brands"),
        "units": fetch_lookup_rows("units"),
        "users": fetch_users(),
        "audit_logs": fetch_recent_audit_logs(12),
        "backups": fetch_backup_rows(),
        "settings": fetch_app_settings(),
    }


def fetch_today_shift() -> dict | None:
    from datetime import date as _date

    today = _date.today().isoformat()
    with get_connection() as connection:
        row = connection.execute(
            """SELECT dis.*, u1.full_name AS opened_by_name, u2.full_name AS closed_by_name
               FROM daily_inventory_shifts dis
               LEFT JOIN users u1 ON dis.opened_by = u1.id
               LEFT JOIN users u2 ON dis.closed_by = u2.id
               WHERE dis.shift_date = ?""",
            (today,),
        ).fetchone()
    return dict(row) if row else None


def fetch_recent_shifts(limit: int = 7) -> list[dict]:
    with get_connection() as connection:
        rows = connection.execute(
            """SELECT dis.*, u1.full_name AS opened_by_name, u2.full_name AS closed_by_name
               FROM daily_inventory_shifts dis
               LEFT JOIN users u1 ON dis.opened_by = u1.id
               LEFT JOIN users u2 ON dis.closed_by = u2.id
               ORDER BY dis.shift_date DESC LIMIT ?""",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def fetch_inventory_context() -> dict[str, object]:
    products = fetch_admin_products()
    active_products = [product for product in products if product["status"] == "active"]
    open_stock_count, stock_count_items = fetch_open_stock_count()
    today_shift = fetch_today_shift()
    recent_shifts = fetch_recent_shifts()
    return {
        "metrics": fetch_dashboard_metrics(),
        "reports": fetch_reports_context(),
        "active_products": active_products,
        "variants_map": fetch_variants_by_product(),
        "inventory_watchlist": fetch_inventory_watchlist(),
        "recent_movements": fetch_recent_stock_movements(),
        "suppliers": fetch_suppliers(),
        "purchase_orders": fetch_purchase_orders(),
        "open_stock_count": open_stock_count,
        "stock_count_items": stock_count_items,
        "sales_controls": fetch_sales_for_control(),
        "today_shift": today_shift,
        "recent_shifts": recent_shifts,
        "units": fetch_lookup_rows("units"),
    }


def fetch_owner_context() -> dict[str, object]:
    return {
        "metrics": fetch_dashboard_metrics(),
        "reports": fetch_reports_context(),
        "audit_logs": fetch_recent_audit_logs(20),
        "owner_alerts": fetch_owner_alerts(20),
        "backups": fetch_backup_rows(),
        "settings": fetch_app_settings(),
        "inventory_watchlist": fetch_inventory_watchlist(),
        "sales_controls": fetch_sales_for_control(12),
    }


def build_pos_categories(products: list[sqlite3.Row]) -> list[dict[str, object]]:
    categories: list[dict[str, object]] = []
    current_category_id: object = object()
    current_bucket: dict[str, object] | None = None

    for product in products:
        if product["category_id"] != current_category_id:
            current_category_id = product["category_id"]
            current_bucket = {
                "id": product["category_id"],
                "name": product["category_name"],
                "products": [],
            }
            categories.append(current_bucket)

        current_bucket["products"].append(product)

    return categories


@app.get("/healthz")
def healthcheck() -> tuple[dict[str, object], int]:
    return {
        "status": "ok",
        "app_env": APP_ENV,
        "database_engine": "sqlite",
        "remote_ready": True,
    }, 200


@app.route("/login", methods=["GET", "POST"])
def login() -> str:
    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        pin = request.form.get("pin", "").strip()
        next_url = request.form.get("next", "").strip()

        with get_connection() as connection:
            user = connection.execute(
                "SELECT id, full_name, username, role, pin_hash, is_active FROM users WHERE username = ?",
                (username,),
            ).fetchone()
            if user and user["is_active"] and check_password_hash(user["pin_hash"], pin):
                session["user_id"] = user["id"]
                connection.execute(
                    "UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = ?",
                    (user["id"],),
                )
                log_audit(connection, "login", "user", user["id"], f"{user['username']} signed in.")
                return redirect(next_url or url_for("pos"))

        flash("Invalid username or PIN.", "error")

    return render_template("login.html", next=request.args.get("next") or request.form.get("next", ""))


@app.route("/logout")
def logout():
    user = get_current_user()
    if user is not None:
        with get_connection() as connection:
            log_audit(connection, "logout", "user", user["id"], f"{user['username']} signed out.")
    session.clear()
    flash("Signed out.", "success")
    return redirect(url_for("login"))


@app.route("/")
@login_required("owner", "admin", "cashier")
def pos() -> str:
    products = fetch_pos_products()
    variants_map = fetch_variants_by_product()
    highlighted_total = sum(product["price"] for product in products[:3])
    low_stock_count = sum(1 for product in products if product["stock"] <= product["reorder_level"])
    return render_template(
        "pos.html",
        products=products,
        product_categories=build_pos_categories(products),
        variants_map=variants_map,
        highlighted_total=highlighted_total,
        low_stock_count=low_stock_count,
        upcoming_products=fetch_upcoming_products(),
        settings=fetch_app_settings(),
    )


@app.route("/checkout", methods=["POST"])
@login_required("owner", "admin", "cashier")
def checkout():
    product_ids = request.form.getlist("product_id")
    variant_ids = request.form.getlist("variant_id")
    quantities = request.form.getlist("quantity")
    payment_method = request.form.get("payment_method", "Card")
    discount_type = request.form.get("discount_type", "none").strip().lower()
    discount_note = request.form.get("discount_note", "").strip()
    raw_custom_discount_rate = request.form.get("custom_discount_rate", "0").strip()

    cart: list[dict[str, object]] = []
    with get_connection() as connection:
        for idx, (raw_product_id, raw_quantity) in enumerate(zip(product_ids, quantities)):
            quantity = int(raw_quantity or 0)
            if quantity <= 0:
                continue

            raw_variant_id = variant_ids[idx] if idx < len(variant_ids) else ""
            variant_id = int(raw_variant_id) if raw_variant_id else None

            product = connection.execute(
                """
                SELECT id, name, sku, price, stock, status
                FROM products
                WHERE id = ?
                """,
                (raw_product_id,),
            ).fetchone()

            if product is None or product["status"] != "active":
                continue

            if variant_id:
                variant = connection.execute(
                    "SELECT id, name, sku_suffix, price, stock FROM product_variants WHERE id = ? AND product_id = ? AND is_active = 1",
                    (variant_id, product["id"]),
                ).fetchone()
                if variant is None:
                    continue
                effective_price = variant["price"]
                effective_stock = variant["stock"]
                display_name = f"{product['name']} ({variant['name']})"
                effective_sku = f"{product['sku']}{variant['sku_suffix']}" if variant["sku_suffix"] else product["sku"]
            else:
                effective_price = product["price"]
                effective_stock = product["stock"]
                display_name = product["name"]
                effective_sku = product["sku"]

            if quantity > effective_stock:
                flash(f"Only {effective_stock} units of {display_name} are available.", "error")
                return redirect(url_for("pos"))

            line_total = round(effective_price * quantity, 2)
            cart.append(
                {
                    "id": product["id"],
                    "variant_id": variant_id,
                    "name": display_name,
                    "sku": effective_sku,
                    "quantity": quantity,
                    "unit_price": effective_price,
                    "line_total": line_total,
                }
            )

        if not cart:
            flash("Choose at least one product before checkout.", "error")
            return redirect(url_for("pos"))

        subtotal = round(sum(item["line_total"] for item in cart), 2)
        if discount_type not in DISCOUNT_PRESETS:
            flash("Choose a valid discount type.", "error")
            return redirect(url_for("pos"))

        if discount_type == "custom":
            try:
                discount_rate = round(float(raw_custom_discount_rate) / 100, 4)
            except ValueError:
                flash("Custom discount must be a valid percentage.", "error")
                return redirect(url_for("pos"))
        else:
            discount_rate = DISCOUNT_PRESETS[discount_type] or 0.0

        if discount_rate < 0 or discount_rate > 1:
            flash("Discount must be between 0% and 100%.", "error")
            return redirect(url_for("pos"))

        discount_amount = round(subtotal * discount_rate, 2)
        discounted_subtotal = round(subtotal - discount_amount, 2)
        tax = round(discounted_subtotal * VAT_RATE, 2)
        total = round(discounted_subtotal + tax, 2)

        sale_row = connection.execute(
            """
            INSERT INTO sales (
                subtotal, discount_type, discount_rate, discount_amount, discount_note,
                tax, total, payment_method, status, cashier_user_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'completed', ?)
            RETURNING id
            """,
            (
                subtotal,
                discount_type,
                discount_rate,
                discount_amount,
                discount_note,
                tax,
                total,
                payment_method,
                session_user_id(),
            ),
        ).fetchone()
        sale_id = sale_row["id"]

        connection.executemany(
            """
            INSERT INTO sale_items (sale_id, product_id, variant_id, quantity, unit_price, line_total)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (sale_id, item["id"], item["variant_id"], item["quantity"], item["unit_price"], item["line_total"])
                for item in cart
            ],
        )
        for item in cart:
            if item["variant_id"]:
                connection.execute(
                    "UPDATE product_variants SET stock = stock - ? WHERE id = ?",
                    (item["quantity"], item["variant_id"]),
                )
            else:
                connection.execute(
                    "UPDATE products SET stock = stock - ? WHERE id = ?",
                    (item["quantity"], item["id"]),
                )
            log_stock_movement(connection, item["id"], -item["quantity"], "sale", item["variant_id"])
            maybe_create_low_stock_alert(connection, item["id"], "checkout")
        log_audit(
            connection,
            "checkout",
            "sale",
            sale_id,
            f"Sale completed via {payment_method}; discount={discount_type}; total={total:.2f}",
        )

    return redirect(url_for("receipt", sale_id=sale_id))


@app.route("/receipt/<int:sale_id>")
@login_required("owner", "admin", "cashier")
def receipt(sale_id: int) -> str:
    with get_connection() as connection:
        sale = connection.execute(
            "SELECT id, created_at, subtotal, discount_type, discount_rate, discount_amount, discount_note, tax, total, payment_method, status FROM sales WHERE id = ?",
            (sale_id,),
        ).fetchone()
        items = connection.execute(
            """
            SELECT p.name, p.sku, si.quantity, si.unit_price, si.line_total,
                   pv.name AS variant_name, pv.sku_suffix AS variant_sku_suffix
            FROM sale_items si
            JOIN products p ON p.id = si.product_id
            LEFT JOIN product_variants pv ON pv.id = si.variant_id
            WHERE si.sale_id = ?
            ORDER BY si.id ASC
            """,
            (sale_id,),
        ).fetchall()

    if sale is None:
        flash("Receipt not found.", "error")
        return redirect(url_for("pos"))

    return render_template("receipt.html", sale=sale, items=items, settings=fetch_app_settings())


@app.route("/admin")
@login_required("owner", "admin")
def admin_dashboard() -> str:
    return render_template("admin_dashboard.html", **fetch_admin_context())


@app.route("/inventory")
@login_required("owner", "admin")
def inventory_dashboard() -> str:
    return render_template("inventory_dashboard.html", **fetch_inventory_context())


@app.route("/owner")
@login_required("owner")
def owner_dashboard() -> str:
    return render_template("owner_dashboard.html", **fetch_owner_context())


@app.route("/admin/categories/add", methods=["POST"])
@login_required("owner", "admin")
def add_category():
    name = normalize_lookup_name(request.form.get("name", ""))
    if not name:
        flash("Category name is required.", "error")
        return redirect_to_admin("categories")

    try:
        with get_connection() as connection:
            next_sort_order = connection.execute(
                "SELECT COALESCE(MAX(sort_order), 0) + 1 AS next_value FROM categories"
            ).fetchone()["next_value"]
            category_row = connection.execute(
                "INSERT INTO categories (name, sort_order) VALUES (?, ?) RETURNING id",
                (name, next_sort_order),
            ).fetchone()
            log_audit(connection, "create", "category", category_row["id"], f"Category created: {name}")
        flash("Category added.", "success")
    except DBIntegrityError:
        flash("That category already exists.", "error")
    return redirect_to_admin("categories")


@app.route("/admin/categories/move", methods=["POST"])
@login_required("owner", "admin")
def move_category():
    category_id = request.form.get("category_id", "").strip()
    move_to = request.form.get("move_to", "").strip()
    if not category_id or not move_to:
        flash("Select a category and a destination number.", "error")
        return redirect_to_admin("categories")

    try:
        category_id_int = int(category_id)
        move_to_int = int(move_to)
    except ValueError:
        flash("Category move position must be a valid whole number.", "error")
        return redirect_to_admin("categories")

    if move_to_int <= 0:
        flash("Category move position must be 1 or greater.", "error")
        return redirect_to_admin("categories")

    with get_connection() as connection:
        category = connection.execute(
            "SELECT id, name FROM categories WHERE id = ?",
            (category_id_int,),
        ).fetchone()
        if category is None:
            flash("Category not found.", "error")
            return redirect_to_admin("categories")

        final_position = move_category_to_position(connection, category_id_int, move_to_int)
        log_audit(connection, "move", "category", category_id_int, f"Moved category to no. {final_position}")

    flash(f"{category['name']} moved to no. {final_position}.", "success")
    return redirect_to_admin("categories")


@app.route("/admin/brands/add", methods=["POST"])
@login_required("owner", "admin")
def add_brand():
    name = normalize_lookup_name(request.form.get("name", ""))
    if not name:
        flash("Brand name is required.", "error")
        return redirect_to_admin("brands")

    try:
        with get_connection() as connection:
            brand_row = connection.execute("INSERT INTO brands (name) VALUES (?) RETURNING id", (name,)).fetchone()
            log_audit(connection, "create", "brand", brand_row["id"], f"Brand created: {name}")
        flash("Brand added.", "success")
    except DBIntegrityError:
        flash("That brand already exists.", "error")
    return redirect_to_admin("brands")


@app.route("/admin/units/add", methods=["POST"])
@login_required("owner", "admin")
def add_unit():
    name = normalize_lookup_name(request.form.get("name", ""))
    if not name:
        flash("Unit name is required.", "error")
        return redirect_to_admin("units")

    try:
        with get_connection() as connection:
            unit_row = connection.execute("INSERT INTO units (name) VALUES (?) RETURNING id", (name,)).fetchone()
            log_audit(connection, "create", "unit", unit_row["id"], f"Unit created: {name}")
        flash("Unit added.", "success")
    except DBIntegrityError:
        flash("That unit already exists.", "error")
    return redirect_to_admin("units")


@app.route("/admin/products/add", methods=["POST"])
@login_required("owner", "admin")
def add_product():
    name = normalize_lookup_name(request.form.get("name", ""))
    raw_price = request.form.get("price", "").strip()
    raw_cost = request.form.get("cost", "").strip()
    raw_stock = request.form.get("stock", "").strip()
    raw_reorder_level = request.form.get("reorder_level", "").strip()
    category_id = request.form.get("category_id", "").strip()
    brand_id = request.form.get("brand_id", "").strip()
    unit_id = request.form.get("unit_id", "").strip()
    status = request.form.get("status", "active").strip().lower()
    image_file = request.files.get("image")

    if not all([name, raw_price, raw_cost, raw_stock, raw_reorder_level, category_id, brand_id, unit_id]):
        flash("Complete all product fields before saving.", "error")
        return redirect_to_admin("products")

    try:
        price = round(float(raw_price), 2)
        cost = round(float(raw_cost), 2)
        stock = int(raw_stock)
        reorder_level = int(raw_reorder_level)
        category_id_int = int(category_id)
        brand_id_int = int(brand_id)
        unit_id_int = int(unit_id)
    except ValueError:
        flash("Price, cost, stock, and reorder level must be valid values.", "error")
        return redirect_to_admin("products")

    if status not in ALLOWED_PRODUCT_STATUSES:
        flash("Choose a valid product status.", "error")
        return redirect_to_admin("products")
    if price < 0 or cost < 0 or stock < 0 or reorder_level < 0:
        flash("Price, cost, stock, and reorder level must be zero or greater.", "error")
        return redirect_to_admin("products")

    with get_connection() as connection:
        lookup_counts = connection.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM categories WHERE id = ?) AS category_match,
                (SELECT COUNT(*) FROM brands WHERE id = ?) AS brand_match,
                (SELECT COUNT(*) FROM units WHERE id = ?) AS unit_match
            """,
            (category_id_int, brand_id_int, unit_id_int),
        ).fetchone()

        if not all(lookup_counts[key] == 1 for key in lookup_counts.keys()):
            flash("Choose valid category, brand, and unit values.", "error")
            return redirect_to_admin("products")

        category_name = connection.execute(
            "SELECT name FROM categories WHERE id = ?",
            (category_id_int,),
        ).fetchone()["name"]
        next_sort_order = connection.execute(
            "SELECT COALESCE(MAX(sort_order), 0) + 1 AS next_value FROM products"
        ).fetchone()["next_value"]
        sku = generate_sku(connection, name, category_name)
        saved_image_path = save_product_image(image_file)
        product_row = connection.execute(
            """
            INSERT INTO products (
                name, sku, price, cost, stock, reorder_level,
                category_id, brand_id, unit_id, status, last_restocked, sort_order, image_path
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CASE WHEN ? > 0 THEN CURRENT_TIMESTAMP ELSE NULL END, ?, ?)
            RETURNING id
            """,
            (
                name,
                sku,
                price,
                cost,
                stock,
                reorder_level,
                category_id_int,
                brand_id_int,
                unit_id_int,
                status,
                stock,
                next_sort_order,
                saved_image_path,
            ),
        ).fetchone()
        log_stock_movement(connection, product_row["id"], stock, "opening_balance")
        log_audit(connection, "create", "product", product_row["id"], f"Product created: {name}")

    flash("Product added.", "success")
    return redirect_to_admin("products")


@app.route("/admin/products/update", methods=["POST"])
@login_required("owner", "admin")
def update_product():
    product_id = request.form.get("product_id", "").strip()
    name = normalize_lookup_name(request.form.get("name", ""))
    raw_price = request.form.get("price", "").strip()
    raw_cost = request.form.get("cost", "").strip()
    raw_reorder_level = request.form.get("reorder_level", "").strip()
    category_id = request.form.get("category_id", "").strip()
    brand_id = request.form.get("brand_id", "").strip()
    unit_id = request.form.get("unit_id", "").strip()
    status = request.form.get("status", "active").strip().lower()
    image_file = request.files.get("image")

    if not all([product_id, name, raw_price, raw_cost, raw_reorder_level, category_id, brand_id, unit_id]):
        flash("Complete all product fields before updating.", "error")
        return redirect_to_admin("products")

    try:
        product_id_int = int(product_id)
        price = round(float(raw_price), 2)
        cost = round(float(raw_cost), 2)
        reorder_level = int(raw_reorder_level)
        category_id_int = int(category_id)
        brand_id_int = int(brand_id)
        unit_id_int = int(unit_id)
    except ValueError:
        flash("Product update fields contain invalid values.", "error")
        return redirect_to_admin("products")

    if status not in ALLOWED_PRODUCT_STATUSES:
        flash("Choose a valid product status.", "error")
        return redirect_to_admin("products")
    if price < 0 or cost < 0 or reorder_level < 0:
        flash("Price, cost, and reorder level must be zero or greater.", "error")
        return redirect_to_admin("products")

    with get_connection() as connection:
        product = connection.execute(
            "SELECT id FROM products WHERE id = ?",
            (product_id_int,),
        ).fetchone()
        if product is None:
            flash("Product not found.", "error")
            return redirect_to_admin("products")

        lookup_counts = connection.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM categories WHERE id = ?) AS category_match,
                (SELECT COUNT(*) FROM brands WHERE id = ?) AS brand_match,
                (SELECT COUNT(*) FROM units WHERE id = ?) AS unit_match
            """,
            (category_id_int, brand_id_int, unit_id_int),
        ).fetchone()
        if not all(lookup_counts[key] == 1 for key in lookup_counts.keys()):
            flash("Choose valid category, brand, and unit values.", "error")
            return redirect_to_admin("products")

        saved_image_path = save_product_image(image_file)
        if saved_image_path:
            connection.execute(
                """
                UPDATE products
                SET name = ?,
                    price = ?,
                    cost = ?,
                    reorder_level = ?,
                    category_id = ?,
                    brand_id = ?,
                    unit_id = ?,
                    status = ?,
                    image_path = ?
                WHERE id = ?
                """,
                (
                    name,
                    price,
                    cost,
                    reorder_level,
                    category_id_int,
                    brand_id_int,
                    unit_id_int,
                    status,
                    saved_image_path,
                    product_id_int,
                ),
            )
        else:
            connection.execute(
                """
                UPDATE products
                SET name = ?,
                    price = ?,
                    cost = ?,
                    reorder_level = ?,
                    category_id = ?,
                    brand_id = ?,
                    unit_id = ?,
                    status = ?
                WHERE id = ?
                """,
                (
                    name,
                    price,
                    cost,
                    reorder_level,
                    category_id_int,
                    brand_id_int,
                    unit_id_int,
                    status,
                    product_id_int,
                ),
            )
        log_audit(connection, "update", "product", product_id_int, f"Product updated: {name}")

    flash("Product updated.", "success")
    return redirect_to_admin("products")


@app.route("/admin/products/move", methods=["POST"])
@login_required("owner", "admin")
def move_product():
    product_id = request.form.get("product_id", "").strip()
    move_to = request.form.get("move_to", "").strip()
    if not product_id or not move_to:
        flash("Select a product and a destination number.", "error")
        return redirect_to_admin("products")

    try:
        product_id_int = int(product_id)
        move_to_int = int(move_to)
    except ValueError:
        flash("Move position must be a valid whole number.", "error")
        return redirect_to_admin("products")

    if move_to_int <= 0:
        flash("Move position must be 1 or greater.", "error")
        return redirect_to_admin("products")

    with get_connection() as connection:
        product = connection.execute(
            "SELECT id, name FROM products WHERE id = ?",
            (product_id_int,),
        ).fetchone()
        if product is None:
            flash("Product not found.", "error")
            return redirect_to_admin("products")

        final_position = move_product_to_position(connection, product_id_int, move_to_int)
        log_audit(connection, "move", "product", product_id_int, f"Moved product to no. {final_position}")

    flash(f"{product['name']} moved to no. {final_position}.", "success")
    return redirect_to_admin("products")


@app.route("/admin/products/remove", methods=["POST"])
@login_required("owner", "admin")
def remove_product():
    product_id = request.form.get("product_id", "").strip()
    if not product_id:
        flash("Select a product to remove.", "error")
        return redirect_to_admin("products")

    try:
        product_id_int = int(product_id)
    except ValueError:
        flash("Invalid product selection.", "error")
        return redirect_to_admin("products")

    with get_connection() as connection:
        product = connection.execute(
            "SELECT id, name, stock, status FROM products WHERE id = ?",
            (product_id_int,),
        ).fetchone()
        if product is None:
            flash("Product not found.", "error")
            return redirect_to_admin("products")

        sales_count = connection.execute(
            "SELECT COUNT(*) AS count FROM sale_items WHERE product_id = ?",
            (product_id_int,),
        ).fetchone()["count"]

        if product["stock"] > 0 or sales_count > 0:
            connection.execute(
                "UPDATE products SET status = 'inactive' WHERE id = ?",
                (product_id_int,),
            )
            log_audit(connection, "archive", "product", product_id_int, "Product archived instead of deleted")
            if product["stock"] > 0:
                flash(
                    "Product archived and removed from cashier view. Existing stock remains recorded until you adjust it.",
                    "success",
                )
            else:
                flash("Product archived instead of deleted because it has sales history.", "success")
            return redirect_to_admin("products")

        connection.execute("DELETE FROM stock_movements WHERE product_id = ?", (product_id_int,))
        connection.execute("DELETE FROM products WHERE id = ?", (product_id_int,))
    log_audit(connection, "delete", "product", product_id_int, "Product permanently deleted")

    flash("Product removed.", "success")
    return redirect_to_admin("products")


@app.route("/admin/variants/add", methods=["POST"])
@login_required("owner", "admin")
def add_variant():
    product_id = request.form.get("product_id", "").strip()
    name = normalize_lookup_name(request.form.get("variant_name", ""))
    raw_price = request.form.get("variant_price", "").strip()
    raw_cost = request.form.get("variant_cost", "").strip()
    raw_stock = request.form.get("variant_stock", "").strip()
    raw_reorder = request.form.get("variant_reorder_level", "").strip()
    sku_suffix = request.form.get("variant_sku_suffix", "").strip()

    if not all([product_id, name, raw_price, raw_cost, raw_stock, raw_reorder]):
        flash("Complete all variant fields before saving.", "error")
        return redirect_to_admin("products")

    try:
        product_id_int = int(product_id)
        price = round(float(raw_price), 2)
        cost = round(float(raw_cost), 2)
        stock = int(raw_stock)
        reorder_level = int(raw_reorder)
    except ValueError:
        flash("Variant fields contain invalid values.", "error")
        return redirect_to_admin("products")

    if price < 0 or cost < 0 or stock < 0 or reorder_level < 0:
        flash("Variant price, cost, stock, and reorder level must be zero or greater.", "error")
        return redirect_to_admin("products")

    with get_connection() as connection:
        product = connection.execute("SELECT id, name FROM products WHERE id = ?", (product_id_int,)).fetchone()
        if product is None:
            flash("Product not found.", "error")
            return redirect_to_admin("products")

        next_sort = connection.execute(
            "SELECT COALESCE(MAX(sort_order), 0) + 1 AS next_value FROM product_variants WHERE product_id = ?",
            (product_id_int,),
        ).fetchone()["next_value"]

        variant_row = connection.execute(
            """
            INSERT INTO product_variants (product_id, name, sku_suffix, price, cost, stock, reorder_level, sort_order)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING id
            """,
            (product_id_int, name, sku_suffix, price, cost, stock, reorder_level, next_sort),
        ).fetchone()
        log_stock_movement(connection, product_id_int, stock, "opening_balance", variant_row["id"])
        log_audit(connection, "create", "variant", variant_row["id"], f"Variant '{name}' added to {product['name']}")

    flash(f"Variant '{name}' added.", "success")
    return redirect_to_admin("products")


@app.route("/admin/variants/update", methods=["POST"])
@login_required("owner", "admin")
def update_variant():
    variant_id = request.form.get("variant_id", "").strip()
    name = normalize_lookup_name(request.form.get("variant_name", ""))
    raw_price = request.form.get("variant_price", "").strip()
    raw_cost = request.form.get("variant_cost", "").strip()
    raw_reorder = request.form.get("variant_reorder_level", "").strip()
    sku_suffix = request.form.get("variant_sku_suffix", "").strip()
    is_active = request.form.get("variant_is_active", "1").strip()

    if not all([variant_id, name, raw_price, raw_cost, raw_reorder]):
        flash("Complete all variant fields before updating.", "error")
        return redirect_to_admin("products")

    try:
        variant_id_int = int(variant_id)
        price = round(float(raw_price), 2)
        cost = round(float(raw_cost), 2)
        reorder_level = int(raw_reorder)
        is_active_int = int(is_active)
    except ValueError:
        flash("Variant fields contain invalid values.", "error")
        return redirect_to_admin("products")

    if price < 0 or cost < 0 or reorder_level < 0:
        flash("Variant price, cost, and reorder level must be zero or greater.", "error")
        return redirect_to_admin("products")

    with get_connection() as connection:
        variant = connection.execute("SELECT id, product_id FROM product_variants WHERE id = ?", (variant_id_int,)).fetchone()
        if variant is None:
            flash("Variant not found.", "error")
            return redirect_to_admin("products")

        connection.execute(
            """
            UPDATE product_variants
            SET name = ?, sku_suffix = ?, price = ?, cost = ?, reorder_level = ?, is_active = ?
            WHERE id = ?
            """,
            (name, sku_suffix, price, cost, reorder_level, is_active_int, variant_id_int),
        )
        log_audit(connection, "update", "variant", variant_id_int, f"Variant updated: {name}")

    flash("Variant updated.", "success")
    return redirect_to_admin("products")


@app.route("/admin/variants/remove", methods=["POST"])
@login_required("owner", "admin")
def remove_variant():
    variant_id = request.form.get("variant_id", "").strip()
    if not variant_id:
        flash("Select a variant to remove.", "error")
        return redirect_to_admin("products")

    try:
        variant_id_int = int(variant_id)
    except ValueError:
        flash("Invalid variant selection.", "error")
        return redirect_to_admin("products")

    with get_connection() as connection:
        variant = connection.execute(
            "SELECT pv.id, pv.name, pv.product_id, pv.stock FROM product_variants pv WHERE pv.id = ?",
            (variant_id_int,),
        ).fetchone()
        if variant is None:
            flash("Variant not found.", "error")
            return redirect_to_admin("products")

        sales_count = connection.execute(
            "SELECT COUNT(*) AS count FROM sale_items WHERE variant_id = ?",
            (variant_id_int,),
        ).fetchone()["count"]

        if variant["stock"] > 0 or sales_count > 0:
            connection.execute("UPDATE product_variants SET is_active = 0 WHERE id = ?", (variant_id_int,))
            log_audit(connection, "archive", "variant", variant_id_int, f"Variant '{variant['name']}' archived")
            flash("Variant archived instead of deleted because it has stock or sales history.", "success")
        else:
            connection.execute("DELETE FROM stock_movements WHERE variant_id = ?", (variant_id_int,))
            connection.execute("DELETE FROM product_variants WHERE id = ?", (variant_id_int,))
            log_audit(connection, "delete", "variant", variant_id_int, f"Variant '{variant['name']}' permanently deleted")
            flash("Variant removed.", "success")

    return redirect_to_admin("products")


@app.route("/admin/inventory/open-day", methods=["POST"])
@login_required("owner", "admin")
def open_inventory_day():
    from datetime import date as _date

    today = _date.today().isoformat()
    with get_connection() as connection:
        existing = connection.execute(
            "SELECT id, status FROM daily_inventory_shifts WHERE shift_date = ?",
            (today,),
        ).fetchone()
        if existing:
            flash(f"Today's inventory shift is already {'open' if existing['status'] == 'open' else 'closed'}.", "error")
            return redirect_to_inventory("stock-control")

        total_units = connection.execute("SELECT COALESCE(SUM(stock), 0) AS total FROM products WHERE status = 'active'").fetchone()["total"]
        shift = connection.execute(
            "INSERT INTO daily_inventory_shifts (shift_date, status, opened_by, opening_units) VALUES (?, 'open', ?, ?) RETURNING id",
            (today, session.get("user_id"), total_units),
        ).fetchone()
        shift_id = shift["id"]

        products = connection.execute("SELECT id, stock FROM products WHERE status = 'active'").fetchall()
        for p in products:
            connection.execute(
                "INSERT INTO daily_shift_snapshots (shift_id, product_id, opening_stock) VALUES (?, ?, ?)",
                (shift_id, p["id"], p["stock"]),
            )

        log_audit(connection, "create", "daily_shift", shift_id, f"Opened inventory day for {today} with {total_units} units")

    flash(f"Inventory day opened for {today} with {total_units} total units.", "success")
    return redirect_to_inventory("stock-control")


@app.route("/admin/inventory/close-day", methods=["POST"])
@login_required("owner", "admin")
def close_inventory_day():
    shift_id_raw = request.form.get("shift_id", "").strip()
    notes = request.form.get("closing_notes", "").strip()[:300]

    try:
        shift_id_int = int(shift_id_raw)
    except (ValueError, TypeError):
        flash("Invalid shift.", "error")
        return redirect_to_inventory("stock-control")

    with get_connection() as connection:
        shift = connection.execute(
            "SELECT id, shift_date, opening_units FROM daily_inventory_shifts WHERE id = ? AND status = 'open'",
            (shift_id_int,),
        ).fetchone()
        if shift is None:
            flash("No open shift found to close.", "error")
            return redirect_to_inventory("stock-control")

        closing_units = connection.execute("SELECT COALESCE(SUM(stock), 0) AS total FROM products WHERE status = 'active'").fetchone()["total"]

        products = connection.execute("SELECT id, stock FROM products WHERE status = 'active'").fetchall()
        for p in products:
            connection.execute(
                "UPDATE daily_shift_snapshots SET closing_stock = ? WHERE shift_id = ? AND product_id = ?",
                (p["stock"], shift_id_int, p["id"]),
            )

        units_sold = connection.execute(
            "SELECT COALESCE(SUM(si.quantity), 0) AS total FROM sale_items si JOIN sales s ON si.sale_id = s.id WHERE DATE(s.created_at) = ? AND s.status = 'completed'",
            (shift["shift_date"],),
        ).fetchone()["total"]

        units_received = connection.execute(
            "SELECT COALESCE(SUM(quantity_change), 0) AS total FROM stock_movements WHERE DATE(created_at) = ? AND quantity_change > 0 AND reason != 'sale'",
            (shift["shift_date"],),
        ).fetchone()["total"]

        units_adjusted = connection.execute(
            "SELECT COALESCE(SUM(quantity_change), 0) AS total FROM stock_movements WHERE DATE(created_at) = ? AND quantity_change < 0 AND reason != 'sale'",
            (shift["shift_date"],),
        ).fetchone()["total"]

        connection.execute(
            """UPDATE daily_inventory_shifts
               SET status = 'closed', closed_at = CURRENT_TIMESTAMP, closed_by = ?,
                   closing_units = ?, units_sold = ?, units_received = ?, units_adjusted = ?, notes = ?
               WHERE id = ?""",
            (session.get("user_id"), closing_units, units_sold, units_received, units_adjusted, notes, shift_id_int),
        )

        log_audit(connection, "update", "daily_shift", shift_id_int,
                  f"Closed inventory day for {shift['shift_date']}: opening={shift['opening_units']}, closing={closing_units}, sold={units_sold}")

    flash(f"Inventory day closed. Opening: {shift['opening_units']} → Closing: {closing_units} units.", "success")
    return redirect_to_inventory("stock-control")


@app.route("/admin/inventory/adjust", methods=["POST"])
@login_required("owner", "admin")
def adjust_inventory():
    product_id = request.form.get("product_id", "").strip()
    variant_id_raw = request.form.get("variant_id", "").strip()
    raw_quantity_change = request.form.get("quantity_change", "").strip()
    reason = request.form.get("reason", "manual_count").strip().lower()

    if not product_id or not raw_quantity_change:
        flash("Select a product and quantity adjustment.", "error")
        return redirect_to_inventory("stock-control")

    try:
        product_id_int = int(product_id)
        quantity_change = int(raw_quantity_change)
        variant_id_int = int(variant_id_raw) if variant_id_raw else None
    except ValueError:
        flash("Inventory adjustments must use whole numbers.", "error")
        return redirect_to_inventory("stock-control")

    if quantity_change == 0:
        flash("Adjustment quantity cannot be zero.", "error")
        return redirect_to_inventory("stock-control")
    if reason not in ALLOWED_INVENTORY_REASONS - {"sale"}:
        flash("Choose a valid inventory reason.", "error")
        return redirect_to_inventory("stock-control")

    with get_connection() as connection:
        product = connection.execute(
            "SELECT id, name, stock, reorder_level, status FROM products WHERE id = ?",
            (product_id_int,),
        ).fetchone()
        if product is None:
            flash("Product not found.", "error")
            return redirect_to_inventory("stock-control")

        if variant_id_int:
            variant = connection.execute(
                "SELECT id, name, stock FROM product_variants WHERE id = ? AND product_id = ?",
                (variant_id_int, product_id_int),
            ).fetchone()
            if variant is None:
                flash("Variant not found.", "error")
                return redirect_to_inventory("stock-control")
            updated_stock = variant["stock"] + quantity_change
            if updated_stock < 0:
                flash("Adjustment would result in negative stock.", "error")
                return redirect_to_inventory("stock-control")
            connection.execute("UPDATE product_variants SET stock = ? WHERE id = ?", (updated_stock, variant_id_int))
            log_stock_movement(connection, product_id_int, quantity_change, reason, variant_id_int)
            adjust_label = f"{product['name']} ({variant['name']})"
        else:
            updated_stock = product["stock"] + quantity_change
            if updated_stock < 0:
                flash("Adjustment would result in negative stock.", "error")
                return redirect_to_inventory("stock-control")
            connection.execute(
                """
                UPDATE products
                SET stock = ?,
                    last_restocked = CASE WHEN ? > 0 THEN CURRENT_TIMESTAMP ELSE last_restocked END
                WHERE id = ?
                """,
                (updated_stock, quantity_change, product_id_int),
            )
            log_stock_movement(connection, product_id_int, quantity_change, reason)
            adjust_label = product["name"]

        maybe_create_adjustment_alert(connection, product, quantity_change, reason)
        maybe_create_low_stock_alert(connection, product_id_int, f"inventory adjustment ({reason})")
        log_audit(
            connection,
            "adjust",
            "inventory",
            product_id_int,
            f"Inventory adjusted by {quantity_change} for {adjust_label}, reason: {reason}",
        )

    flash("Inventory adjusted.", "success")
    return redirect_to_inventory("stock-control")


@app.route("/admin/users/add", methods=["POST"])
@login_required("owner")
def add_user():
    full_name = normalize_lookup_name(request.form.get("full_name", ""))
    username = request.form.get("username", "").strip().lower()
    role = request.form.get("role", "cashier").strip().lower()
    pin = request.form.get("pin", "").strip()

    if not all([full_name, username, role, pin]):
        flash("Complete all user fields.", "error")
        return redirect_to_admin("users")
    if role not in ALLOWED_USER_ROLES:
        flash("Choose a valid role.", "error")
        return redirect_to_admin("users")

    try:
        with get_connection() as connection:
            user_row = connection.execute(
                "INSERT INTO users (full_name, username, role, pin_hash) VALUES (?, ?, ?, ?) RETURNING id",
                (full_name, username, role, generate_password_hash(pin)),
            ).fetchone()
            log_audit(connection, "create", "user", user_row["id"], f"User created: {username}")
        flash("User added.", "success")
    except DBIntegrityError:
        flash("That username already exists.", "error")
    return redirect_to_admin("users")


@app.route("/inventory/suppliers/add", methods=["POST"])
@login_required("owner", "admin")
def add_supplier():
    name = normalize_lookup_name(request.form.get("name", ""))
    contact_person = normalize_lookup_name(request.form.get("contact_person", ""))
    phone = request.form.get("phone", "").strip()
    email = request.form.get("email", "").strip()
    notes = request.form.get("notes", "").strip()
    if not name:
        flash("Supplier name is required.", "error")
        return redirect_to_inventory("suppliers")

    try:
        with get_connection() as connection:
            supplier_row = connection.execute(
                """
                INSERT INTO suppliers (name, contact_person, phone, email, notes)
                VALUES (?, ?, ?, ?, ?)
                RETURNING id
                """,
                (name, contact_person, phone, email, notes),
            ).fetchone()
            log_audit(connection, "create", "supplier", supplier_row["id"], f"Supplier created: {name}")
        flash("Supplier added.", "success")
    except DBIntegrityError:
        flash("That supplier already exists.", "error")
    return redirect_to_inventory("suppliers")


@app.route("/inventory/purchase-orders/add", methods=["POST"])
@login_required("owner", "admin")
def add_purchase_order():
    supplier_id = request.form.get("supplier_id", "").strip()
    product_id = request.form.get("product_id", "").strip()
    ordered_quantity = request.form.get("ordered_quantity", "").strip()
    unit_cost = request.form.get("unit_cost", "").strip()
    notes = request.form.get("notes", "").strip()

    try:
        supplier_id_int = int(supplier_id)
        product_id_int = int(product_id)
        ordered_quantity_int = int(ordered_quantity)
        unit_cost_value = round(float(unit_cost), 2)
    except ValueError:
        flash("Purchase order values are invalid.", "error")
        return redirect_to_inventory("purchase-orders")

    if ordered_quantity_int <= 0 or unit_cost_value < 0:
        flash("Ordered quantity must be positive and unit cost cannot be negative.", "error")
        return redirect_to_inventory("purchase-orders")

    with get_connection() as connection:
        supplier = connection.execute("SELECT id, name FROM suppliers WHERE id = ? AND is_active = 1", (supplier_id_int,)).fetchone()
        product = connection.execute("SELECT id, name FROM products WHERE id = ?", (product_id_int,)).fetchone()
        if supplier is None or product is None:
            flash("Select a valid supplier and product.", "error")
            return redirect_to_inventory("purchase-orders")

        purchase_order_row = connection.execute(
            "INSERT INTO purchase_orders (supplier_id, notes, created_by) VALUES (?, ?, ?) RETURNING id",
            (supplier_id_int, notes, session_user_id()),
        ).fetchone()
        po_id = purchase_order_row["id"]
        connection.execute(
            "INSERT INTO purchase_order_items (purchase_order_id, product_id, ordered_quantity, unit_cost) VALUES (?, ?, ?, ?)",
            (po_id, product_id_int, ordered_quantity_int, unit_cost_value),
        )
        log_audit(connection, "create", "purchase_order", po_id, f"PO created for {supplier['name']} / {product['name']}")

    flash("Purchase order created.", "success")
    return redirect_to_inventory("purchase-orders")


@app.route("/inventory/purchase-orders/receive", methods=["POST"])
@login_required("owner", "admin")
def receive_purchase_order():
    purchase_order_id = request.form.get("purchase_order_id", "").strip()
    received_quantity = request.form.get("received_quantity", "").strip()

    try:
        purchase_order_id_int = int(purchase_order_id)
        received_quantity_int = int(received_quantity)
    except ValueError:
        flash("Received quantity must be a whole number.", "error")
        return redirect_to_inventory("purchase-orders")

    if received_quantity_int <= 0:
        flash("Received quantity must be greater than zero.", "error")
        return redirect_to_inventory("purchase-orders")

    with get_connection() as connection:
        order_row = connection.execute(
            """
            SELECT po.id, po.status, poi.id AS item_id, poi.product_id, poi.ordered_quantity, poi.received_quantity, p.name
            FROM purchase_orders po
            JOIN purchase_order_items poi ON poi.purchase_order_id = po.id
            JOIN products p ON p.id = poi.product_id
            WHERE po.id = ?
            """,
            (purchase_order_id_int,),
        ).fetchone()
        if order_row is None:
            flash("Purchase order not found.", "error")
            return redirect_to_inventory("purchase-orders")

        remaining_quantity = order_row["ordered_quantity"] - order_row["received_quantity"]
        if received_quantity_int > remaining_quantity:
            flash(f"Only {remaining_quantity} unit(s) remain to be received.", "error")
            return redirect_to_inventory("purchase-orders")

        new_received_total = order_row["received_quantity"] + received_quantity_int
        new_status = "received" if new_received_total >= order_row["ordered_quantity"] else "partial"
        connection.execute(
            "UPDATE purchase_order_items SET received_quantity = ? WHERE id = ?",
            (new_received_total, order_row["item_id"]),
        )
        connection.execute(
            "UPDATE purchase_orders SET status = ?, received_at = CURRENT_TIMESTAMP WHERE id = ?",
            (new_status, purchase_order_id_int),
        )
        connection.execute(
            "UPDATE products SET stock = stock + ?, last_restocked = CURRENT_TIMESTAMP WHERE id = ?",
            (received_quantity_int, order_row["product_id"]),
        )
        log_stock_movement(connection, order_row["product_id"], received_quantity_int, "purchase_receive")
        log_audit(connection, "receive", "purchase_order", purchase_order_id_int, f"Received {received_quantity_int} unit(s) for {order_row['name']}")

    flash("Purchase order received.", "success")
    return redirect_to_inventory("purchase-orders")


@app.route("/inventory/stock-counts/create", methods=["POST"])
@login_required("owner", "admin")
def create_stock_count():
    title = normalize_lookup_name(request.form.get("title", "")) or "Cycle Count"
    with get_connection() as connection:
        existing = connection.execute("SELECT id FROM stock_counts WHERE status = 'open' LIMIT 1").fetchone()
        if existing is not None:
            flash("Complete the current open stock count first.", "error")
            return redirect_to_inventory("stock-counts")

        stock_count_row = connection.execute(
            "INSERT INTO stock_counts (title, created_by) VALUES (?, ?) RETURNING id",
            (title, session_user_id()),
        ).fetchone()
        stock_count_id = stock_count_row["id"]
        products = connection.execute(
            "SELECT id, stock FROM products WHERE status = 'active' ORDER BY sort_order ASC, id ASC"
        ).fetchall()
        connection.executemany(
            "INSERT INTO stock_count_items (stock_count_id, product_id, system_stock) VALUES (?, ?, ?)",
            [(stock_count_id, product["id"], product["stock"]) for product in products],
        )
        log_audit(connection, "create", "stock_count", stock_count_id, f"Stock count opened: {title}")

    flash("Stock count started.", "success")
    return redirect_to_inventory("stock-counts")


@app.route("/inventory/stock-counts/complete", methods=["POST"])
@login_required("owner", "admin")
def complete_stock_count():
    stock_count_id = request.form.get("stock_count_id", "").strip()
    try:
        stock_count_id_int = int(stock_count_id)
    except ValueError:
        flash("Invalid stock count selection.", "error")
        return redirect_to_inventory("stock-counts")

    with get_connection() as connection:
        count_row = connection.execute(
            "SELECT id, title, status FROM stock_counts WHERE id = ?",
            (stock_count_id_int,),
        ).fetchone()
        if count_row is None or count_row["status"] != "open":
            flash("Open stock count not found.", "error")
            return redirect_to_inventory("stock-counts")

        items = connection.execute(
            "SELECT id, product_id, system_stock FROM stock_count_items WHERE stock_count_id = ?",
            (stock_count_id_int,),
        ).fetchall()

        for item in items:
            raw_counted = request.form.get(f"counted_{item['id']}", "").strip()
            try:
                counted_stock = int(raw_counted)
            except ValueError:
                flash("All counted quantities must be whole numbers.", "error")
                return redirect_to_inventory("stock-counts")
            if counted_stock < 0:
                flash("Counted stock cannot be negative.", "error")
                return redirect_to_inventory("stock-counts")

            variance = counted_stock - item["system_stock"]
            connection.execute(
                "UPDATE stock_count_items SET counted_stock = ?, variance = ? WHERE id = ?",
                (counted_stock, variance, item["id"]),
            )
            if variance != 0:
                connection.execute(
                    "UPDATE products SET stock = ?, last_restocked = CASE WHEN ? > 0 THEN CURRENT_TIMESTAMP ELSE last_restocked END WHERE id = ?",
                    (counted_stock, variance, item["product_id"]),
                )
                log_stock_movement(connection, item["product_id"], variance, "manual_count")
                product = connection.execute(
                    "SELECT id, name, stock, reorder_level FROM products WHERE id = ?",
                    (item["product_id"],),
                ).fetchone()
                if product is not None:
                    maybe_create_adjustment_alert(connection, product, variance, "manual_count")
                    maybe_create_low_stock_alert(connection, item["product_id"], "stock count")

        connection.execute(
            "UPDATE stock_counts SET status = 'completed', completed_at = CURRENT_TIMESTAMP WHERE id = ?",
            (stock_count_id_int,),
        )
        log_audit(connection, "complete", "stock_count", stock_count_id_int, f"Stock count completed: {count_row['title']}")

    flash("Stock count completed and variances applied.", "success")
    return redirect_to_inventory("stock-counts")


@app.route("/inventory/sales/void", methods=["POST"])
@login_required("owner", "admin")
def void_sale():
    sale_id = request.form.get("sale_id", "").strip()
    try:
        sale_id_int = int(sale_id)
    except ValueError:
        flash("Invalid sale selection.", "error")
        return redirect_to_inventory("sales-controls")

    with get_connection() as connection:
        sale = connection.execute("SELECT id, status FROM sales WHERE id = ?", (sale_id_int,)).fetchone()
        if sale is None or sale["status"] != "completed":
            flash("Only completed sales can be voided.", "error")
            return redirect_to_inventory("sales-controls")

        build_sale_stock_return(connection, sale_id_int, "void")
        connection.execute("UPDATE sales SET status = 'voided' WHERE id = ?", (sale_id_int,))
        create_owner_alert(
            connection,
            "sale_void",
            "warning",
            f"Sale voided: #{sale_id_int}",
            f"Sale #{sale_id_int} was voided and stock was returned to inventory.",
            "sale",
            sale_id_int,
            "alert_void_refund_email",
        )
        log_audit(connection, "void", "sale", sale_id_int, "Sale voided and stock returned")

    flash("Sale voided.", "success")
    return redirect_to_inventory("sales-controls")


@app.route("/inventory/sales/refund", methods=["POST"])
@login_required("owner", "admin")
def refund_sale():
    sale_id = request.form.get("sale_id", "").strip()
    try:
        sale_id_int = int(sale_id)
    except ValueError:
        flash("Invalid sale selection.", "error")
        return redirect_to_inventory("sales-controls")

    with get_connection() as connection:
        sale = connection.execute("SELECT id, status FROM sales WHERE id = ?", (sale_id_int,)).fetchone()
        if sale is None or sale["status"] != "completed":
            flash("Only completed sales can be refunded.", "error")
            return redirect_to_inventory("sales-controls")

        build_sale_stock_return(connection, sale_id_int, "refund")
        connection.execute("UPDATE sales SET status = 'refunded' WHERE id = ?", (sale_id_int,))
        create_owner_alert(
            connection,
            "sale_refund",
            "warning",
            f"Sale refunded: #{sale_id_int}",
            f"Sale #{sale_id_int} was refunded and stock was returned to inventory.",
            "sale",
            sale_id_int,
            "alert_void_refund_email",
        )
        log_audit(connection, "refund", "sale", sale_id_int, "Sale refunded and stock returned")

    flash("Sale refunded.", "success")
    return redirect_to_inventory("sales-controls")


@app.route("/owner/backups/create", methods=["POST"])
@login_required("owner")
def create_backup():
    backup_name = f"veyron-pos-backup-{datetime.now().strftime('%Y%m%d-%H%M%S')}.db"
    backup_path = BACKUP_DIR / backup_name
    try:
        copy_database(backup_path)
    except RuntimeError as error:
        flash(str(error), "error")
        return redirect(url_for("owner_dashboard"))
    with get_connection() as connection:
        log_audit(connection, "backup", "database", None, f"Backup created: {backup_name}")
    flash("Backup created.", "success")
    return redirect(url_for("owner_dashboard"))


@app.route("/owner/backups/download/<path:backup_name>")
@login_required("owner")
def download_backup(backup_name: str):
    backup_path = BACKUP_DIR / Path(backup_name).name
    if not backup_path.exists():
        flash("Backup not found.", "error")
        return redirect(url_for("owner_dashboard"))
    return send_file(backup_path, as_attachment=True, download_name=backup_path.name)


@app.route("/owner/backups/restore", methods=["POST"])
@login_required("owner")
def restore_backup():
    if DATABASE_ENGINE == "postgres":
        flash("Restore is only available for SQLite backups. Use managed PostgreSQL restore tooling in production.", "error")
        return redirect(url_for("owner_dashboard"))

    backup_name = request.form.get("backup_name", "").strip()
    backup_path = BACKUP_DIR / Path(backup_name).name
    if not backup_path.exists():
        flash("Backup not found.", "error")
        return redirect(url_for("owner_dashboard"))

    safety_name = f"pre-restore-{backup_path.name}"
    copy_database(BACKUP_DIR / safety_name)
    shutil.copy2(backup_path, DATABASE)
    with get_connection() as connection:
        log_audit(connection, "restore", "database", None, f"Database restored from {backup_path.name}")
    flash("Backup restored.", "success")
    return redirect(url_for("owner_dashboard"))


@app.route("/owner/settings/save", methods=["POST"])
@login_required("owner")
def save_settings():
    settings = {
        "auto_print_receipt": "1" if request.form.get("auto_print_receipt") else "0",
        "cash_drawer_enabled": "1" if request.form.get("cash_drawer_enabled") else "0",
        "printer_mode": request.form.get("printer_mode", "browser").strip() or "browser",
        "drawer_open_note": request.form.get("drawer_open_note", "").strip() or DEFAULT_APP_SETTINGS["drawer_open_note"],
        "alert_low_stock_email": "1" if request.form.get("alert_low_stock_email") else "0",
        "alert_void_refund_email": "1" if request.form.get("alert_void_refund_email") else "0",
        "alert_variance_email": "1" if request.form.get("alert_variance_email") else "0",
    }
    with get_connection() as connection:
        for key, value in settings.items():
            connection.execute(
                "INSERT INTO app_settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
        log_audit(connection, "update", "settings", None, "Hardware and receipt settings updated")
    flash("Settings saved.", "success")
    return redirect(url_for("owner_dashboard"))


@app.route("/owner/hardware/drawer", methods=["POST"])
@login_required("owner", "admin")
def open_cash_drawer_hook():
    with get_connection() as connection:
        log_audit(connection, "drawer_open", "hardware", None, "Cash drawer open requested from receipt screen")
    flash("Drawer open logged. Use a local printer bridge for actual ESC/POS drawer pulses.", "success")
    return redirect(request.form.get("return_to") or url_for("pos"))


init_db()


if __name__ == "__main__":
    app.run(
        host=os.getenv("FLASK_HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", os.getenv("FLASK_PORT", "5000"))),
        debug=os.getenv("FLASK_DEBUG", "1" if not IS_PRODUCTION else "0") == "1",
    )