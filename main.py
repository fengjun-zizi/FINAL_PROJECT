"""
DVD Rental Interactive Dashboard
FastAPI backend with DeepSeek AI floating chat (function calling enabled)
"""
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi import Request
from pydantic import BaseModel
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
DEFAULT_DB_URL = "postgresql+psycopg2://postgres:fengjunli123456@localhost:5432/dvdrental"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DB_URL)

DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "sk-882d02587652426c9e8486aa44daf284")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")
WORKSPACE_ROOT = Path(__file__).resolve().parent
VISUAL_STATE_PATH = WORKSPACE_ROOT / "dashboard_visual_state.json"
AI_OUTPUTS_PATH = WORKSPACE_ROOT / "dashboard_ai_outputs.json"
EDITABLE_SOURCE_SUFFIXES = {".py", ".js", ".html", ".css", ".json", ".md", ".txt"}
MAX_SOURCE_FILE_CHARS = 50000
MAX_TOOL_ROUNDS = 6
MAX_DEVELOPER_HISTORY = 6

COMMON_EDIT_TARGETS = [
    "templates/index.html",
    "static/js/dashboard.js",
    "static/js/chat.js",
    "static/css/styles.css",
    "main.py",
]

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

app = FastAPI(title="DVD Rental Interactive Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


# ---------------------------------------------------------------------
# DB HELPERS
# ---------------------------------------------------------------------
def query(sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def df_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Convert df to JSON-safe records."""
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].astype(str)
        elif out[col].dtype.name.startswith("Int") or out[col].dtype.name.startswith("Float"):
            out[col] = out[col].astype(float).where(out[col].notna(), None)
    out = out.where(pd.notna(out), None)
    return out.to_dict(orient="records")


DB_MUTATION_TABLES: Dict[str, Dict[str, Any]] = {
    "payment": {
        "label": "payment records",
        "pk": "payment_id",
        "allowed_fields": {
            "payment_id": {"type": "int", "filter_only": True},
            "customer_id": {"type": "int"},
            "staff_id": {"type": "int"},
            "rental_id": {"type": "int"},
            "amount": {"type": "float"},
            "payment_date": {"type": "datetime"},
        },
        "required_insert": {"customer_id", "staff_id", "rental_id", "amount"},
        "default_insert_sql": {"payment_date": "CURRENT_TIMESTAMP"},
        "references": {
            "customer_id": ("customer", "customer_id"),
            "staff_id": ("staff", "staff_id"),
            "rental_id": ("rental", "rental_id"),
        },
    },
    "rental": {
        "label": "rental records",
        "pk": "rental_id",
        "allowed_fields": {
            "rental_id": {"type": "int", "filter_only": True},
            "rental_date": {"type": "datetime"},
            "inventory_id": {"type": "int"},
            "customer_id": {"type": "int"},
            "return_date": {"type": "datetime", "nullable": True},
            "staff_id": {"type": "int"},
        },
        "required_insert": {"rental_date", "inventory_id", "customer_id", "staff_id"},
        "references": {
            "inventory_id": ("inventory", "inventory_id"),
            "customer_id": ("customer", "customer_id"),
            "staff_id": ("staff", "staff_id"),
        },
        "cascade_children": [
            {"table": "payment", "fk": "rental_id", "pk": "payment_id"},
        ],
    },
}

DB_MUTATION_TABLE_ALIASES: Dict[str, set[str]] = {
    "payment": {
        "payment", "payments", "purchase", "purchases", "transaction", "transactions",
        "payment record", "payment records", "purchase record", "purchase records",
        "transaction record", "transaction records", "pembelian", "transaksi",
        "购买", "购买记录", "付款", "付款记录", "支付", "支付记录", "交易", "交易记录",
    },
    "rental": {
        "rental", "rentals", "rental record", "rental records", "rent record", "rent records",
        "sewa", "penyewaan", "rental data", "租赁", "出租", "租赁记录", "出租记录",
    },
}

DB_QUERY_SOURCES: Dict[str, Dict[str, Any]] = {
    "payment": {
        "label": "payment records",
        "from_sql": """
            FROM payment p
            LEFT JOIN rental r ON p.rental_id = r.rental_id
            LEFT JOIN inventory i ON r.inventory_id = i.inventory_id
            LEFT JOIN film f ON i.film_id = f.film_id
        """,
        "pk": "payment_id",
        "fields": {
            "payment_id": {"expr": "p.payment_id", "type": "int"},
            "customer_id": {"expr": "p.customer_id", "type": "int"},
            "staff_id": {"expr": "p.staff_id", "type": "int"},
            "rental_id": {"expr": "p.rental_id", "type": "int"},
            "title": {"expr": "f.title", "type": "text"},
            "amount": {"expr": "p.amount", "type": "float"},
            "payment_date": {"expr": "p.payment_date", "type": "datetime"},
        },
    },
    "rental": {
        "label": "rental records",
        "from_sql": """
            FROM rental r
            LEFT JOIN inventory i ON r.inventory_id = i.inventory_id
            LEFT JOIN film f ON i.film_id = f.film_id
        """,
        "pk": "rental_id",
        "fields": {
            "rental_id": {"expr": "r.rental_id", "type": "int"},
            "rental_date": {"expr": "r.rental_date", "type": "datetime"},
            "inventory_id": {"expr": "r.inventory_id", "type": "int"},
            "customer_id": {"expr": "r.customer_id", "type": "int"},
            "title": {"expr": "f.title", "type": "text"},
            "return_date": {"expr": "r.return_date", "type": "datetime"},
            "staff_id": {"expr": "r.staff_id", "type": "int"},
        },
    },
    "inventory": {
        "label": "inventory records",
        "from_sql": """
            FROM inventory i
            JOIN film f ON i.film_id = f.film_id
        """,
        "pk": "inventory_id",
        "fields": {
            "inventory_id": {"expr": "i.inventory_id", "type": "int"},
            "film_id": {"expr": "i.film_id", "type": "int"},
            "title": {"expr": "f.title", "type": "text"},
            "store_id": {"expr": "i.store_id", "type": "int"},
            "last_update": {"expr": "i.last_update", "type": "datetime"},
        },
    },
    "customer": {
        "label": "customer records",
        "from_sql": """
            FROM customer c
            JOIN address a ON c.address_id = a.address_id
        """,
        "pk": "customer_id",
        "fields": {
            "customer_id": {"expr": "c.customer_id", "type": "int"},
            "first_name": {"expr": "c.first_name", "type": "text"},
            "last_name": {"expr": "c.last_name", "type": "text"},
            "email": {"expr": "c.email", "type": "text"},
            "store_id": {"expr": "c.store_id", "type": "int"},
            "activebool": {"expr": "c.activebool", "type": "bool"},
            "city": {"expr": "a.district", "type": "text"},
        },
    },
    "film": {
        "label": "film records",
        "from_sql": "FROM film f",
        "pk": "film_id",
        "fields": {
            "film_id": {"expr": "f.film_id", "type": "int"},
            "title": {"expr": "f.title", "type": "text"},
            "rating": {"expr": "f.rating::text", "type": "text"},
            "rental_rate": {"expr": "f.rental_rate", "type": "float"},
            "rental_duration": {"expr": "f.rental_duration", "type": "int"},
            "length": {"expr": "f.length", "type": "int"},
            "release_year": {"expr": "f.release_year", "type": "int"},
        },
    },
}

DB_QUERY_TABLE_ALIASES: Dict[str, set[str]] = {
    **DB_MUTATION_TABLE_ALIASES,
    "inventory": {"inventory", "inventories", "stock", "copies", "库存", "库存记录", "库存数据"},
    "customer": {"customer", "customers", "pelanggan", "客户", "顾客", "客户记录", "顾客记录"},
    "film": {"film", "films", "movie", "movies", "judul", "电影", "影片", "电影记录"},
}

DB_MUTATION_FIELD_ALIASES: Dict[str, Dict[str, set[str]]] = {
    "payment": {
        "payment_id": {"payment id", "payment_id", "purchase id", "transaction id", "付款id", "支付id", "购买id", "交易id"},
        "customer_id": {"customer id", "customer_id", "customer", "pelanggan", "pelanggan id", "客户id", "顾客id", "客户"},
        "staff_id": {"staff id", "staff_id", "staff", "cashier", "pegawai", "员工id", "员工"},
        "rental_id": {"rental id", "rental_id", "rental", "sewa id", "租赁id", "出租id"},
        "amount": {"amount", "price", "total", "payment amount", "nominal", "biaya", "harga", "金额", "总额", "支付金额"},
        "payment_date": {"payment date", "payment_date", "paid at", "transaction date", "tanggal pembayaran", "支付日期", "付款日期", "交易日期"},
    },
    "rental": {
        "rental_id": {"rental id", "rental_id", "renta id", "renta_id", "sewa id", "租赁id", "出租id"},
        "rental_date": {"rental date", "rental_date", "rent date", "tanggal rental", "tanggal sewa", "租赁日期", "出租日期"},
        "inventory_id": {"inventory id", "inventory_id", "inventory", "stock id", "copy id", "库存id", "库存"},
        "customer_id": {"customer id", "customer_id", "customer", "pelanggan", "pelanggan id", "客户id", "顾客id", "客户"},
        "return_date": {"return date", "return_date", "returned at", "tanggal kembali", "归还日期", "返回日期"},
        "staff_id": {"staff id", "staff_id", "staff", "pegawai", "员工id", "员工"},
    },
}

DB_QUERY_FIELD_ALIASES: Dict[str, Dict[str, set[str]]] = {
    "payment": DB_MUTATION_FIELD_ALIASES["payment"],
    "rental": DB_MUTATION_FIELD_ALIASES["rental"],
    "inventory": {
        "inventory_id": {"inventory id", "inventory_id", "stock id", "copy id", "库存id", "库存"},
        "film_id": {"film id", "film_id", "movie id", "judul id", "电影id"},
        "title": {"title", "film title", "movie title", "judul", "film", "movie", "标题", "电影名", "影片名"},
        "store_id": {"store id", "store_id", "store", "toko", "门店id", "门店"},
        "last_update": {"last update", "updated at", "更新时间"},
    },
    "customer": {
        "customer_id": {"customer id", "customer_id", "pelanggan id", "客户id", "顾客id"},
        "first_name": {"first name", "firstname", "nama depan", "名"},
        "last_name": {"last name", "lastname", "nama belakang", "姓"},
        "email": {"email", "mail", "邮箱"},
        "store_id": {"store id", "store_id", "store", "toko", "门店id", "门店"},
        "activebool": {"active", "is active", "aktif", "是否启用", "启用"},
        "city": {"city", "district", "kota", "城市", "地区"},
    },
    "film": {
        "film_id": {"film id", "film_id", "movie id", "judul id", "电影id"},
        "title": {"title", "film title", "movie title", "judul", "film", "movie", "标题", "电影名", "影片名"},
        "rating": {"rating", "rated", "等级", "评分"},
        "rental_rate": {"rental rate", "rate", "price", "harga", "租金", "价格"},
        "rental_duration": {"rental duration", "duration", "days", "租期", "天数"},
        "length": {"length", "runtime", "duration minutes", "片长", "时长"},
        "release_year": {"release year", "year", "tahun", "年份"},
    },
}

# Extend whitelisted record queries so payment/rental rows can also be filtered by film title.
DB_QUERY_SOURCES["payment"]["from_sql"] = """
    FROM payment p
    LEFT JOIN rental r ON p.rental_id = r.rental_id
    LEFT JOIN inventory i ON r.inventory_id = i.inventory_id
    LEFT JOIN film f ON i.film_id = f.film_id
"""
DB_QUERY_SOURCES["payment"]["fields"]["title"] = {"expr": "f.title", "type": "text"}
DB_QUERY_SOURCES["rental"]["from_sql"] = """
    FROM rental r
    LEFT JOIN inventory i ON r.inventory_id = i.inventory_id
    LEFT JOIN film f ON i.film_id = f.film_id
"""
DB_QUERY_SOURCES["rental"]["fields"]["title"] = {"expr": "f.title", "type": "text"}
DB_MUTATION_FIELD_ALIASES["payment"]["title"] = {"title", "film title", "movie title", "judul", "film", "movie", "标题", "电影标题", "影片标题", "电影"}
DB_MUTATION_FIELD_ALIASES["rental"]["title"] = {"title", "film title", "movie title", "judul", "film", "movie", "标题", "电影标题", "影片标题", "电影"}

DB_QUERY_OPERATION_ALIASES: set[str] = {
    "query", "show", "list", "find", "search", "lookup", "view", "check", "get",
    "lihat", "tampilkan", "cari", "cek", "ambil", "查询", "查看", "列出", "找出", "查找", "搜索", "显示",
}

DB_MUTATION_OPERATION_ALIASES: Dict[str, set[str]] = {
    "insert": {"insert", "add", "create", "new", "append", "tambah", "tambahkan", "buat", "masukkan", "新增", "添加", "加入", "插入"},
    "update": {"update", "modify", "change", "edit", "set", "ubah", "ganti", "perbarui", "修正", "更新", "修改", "改成", "改为"},
    "delete": {"delete", "remove", "drop", "erase", "hapus", "buang", "删掉", "删除", "移除"},
}


def db_record_exists(conn, table: str, key: str, value: Any) -> bool:
    sql = text(f"SELECT 1 FROM {table} WHERE {key} = :value LIMIT 1")
    return conn.execute(sql, {"value": value}).first() is not None


def coerce_mutation_value(table_name: str, field_name: str, value: Any) -> Any:
    meta = DB_MUTATION_TABLES[table_name]["allowed_fields"][field_name]
    field_type = meta["type"]
    if value is None:
        if meta.get("nullable"):
            return None
        raise ValueError(f"{field_name} cannot be null")

    if field_type == "int":
        return int(str(value).strip())
    if field_type == "float":
        return round(float(str(value).strip()), 2)
    if field_type == "datetime":
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            raise ValueError(f"{field_name} must be a valid date or datetime")
        return pd.Timestamp(parsed).to_pydatetime()
    return str(value).strip()


def coerce_query_value(field_type: str, field_name: str, value: Any) -> Any:
    if value is None:
        return None
    if field_type == "int":
        return int(str(value).strip())
    if field_type == "float":
        return round(float(str(value).strip()), 2)
    if field_type == "datetime":
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            raise ValueError(f"{field_name} must be a valid date or datetime")
        return pd.Timestamp(parsed).to_pydatetime()
    if field_type == "bool":
        lowered = str(value).strip().lower()
        if lowered in {"true", "1", "yes", "y", "aktif", "启用"}:
            return True
        if lowered in {"false", "0", "no", "n", "nonaktif", "停用"}:
            return False
        raise ValueError(f"{field_name} must be true or false")
    return str(value).strip()


def canonicalize_mutation_operation(value: Any) -> Optional[str]:
    raw = normalize_intent_text(str(value or ""))
    for operation, aliases in DB_MUTATION_OPERATION_ALIASES.items():
        if raw in aliases or any(alias in raw for alias in aliases):
            return operation
    return None


def canonicalize_mutation_table(value: Any) -> Optional[str]:
    raw = normalize_intent_text(str(value or ""))
    for table_name, aliases in DB_MUTATION_TABLE_ALIASES.items():
        if raw == table_name or raw in aliases or any(alias in raw for alias in aliases):
            return table_name
    return None


def canonicalize_mutation_field(table_name: str, key: Any) -> Optional[str]:
    raw = normalize_intent_text(str(key or "")).replace("_", " ").strip()
    raw = raw.replace(" id", "_id").replace(" date", "_date")
    if raw in DB_MUTATION_TABLES[table_name]["allowed_fields"]:
        return raw
    for field_name, aliases in DB_MUTATION_FIELD_ALIASES[table_name].items():
        if raw == field_name or raw in aliases:
            return field_name
    return None


def canonicalize_query_table(value: Any) -> Optional[str]:
    raw = normalize_intent_text(str(value or ""))
    for table_name, aliases in DB_QUERY_TABLE_ALIASES.items():
        if raw == table_name or raw in aliases or any(alias in raw for alias in aliases):
            return table_name
    return None


def canonicalize_query_field(table_name: str, key: Any) -> Optional[str]:
    raw = normalize_intent_text(str(key or "")).replace("_", " ").strip()
    raw = raw.replace(" id", "_id").replace(" date", "_date")
    if raw in DB_QUERY_SOURCES[table_name]["fields"]:
        return raw
    for field_name, aliases in DB_QUERY_FIELD_ALIASES[table_name].items():
        if raw == field_name or raw in aliases:
            return field_name
    return None


def normalize_mutation_mapping(table_name: str, payload: Any, *, allow_filter_only: bool = True) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    out: Dict[str, Any] = {}
    for raw_key, raw_value in payload.items():
        field_name = canonicalize_mutation_field(table_name, raw_key)
        if not field_name:
            continue
        field_meta = DB_MUTATION_TABLES[table_name]["allowed_fields"][field_name]
        if not allow_filter_only and field_meta.get("filter_only"):
            continue
        if isinstance(raw_value, list):
            out[field_name] = [coerce_mutation_value(table_name, field_name, item) for item in raw_value]
        else:
            out[field_name] = coerce_mutation_value(table_name, field_name, raw_value)
    return out


def build_sql_filters(filters: Dict[str, Any], prefix: str = "filter") -> tuple[List[str], Dict[str, Any]]:
    where_clauses: List[str] = []
    params: Dict[str, Any] = {}
    for field, value in filters.items():
        if isinstance(value, list):
            placeholders = []
            for idx, item in enumerate(value):
                param_name = f"{prefix}_{field}_{idx}"
                placeholders.append(f":{param_name}")
                params[param_name] = item
            where_clauses.append(f"{field} IN ({', '.join(placeholders)})")
        else:
            param_name = f"{prefix}_{field}"
            where_clauses.append(f"{field} = :{param_name}")
            params[param_name] = value
    return where_clauses, params


def execute_db_mutation(action: Dict[str, Any]) -> Dict[str, Any]:
    try:
        operation = canonicalize_mutation_operation(action.get("operation") or action.get("mode"))
        table_name = canonicalize_mutation_table(action.get("table"))
        if not operation:
            raise ValueError("Unsupported database operation. Use insert, update, or delete.")
        if not table_name:
            raise ValueError("Only payment or rental records can be modified.")

        table_meta = DB_MUTATION_TABLES[table_name]
        filters = normalize_mutation_mapping(table_name, action.get("filters"), allow_filter_only=True)
        values = normalize_mutation_mapping(table_name, action.get("values"), allow_filter_only=False)

        if operation in {"update", "delete"} and not filters:
            raise ValueError(f"{operation.title()} needs at least one filter. I won't modify records blindly.")
        if operation in {"insert", "update"} and not values:
            raise ValueError(f"{operation.title()} needs at least one value to write.")
        if operation == "insert":
            missing = sorted(table_meta["required_insert"] - set(values.keys()))
            if missing:
                raise ValueError(f"Insert into {table_name} is missing required fields: {', '.join(missing)}")

        with engine.begin() as conn:
            for field_name, (ref_table, ref_key) in table_meta.get("references", {}).items():
                if field_name in values:
                    value_items = values[field_name] if isinstance(values[field_name], list) else [values[field_name]]
                    for item in value_items:
                        if not db_record_exists(conn, ref_table, ref_key, item):
                            raise ValueError(f"{field_name}={item} does not exist in {ref_table}.{ref_key}")
                if field_name in filters:
                    filter_items = filters[field_name] if isinstance(filters[field_name], list) else [filters[field_name]]
                    for item in filter_items:
                        if not db_record_exists(conn, ref_table, ref_key, item):
                            raise ValueError(f"{field_name}={item} does not exist in {ref_table}.{ref_key}")

            if operation == "insert":
                insert_values = dict(values)
                columns = list(insert_values.keys())
                value_sql = [f":{field}" for field in columns]
                for field_name, sql_default in table_meta.get("default_insert_sql", {}).items():
                    if field_name not in insert_values:
                        columns.append(field_name)
                        value_sql.append(sql_default)
                sql = text(
                    f"INSERT INTO {table_name} ({', '.join(columns)}) "
                    f"VALUES ({', '.join(value_sql)}) RETURNING {table_meta['pk']}"
                )
                inserted_id = conn.execute(sql, insert_values).scalar()
                return {
                    **action,
                    "type": "mutate_records",
                    "ok": True,
                    "operation": operation,
                    "table": table_name,
                    "affected_rows": 1,
                    "inserted_id": inserted_id,
                    "summary": f"Inserted 1 {table_name} record (id={inserted_id}).",
                }

            where_clauses, params = build_sql_filters(filters, "filter")

            if operation == "delete":
                target_sql = text(
                    f"SELECT {table_meta['pk']} FROM {table_name} WHERE {' AND '.join(where_clauses)}"
                )
                target_ids = [row[0] for row in conn.execute(target_sql, params).fetchall()]
                if not target_ids:
                    raise ValueError("No matching records were found to delete.")

                related_deleted: Dict[str, int] = {}
                for child in table_meta.get("cascade_children", []):
                    child_filter_clauses, child_params = build_sql_filters({child["fk"]: target_ids}, "cascade")
                    child_sql = text(
                        f"DELETE FROM {child['table']} WHERE {' AND '.join(child_filter_clauses)} "
                        f"RETURNING {child['pk']}"
                    )
                    child_rows = conn.execute(child_sql, child_params).fetchall()
                    related_deleted[child["table"]] = len(child_rows)

                sql = text(
                    f"DELETE FROM {table_name} WHERE {' AND '.join(where_clauses)} "
                    f"RETURNING {table_meta['pk']}"
                )
                rows = conn.execute(sql, params).fetchall()
                count = len(rows)
                related_summary = ", ".join(
                    f"{child_table}={child_count}" for child_table, child_count in related_deleted.items() if child_count
                )
                summary = f"Deleted {count} {table_name} record(s)."
                if related_summary:
                    summary += f" Cascade deleted related records: {related_summary}."
                return {
                    **action,
                    "type": "mutate_records",
                    "ok": True,
                    "operation": operation,
                    "table": table_name,
                    "affected_rows": count,
                    "related_deleted": related_deleted,
                    "summary": summary,
                }

            set_clauses = [f"{field} = :value_{field}" for field in values.keys()]
            params.update({f"value_{field}": value for field, value in values.items()})
            sql = text(
                f"UPDATE {table_name} SET {', '.join(set_clauses)} "
                f"WHERE {' AND '.join(where_clauses)} RETURNING {table_meta['pk']}"
            )
            rows = conn.execute(sql, params).fetchall()
            count = len(rows)
            if count == 0:
                raise ValueError("No matching records were found to update.")
            return {
                **action,
                "type": "mutate_records",
                "ok": True,
                "operation": operation,
                "table": table_name,
                "affected_rows": count,
                "summary": f"Updated {count} {table_name} record(s).",
            }
    except Exception as exc:
        return {
            **action,
            "type": "mutate_records",
            "ok": False,
            "error": str(exc),
        }


# ---------------------------------------------------------------------
# DATA LAYER — All queries used by the dashboard
# ---------------------------------------------------------------------
def kpi_overview() -> Dict[str, Any]:
    df = query("""
        SELECT
            (SELECT COUNT(*) FROM film)                              AS total_film,
            (SELECT COUNT(DISTINCT category_id) FROM film_category)  AS total_genre,
            (SELECT ROUND(AVG(length)::numeric,1) FROM film)         AS avg_duration,
            (SELECT COUNT(DISTINCT language_id) FROM film)           AS total_language,
            (SELECT ROUND(AVG(rental_rate)::numeric,2) FROM film)    AS avg_rate,
            (SELECT COUNT(*) FROM film WHERE rental_duration <= 3)   AS short_rental_films
    """).iloc[0]
    return {
        "total_film": int(df.total_film),
        "total_genre": int(df.total_genre),
        "avg_duration": float(df.avg_duration),
        "total_language": int(df.total_language),
        "avg_rate": float(df.avg_rate),
        "short_rental_films": int(df.short_rental_films),
    }


def kpi_popularity() -> Dict[str, Any]:
    df = query("""
        SELECT
            COUNT(*) AS total_rental,
            COUNT(DISTINCT customer_id) AS active_customers,
            COUNT(DISTINCT inventory_id) AS items_rented,
            ROUND(AVG(EXTRACT(EPOCH FROM (return_date - rental_date))/86400)::numeric,1) AS avg_days,
            COUNT(*) FILTER (WHERE return_date IS NULL) AS not_returned,
            COUNT(DISTINCT DATE(rental_date)) AS active_days
        FROM rental
    """).iloc[0]
    return {
        "total_rental": int(df.total_rental),
        "active_customers": int(df.active_customers),
        "items_rented": int(df.items_rented),
        "avg_days": float(df.avg_days),
        "not_returned": int(df.not_returned),
        "active_days": int(df.active_days),
    }


def kpi_actor() -> Dict[str, Any]:
    a = query("SELECT COUNT(*) AS c FROM actor").iloc[0].c
    f = query("SELECT COUNT(*) AS c FROM film").iloc[0].c
    g = query("SELECT COUNT(DISTINCT category_id) AS c FROM film_category").iloc[0].c
    return {"total_actors": int(a), "total_films": int(f), "total_genres": int(g)}


def kpi_revenue() -> Dict[str, Any]:
    df = query("""
        SELECT ROUND(SUM(amount)::numeric,2) AS total_rev,
               COUNT(*) AS total_payment,
               ROUND(AVG(amount)::numeric,2) AS avg_payment,
               COUNT(DISTINCT customer_id) AS total_cust
        FROM payment
    """).iloc[0]
    unpaid = query("""
        SELECT COUNT(*) AS u
        FROM customer c
        WHERE c.customer_id NOT IN (SELECT DISTINCT customer_id FROM payment)
    """).iloc[0].u
    return {
        "total_rev": float(df.total_rev),
        "total_payment": int(df.total_payment),
        "avg_payment": float(df.avg_payment),
        "total_cust": int(df.total_cust),
        "unpaid_customers": int(unpaid),
    }


# ---------- chart datasets ----------
def genre_distribution() -> List[Dict]:
    return df_to_records(query("""
        SELECT c.name AS genre, COUNT(f.film_id) AS film_count
        FROM film f
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        GROUP BY c.name ORDER BY film_count DESC
    """))


def rating_distribution() -> List[Dict]:
    return df_to_records(query(
        "SELECT rating::text AS rating, COUNT(*) AS count FROM film GROUP BY rating ORDER BY count DESC"
    ))


def duration_by_genre() -> List[Dict]:
    return df_to_records(query("""
        SELECT c.name AS genre,
               ROUND(AVG(f.length)::numeric,1) AS avg_duration,
               MIN(f.length) AS min_dur,
               MAX(f.length) AS max_dur,
               COUNT(f.film_id) AS film_count
        FROM film f
        JOIN film_category fc ON f.film_id=fc.film_id
        JOIN category c ON fc.category_id=c.category_id
        GROUP BY c.name ORDER BY avg_duration DESC
    """))


def rental_period_distribution() -> List[Dict]:
    return df_to_records(query("""
        SELECT rental_duration::text || ' days' AS rental_period,
               COUNT(*) AS film_count
        FROM film GROUP BY rental_duration ORDER BY rental_duration
    """))


def genre_rating_heatmap() -> List[Dict]:
    return df_to_records(query("""
        SELECT c.name AS genre, f.rating::text AS rating, COUNT(*) AS count
        FROM film f
        JOIN film_category fc ON f.film_id=fc.film_id
        JOIN category c ON fc.category_id=c.category_id
        GROUP BY c.name, f.rating
        ORDER BY c.name, f.rating
    """))


def top_rented_films(limit: int = 15, genre: Optional[str] = None) -> List[Dict]:
    where_genre = ""
    if genre:
        safe = genre.replace("'", "''")
        where_genre = f"WHERE c.name ILIKE '{safe}'"
    return df_to_records(query(f"""
        SELECT f.title, c.name AS genre, f.rating::text AS rating,
               COUNT(r.rental_id) AS total_rentals, f.rental_rate
        FROM film f JOIN film_category fc ON f.film_id=fc.film_id
        JOIN category c ON fc.category_id=c.category_id
        JOIN inventory i ON f.film_id=i.film_id
        JOIN rental r ON i.inventory_id=r.inventory_id
        {where_genre}
        GROUP BY f.film_id, f.title, c.name, f.rating, f.rental_rate
        ORDER BY total_rentals DESC LIMIT {int(limit)}
    """))


def least_rented_films(limit: int = 15) -> List[Dict]:
    return df_to_records(query(f"""
        SELECT f.title, c.name AS genre, f.rating::text AS rating,
               COALESCE(COUNT(r.rental_id), 0) AS total_rentals,
               COUNT(DISTINCT i.inventory_id) AS stock_units
        FROM film f JOIN film_category fc ON f.film_id=fc.film_id
        JOIN category c ON fc.category_id=c.category_id
        JOIN inventory i ON f.film_id=i.film_id
        LEFT JOIN rental r ON i.inventory_id=r.inventory_id
        GROUP BY f.film_id, f.title, c.name, f.rating
        ORDER BY total_rentals ASC, f.title ASC LIMIT {int(limit)}
    """))


def monthly_rental_trend() -> List[Dict]:
    return df_to_records(query("""
        WITH bounds AS (
            SELECT
                DATE_TRUNC('month', MIN(rental_date))::date AS min_month,
                DATE_TRUNC('month', MAX(rental_date))::date AS max_month
            FROM rental
        ),
        months AS (
            SELECT GENERATE_SERIES(min_month, max_month, INTERVAL '1 month')::date AS month
            FROM bounds
        ),
        stores AS (
            SELECT DISTINCT store_id FROM inventory
        ),
        rental_counts AS (
            SELECT
                DATE_TRUNC('month', r.rental_date)::date AS month,
                i.store_id,
                COUNT(*)::int AS total
            FROM rental r
            JOIN inventory i ON r.inventory_id = i.inventory_id
            GROUP BY month, i.store_id
        )
        SELECT
            TO_CHAR(m.month, 'YYYY-MM') AS month,
            'Store ' || s.store_id::text AS store,
            COALESCE(rc.total, 0) AS total
        FROM months m
        CROSS JOIN stores s
        LEFT JOIN rental_counts rc ON rc.month = m.month AND rc.store_id = s.store_id
        ORDER BY m.month, s.store_id
    """))


def ml_next_month_popular_films(limit: int = 10) -> Dict[str, Any]:
    """Forecast demand opportunity and business recommendation per film.

    This replaces a plain popularity ranking with a decision-oriented score:
    predicted demand, opportunity score, action recommendation, and risk.
    """
    df = query("""
        SELECT
            f.film_id,
            f.title,
            c.name AS genre,
            f.rating::text AS rating,
            ROUND(f.rental_rate::numeric, 2) AS rental_rate,
            (SELECT COUNT(*) FROM inventory inv WHERE inv.film_id = f.film_id)::int AS stock_units,
            DATE_TRUNC('month', r.rental_date)::date AS rental_month,
            COUNT(*)::int AS rentals
        FROM rental r
        JOIN inventory i ON r.inventory_id = i.inventory_id
        JOIN film f ON i.film_id = f.film_id
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        GROUP BY f.film_id, f.title, c.name, f.rating, f.rental_rate, rental_month
        ORDER BY rental_month, f.title
    """)

    if df.empty:
        return {"next_month": None, "model": "demand_opportunity_forecast", "predictions": []}

    df["rental_month"] = pd.to_datetime(df["rental_month"])
    df["month_key"] = df["rental_month"].dt.strftime("%Y-%m")
    months = sorted(df["month_key"].dropna().unique())
    month_index = {month: idx for idx, month in enumerate(months)}
    next_month = str(pd.Period(months[-1], freq="M") + 1)
    last_idx = len(months) - 1
    next_idx = len(months)

    raw_rows = []
    for film_id, group in df.groupby("film_id"):
        meta = group.iloc[0]
        series = [0.0] * len(months)
        for _, item in group.iterrows():
            series[month_index[item.month_key]] = float(item.rentals)

        observed = [value for value in series if value > 0]
        if not observed:
            continue

        recent = series[-3:]
        recent_weights = [0.2, 0.3, 0.5][-len(recent):]
        recent_avg = sum(value * weight for value, weight in zip(recent, recent_weights)) / sum(recent_weights)

        xs = list(range(len(series)))
        ys = series
        x_mean = sum(xs) / len(xs)
        y_mean = sum(ys) / len(ys)
        denom = sum((x - x_mean) ** 2 for x in xs) or 1.0
        slope = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys)) / denom
        intercept = y_mean - slope * x_mean
        linear_pred = max(0.0, intercept + slope * next_idx)
        last_month_rentals = series[last_idx]
        total_rentals = sum(series)

        predicted = max(0.0, (linear_pred * 0.45) + (recent_avg * 0.35) + (last_month_rentals * 0.20))
        trend_label = "rising" if slope > 0.25 else "declining" if slope < -0.25 else "stable"
        confidence = min(95, max(45, 50 + min(len(observed), 8) * 4 + min(abs(slope) * 8, 12)))

        raw_rows.append({
            "rank": 0,
            "title": str(meta.title),
            "genre": str(meta.genre),
            "rating": str(meta.rating),
            "rental_rate": float(meta.rental_rate),
            "stock_units": int(meta.stock_units),
            "total_rentals": round(total_rentals, 1),
            "last_month_rentals": round(last_month_rentals, 1),
            "recent_avg": round(recent_avg, 1),
            "trend": trend_label,
            "trend_slope": round(slope, 2),
            "predicted_rentals": round(predicted, 1),
            "predicted_demand": round(predicted, 1),
            "confidence": round(confidence, 0),
        })

    max_predicted = max([row["predicted_demand"] for row in raw_rows] + [1])
    max_recent = max([row["recent_avg"] for row in raw_rows] + [1])
    max_total = max([row["total_rentals"] for row in raw_rows] + [1])
    max_stock = max([row["stock_units"] for row in raw_rows] + [1])
    max_rate = max([row["rental_rate"] for row in raw_rows] + [1])

    rows = []
    high_demand_cutoff = max_predicted * 0.65
    for row in raw_rows:
        price_attractiveness = max(0.0, (max_rate - row["rental_rate"]) / max_rate)
        trend_bonus = 5 if row["trend"] == "rising" else -4 if row["trend"] == "declining" else 0
        opportunity_score = (
            (row["predicted_demand"] / max_predicted) * 38
            + (row["recent_avg"] / max_recent) * 24
            + (row["total_rentals"] / max_total) * 14
            + (row["stock_units"] / max_stock) * 9
            + price_attractiveness * 5
            + (row["confidence"] / 100) * 5
            + trend_bonus
        )
        opportunity_score = max(0, min(100, opportunity_score))

        if opportunity_score >= 75:
            recommendation = "Promote next month"
        elif opportunity_score >= 60:
            recommendation = "Feature prominently"
        elif opportunity_score >= 45:
            recommendation = "Keep visible and monitor"
        else:
            recommendation = "Low priority"

        if row["predicted_demand"] >= high_demand_cutoff and row["stock_units"] <= 3:
            risk = "Stock risk"
        elif row["trend"] == "declining":
            risk = "Demand drop risk"
        elif row["recent_avg"] <= 1:
            risk = "Low recent demand"
        else:
            risk = "Low risk"

        reason = (
            f"Predicted demand {row['predicted_demand']}, {row['trend']} trend, "
            f"recent average {row['recent_avg']}, stock {row['stock_units']} units."
        )

        rows.append({
            **row,
            "opportunity_score": round(opportunity_score, 1),
            "recommendation": recommendation,
            "risk": risk,
            "reason": reason,
        })

    rows.sort(
        key=lambda item: (
            item["opportunity_score"],
            item["predicted_demand"],
            item["recent_avg"],
            item["total_rentals"],
        ),
        reverse=True,
    )
    rows = rows[:int(limit)]
    for idx, item in enumerate(rows, start=1):
        item["rank"] = idx

    return {
        "next_month": next_month,
        "model": "demand_opportunity_forecast",
        "explanation": "Scores combine forecast demand, recent demand, historical demand, stock readiness, price attractiveness, confidence, and trend risk.",
        "predictions": rows,
    }


def normalize_match_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def ml_popularity_advisor(items: List[str], limit: int = 10) -> Dict[str, Any]:
    """Score user-provided film/genre/rating inputs for rental decisions."""
    cleaned_items = []
    for item in items or []:
        text_value = str(item or "").strip()
        if text_value:
            cleaned_items.append(text_value)
    cleaned_items = cleaned_items[:20]
    if not cleaned_items:
        return {
            "model": "candidate_demand_opportunity_score",
            "inputs": [],
            "recommendations": [],
            "message": "No candidate items were provided.",
        }

    stats = query("""
        WITH max_rental AS (
            SELECT MAX(rental_date) AS max_date FROM rental
        )
        SELECT
            f.film_id,
            f.title,
            c.name AS genre,
            f.rating::text AS rating,
            ROUND(f.rental_rate::numeric, 2) AS rental_rate,
            COUNT(r.rental_id)::int AS total_rentals,
            COUNT(r.rental_id) FILTER (
                WHERE r.rental_date >= (SELECT max_date FROM max_rental) - INTERVAL '30 days'
            )::int AS recent_30d,
            COUNT(r.rental_id) FILTER (
                WHERE r.rental_date >= (SELECT max_date FROM max_rental) - INTERVAL '90 days'
            )::int AS recent_90d,
            COUNT(DISTINCT i.inventory_id)::int AS stock_units
        FROM film f
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        JOIN inventory i ON f.film_id = i.film_id
        LEFT JOIN rental r ON i.inventory_id = r.inventory_id
        GROUP BY f.film_id, f.title, c.name, f.rating, f.rental_rate
    """)

    if stats.empty:
        return {
            "model": "candidate_demand_opportunity_score",
            "inputs": cleaned_items,
            "recommendations": [],
            "message": "No rental statistics were available.",
        }

    forecast = ml_next_month_popular_films(1000).get("predictions", [])
    forecast_by_title = {normalize_match_text(row["title"]): row for row in forecast}

    max_total = max(float(stats["total_rentals"].max() or 1), 1.0)
    max_recent = max(float(stats["recent_30d"].max() or 1), 1.0)
    max_recent_90 = max(float(stats["recent_90d"].max() or 1), 1.0)
    max_stock = max(float(stats["stock_units"].max() or 1), 1.0)
    max_forecast = max([float(row.get("opportunity_score", 0) or 0) for row in forecast] + [1.0])
    max_rate = max(float(stats["rental_rate"].max() or 1), 1.0)

    matches: Dict[int, Dict[str, Any]] = {}
    unmatched = []
    for raw_item in cleaned_items:
        item_key = normalize_match_text(raw_item)
        matched_any = False
        for _, row in stats.iterrows():
            title_key = normalize_match_text(row.title)
            genre_key = normalize_match_text(row.genre)
            rating_key = normalize_match_text(row.rating)
            is_match = (
                item_key in title_key
                or title_key in item_key
                or item_key == genre_key
                or item_key in genre_key
                or item_key == rating_key
            )
            if not is_match:
                continue

            matched_any = True
            film_id = int(row.film_id)
            forecast_row = forecast_by_title.get(title_key, {})
            forecast_score = float(forecast_row.get("opportunity_score", 0) or 0)
            predicted_demand = float(forecast_row.get("predicted_demand", 0) or 0)
            total_score = float(row.total_rentals or 0)
            recent_score = float(row.recent_30d or 0)
            recent_90_score = float(row.recent_90d or 0)
            stock_score = float(row.stock_units or 0)
            price_score = max_rate - float(row.rental_rate or 0)

            score = (
                (forecast_score / max_forecast) * 35
                + (recent_score / max_recent) * 25
                + (total_score / max_total) * 20
                + (recent_90_score / max_recent_90) * 10
                + (stock_score / max_stock) * 5
                + (price_score / max_rate) * 5
            )

            trend = forecast_row.get("trend", "stable")
            forecast_risk = forecast_row.get("risk", "Unknown risk")
            if score >= 70:
                recommendation = "Prioritize for next-month promotion"
            elif score >= 50:
                recommendation = "Keep visible and monitor demand"
            elif score >= 32:
                recommendation = "Use selectively for niche demand"
            else:
                recommendation = "Low priority unless strategically needed"

            reason_parts = [
                f"{int(total_score)} historical rentals",
                f"{int(recent_score)} rentals in recent 30 days",
                f"{round(predicted_demand, 1)} predicted demand",
                f"{trend} forecast trend",
                str(forecast_risk),
            ]

            candidate = {
                "matched_input": raw_item,
                "film_id": film_id,
                "title": str(row.title),
                "genre": str(row.genre),
                "rating": str(row.rating),
                "rental_rate": float(row.rental_rate),
                "total_rentals": int(total_score),
                "recent_30d": int(recent_score),
                "recent_90d": int(recent_90_score),
                "stock_units": int(stock_score),
                "opportunity_score": round(forecast_score, 1),
                "predicted_demand": round(predicted_demand, 1),
                "trend": str(trend),
                "risk": str(forecast_risk),
                "score": round(score, 1),
                "recommendation": recommendation,
                "reason": "; ".join(reason_parts),
            }

            previous = matches.get(film_id)
            if previous is None or candidate["score"] > previous["score"]:
                matches[film_id] = candidate

        if not matched_any:
            unmatched.append(raw_item)

    recommendations = sorted(matches.values(), key=lambda item: item["score"], reverse=True)[:int(limit or 10)]
    for idx, item in enumerate(recommendations, start=1):
        item["rank"] = idx

    return {
        "model": "candidate_demand_opportunity_score",
        "inputs": cleaned_items,
        "unmatched": unmatched,
        "recommendations": recommendations,
        "message": "Scores combine demand opportunity forecast, recent rentals, lifetime demand, stock availability, and rental rate.",
    }


def rental_by_dow() -> List[Dict]:
    return df_to_records(query("""
        SELECT TRIM(TO_CHAR(rental_date, 'Day')) AS day,
               EXTRACT(DOW FROM rental_date)::int AS sort_order,
               COUNT(*) AS total
        FROM rental GROUP BY day, sort_order ORDER BY sort_order
    """))


def top_films_per_store() -> List[Dict]:
    return df_to_records(query("""
        WITH ranked AS (
            SELECT i.store_id, f.title, c.name AS genre,
                   COUNT(r.rental_id) AS total_rentals,
                   ROW_NUMBER() OVER (PARTITION BY i.store_id ORDER BY COUNT(r.rental_id) DESC) AS rn
            FROM rental r JOIN inventory i ON r.inventory_id=i.inventory_id
            JOIN film f ON i.film_id=f.film_id
            JOIN film_category fc ON f.film_id=fc.film_id
            JOIN category c ON fc.category_id=c.category_id
            GROUP BY i.store_id, f.title, c.name
        )
        SELECT 'Store '||store_id AS store, title, genre, total_rentals
        FROM ranked WHERE rn <= 5 ORDER BY store_id, total_rentals DESC
    """))


def actor_film_count(limit: Optional[int] = None) -> List[Dict]:
    lim = f"LIMIT {int(limit)}" if limit else ""
    return df_to_records(query(f"""
        SELECT a.first_name||' '||a.last_name AS actor, COUNT(fa.film_id) AS film_count
        FROM actor a JOIN film_actor fa ON a.actor_id=fa.actor_id
        GROUP BY a.actor_id ORDER BY film_count DESC {lim}
    """))


def actor_rental_count(limit: Optional[int] = None) -> List[Dict]:
    lim = f"LIMIT {int(limit)}" if limit else ""
    return df_to_records(query(f"""
        SELECT a.first_name||' '||a.last_name AS actor, COUNT(r.rental_id) AS rental_count
        FROM actor a JOIN film_actor fa ON a.actor_id=fa.actor_id
        JOIN inventory i ON fa.film_id=i.film_id
        JOIN rental r ON i.inventory_id=r.inventory_id
        GROUP BY a.actor_id ORDER BY rental_count DESC {lim}
    """))


def top_actor_genre_mix() -> List[Dict]:
    return df_to_records(query("""
        WITH top_actors AS (
            SELECT a.actor_id, a.first_name||' '||a.last_name AS actor
            FROM actor a JOIN film_actor fa ON a.actor_id=fa.actor_id
            JOIN inventory i ON fa.film_id=i.film_id
            JOIN rental r ON i.inventory_id=r.inventory_id
            GROUP BY a.actor_id ORDER BY COUNT(r.rental_id) DESC LIMIT 5
        )
        SELECT ta.actor, c.name AS genre, COUNT(*) AS count
        FROM top_actors ta
        JOIN film_actor fa ON ta.actor_id=fa.actor_id
        JOIN film_category fc ON fa.film_id=fc.film_id
        JOIN category c ON fc.category_id=c.category_id
        GROUP BY ta.actor, c.name ORDER BY ta.actor, count DESC
    """))


def monthly_revenue_per_store() -> List[Dict]:
    return df_to_records(query("""
        SELECT TO_CHAR(p.payment_date,'YYYY-MM') AS month,
               'Store '||s.store_id::text AS store,
               ROUND(SUM(p.amount)::numeric,2) AS revenue
        FROM payment p
        JOIN staff st ON p.staff_id = st.staff_id
        JOIN store s ON st.store_id = s.store_id
        GROUP BY month, s.store_id ORDER BY month, s.store_id
    """))


def monthly_revenue() -> List[Dict]:
    return df_to_records(query("""
        SELECT TO_CHAR(payment_date,'YYYY-MM') AS month,
               ROUND(SUM(amount)::numeric,2) AS total_revenue
        FROM payment
        GROUP BY month ORDER BY month
    """))


def revenue_by_genre() -> List[Dict]:
    return df_to_records(query("""
        SELECT c.name AS genre, ROUND(SUM(p.amount)::numeric,2) AS revenue
        FROM payment p JOIN rental r ON p.rental_id=r.rental_id
        JOIN inventory i ON r.inventory_id=i.inventory_id
        JOIN film_category fc ON i.film_id=fc.film_id
        JOIN category c ON fc.category_id=c.category_id
        GROUP BY c.name ORDER BY revenue DESC
    """))


def top_customers_by_spending(limit: int = 10) -> List[Dict]:
    return df_to_records(query(f"""
        SELECT c.first_name||' '||c.last_name AS customer,
                ROUND(SUM(p.amount)::numeric,2) AS spending
        FROM customer c JOIN payment p ON c.customer_id=p.customer_id
        GROUP BY c.customer_id ORDER BY spending DESC LIMIT {int(limit)}
    """))


def genre_extremes() -> List[Dict]:
    return df_to_records(query("""
        SELECT genre, title, length AS duration, type FROM (
            SELECT c.name AS genre, f.title, f.length,
                   'Longest' AS type,
                   ROW_NUMBER() OVER (PARTITION BY c.name ORDER BY f.length DESC) AS rn
            FROM film f JOIN film_category fc ON f.film_id=fc.film_id
            JOIN category c ON fc.category_id=c.category_id
            UNION ALL
            SELECT c.name, f.title, f.length,
                   'Shortest',
                   ROW_NUMBER() OVER (PARTITION BY c.name ORDER BY f.length ASC)
            FROM film f JOIN film_category fc ON f.film_id=fc.film_id
            JOIN category c ON fc.category_id=c.category_id
        ) x WHERE rn = 1 ORDER BY genre, type DESC
    """))


class VisualStateRequest(BaseModel):
    mode: str
    color: Optional[str] = None
    theme: Optional[str] = None
    updatedAt: Optional[int] = None


def default_visual_state() -> Dict[str, Any]:
    return {"mode": "theme", "theme": "dark", "updatedAt": 0}


def read_visual_state_file() -> Dict[str, Any]:
    if not VISUAL_STATE_PATH.exists():
        return default_visual_state()
    try:
        data = json.loads(VISUAL_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return default_visual_state()
    if not isinstance(data, dict):
        return default_visual_state()
    return data


def write_visual_state_file(state: VisualStateRequest) -> Dict[str, Any]:
    data = state.model_dump(exclude_none=True)
    mode = data.get("mode")
    if mode == "custom-background":
        if not data.get("color"):
            raise ValueError("custom-background state requires color")
    elif mode == "theme":
        if data.get("theme") not in {"dark", "light", "gold", "ocean", "sunset"}:
            raise ValueError("theme state requires a valid theme")
    else:
        raise ValueError("unknown visual state mode")

    if "updatedAt" not in data:
        data["updatedAt"] = 0
    VISUAL_STATE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return data


def default_ai_outputs() -> Dict[str, Any]:
    return {"items": [], "updatedAt": 0}


def read_ai_outputs_file() -> Dict[str, Any]:
    if not AI_OUTPUTS_PATH.exists():
        return default_ai_outputs()
    try:
        data = json.loads(AI_OUTPUTS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return default_ai_outputs()
    if not isinstance(data, dict) or not isinstance(data.get("items"), list):
        return default_ai_outputs()
    return data


def write_ai_outputs_file(payload: Dict[str, Any]) -> Dict[str, Any]:
    items = payload.get("items", []) if isinstance(payload, dict) else []
    if not isinstance(items, list):
        raise ValueError("items must be a list")

    safe_items = []
    for item in items[-8:]:
        if not isinstance(item, dict):
            continue
        if item.get("type") not in {"chart", "table"}:
            continue
        safe_items.append(item)

    data = {
        "items": safe_items,
        "updatedAt": payload.get("updatedAt", 0) if isinstance(payload, dict) else 0,
    }
    AI_OUTPUTS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return data


# ---------------------------------------------------------------------
# REST endpoints used by the page
# ---------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")


@app.get("/api/visual-state")
def api_visual_state():
    return read_visual_state_file()


@app.post("/api/visual-state")
def api_update_visual_state(state: VisualStateRequest):
    try:
        return write_visual_state_file(state)
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@app.get("/api/ai-outputs")
def api_ai_outputs():
    return read_ai_outputs_file()


@app.post("/api/ai-outputs")
def api_update_ai_outputs(payload: Dict[str, Any]):
    try:
        return write_ai_outputs_file(payload)
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@app.get("/api/dashboard")
def api_dashboard():
    """Single endpoint that returns everything the page needs on first load."""
    return {
        "kpi": {
            "overview": kpi_overview(),
            "popularity": kpi_popularity(),
            "actor": kpi_actor(),
            "revenue": kpi_revenue(),
        },
        "overview": {
            "genre_distribution": genre_distribution(),
            "rating_distribution": rating_distribution(),
            "duration_by_genre": duration_by_genre(),
            "rental_period_distribution": rental_period_distribution(),
            "genre_rating_heatmap": genre_rating_heatmap(),
            "genre_extremes": genre_extremes(),
        },
        "popularity": {
            "top_rented_films": top_rented_films(15),
            "monthly_rental_trend": monthly_rental_trend(),
            "rental_by_dow": rental_by_dow(),
            "top_films_per_store": top_films_per_store(),
            "least_rented_films": least_rented_films(15),
        },
        "actor": {
            "actor_film_count": actor_film_count(),
            "actor_rental_count": actor_rental_count(),
            "top_actor_genre_mix": top_actor_genre_mix(),
        },
        "revenue": {
            "monthly_revenue_per_store": monthly_revenue_per_store(),
            "monthly_revenue": monthly_revenue(),
            "revenue_by_genre": revenue_by_genre(),
            "top_customers": top_customers_by_spending(10),
        },
    }


# ---------------------------------------------------------------------
# AI CHAT (DeepSeek with function-calling style actions)
# ---------------------------------------------------------------------
class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    messages: List[ChatMessage]


# Tools the AI can call. We use a JSON-instruction style (DeepSeek supports
# OpenAI-format function calling, but to keep this simple & robust across
# variants we let the model emit a JSON action block we parse server-side).
ACTION_SCHEMA = """
You can ALWAYS reply in plain conversational text.
ADDITIONALLY, when the user asks you to DO something interactive on the dashboard
(change theme, change background color, switch chart type, render a chart/table, filter, show a top list, jump to a section),
APPEND a fenced JSON action block at the END of your reply, like this:

```action
{
  "actions": [
    { "type": "set_theme", "theme": "light" }
  ]
}
```

Available action types (use ONLY these):

1. set_theme              -> {"type":"set_theme","theme":"dark"|"light"|"gold"|"ocean"|"sunset"}
2. scroll_to              -> {"type":"scroll_to","section":"overview"|"popularity"|"actor"|"revenue"}
3. render_chart           -> {"type":"render_chart","chart":"<chart_id>","params":{...}}
4. render_table           -> {"type":"render_table","table":"<chart_id>","params":{...},"title":"<short title>"}
5. filter_genre           -> {"type":"filter_genre","genre":"Action"}    (use "all" to clear)
6. highlight_kpi          -> {"type":"highlight_kpi","kpi":"total_revenue"}
7. set_background         -> {"type":"set_background","color":"<valid CSS color>"}
8. set_chart_type         -> {"type":"set_chart_type","chart":"monthly_revenue"|"monthly_revenue_per_store"|"monthly_rental_trend","chart_type":"line"|"bar"|"area"|"scatter"}
9. render_custom_chart    -> {"type":"render_custom_chart","title":"<short title>","spec":{"chart_type":"bar"|"line"|"area"|"scatter"|"pie"|"donut","dimension":"<allowed_dimension>","metric":"<allowed_metric>","series":"<allowed_dimension>|null","limit":3..30,"sort":"asc"|"desc","filters":{"genre":"Action","rating":"PG"}}}
10. delete_ai_output      -> {"type":"delete_ai_output","target":"latest"|"<chart title>"|"<chart id>"}
11. clear_ai_outputs      -> {"type":"clear_ai_outputs"}
12. update_ai_chart       -> {"type":"update_ai_chart","target":"latest"|"<chart title>"|"<chart id>","chart_type":"bar"|"line"|"area"|"scatter"|"pie"|"donut","title":"<optional new title>"}
13. query_records         -> {"type":"query_records","table":"payment"|"rental"|"inventory"|"customer"|"film","filters":{"payment_id":17503},"limit":10}
14. mutate_records        -> {"type":"mutate_records","operation":"insert"|"update"|"delete","table":"payment"|"rental","filters":{"payment_id":17503},"values":{"amount":7.99}}

Use set_background when the user asks to change the page/background color,
for example "make the background green", "adjust the page color to white",
"ubah latar belakang jadi putih", "atur background ke ungu",
"把背景调成白色", or "把背景调为紫色".
The color can be any valid CSS
color, including CSS color names ("green", "lavender", "rebeccapurple"),
hex values ("#22c55e"), rgb()/rgba(), or hsl()/hsla(). Preserve the exact
color requested when possible.

Valid chart ids:
  - "top_rented_films"        params: {"limit": 5..30, "genre": "Action" | null}
  - "top_customers"           params: {"limit": 5..20}
  - "revenue_by_genre"        params: {}
  - "monthly_revenue"         params: {}
  - "monthly_revenue_per_store" params: {}
  - "genre_distribution"      params: {}
  - "rating_distribution"     params: {}
  - "actor_rental_count"      params: {"limit": 5..30}
  - "actor_film_count"        params: {"limit": 5..30}
  - "least_rented_films"      params: {"limit": 5..30}
  - "monthly_rental_trend"    params: {}
  - "rental_by_dow"           params: {}

For chart requests that are NOT covered by the valid chart ids above, use
render_custom_chart instead of refusing. The custom chart must still use only
dvdrental data and only these allowed dimensions/metrics:
{custom_chart_field_description()}
If the requested chart would require data outside dvdrental or outside the
allowed dimensions/metrics, do not emit any chart action for it.

Switch existing chart types with set_chart_type when the user asks to change
a chart into a bar/line/area/scatter chart. Examples:
"change monthly revenue to bar chart", "ubah grafik revenue jadi batang",
"把收入趋势图切换成柱状图", "把这个图改成面积图".
Use chart "monthly_revenue_per_store" for the Monthly Revenue Trend per Store chart.
Use chart "monthly_rental_trend" for the Monthly Rental Trend chart.

Use render_table, not plain text, whenever the user asks for a table.
Examples: "generate a table of top films", "show top customers as a table",
"生成表格", "用表格展示", "buat tabel", "tampilkan sebagai tabel".
Use the same ids and params as render_chart. The backend will attach rows.
If the table request is vague, default to top_rented_films with {"limit": 10}.

Use query_records when the user wants to directly inspect rows from the
database. Query results must be shown as tables on the dashboard, not only
inside chat text.

Allowed query tables:
- payment: payment_id, customer_id, staff_id, rental_id, amount, payment_date
- rental: rental_id, rental_date, inventory_id, customer_id, return_date, staff_id
- inventory: inventory_id, film_id, title, store_id, last_update
- customer: customer_id, first_name, last_name, email, store_id, activebool, city
- film: film_id, title, rating, rental_rate, rental_duration, length, release_year

If the user asks to query data outside these tables/fields, explain that the
requested data is not available through the current dvdrental query whitelist.

Use mutate_records only when the user explicitly wants to modify database data
that exists in dvdrental. Only these tables are allowed:
- payment: payment_id, customer_id, staff_id, rental_id, amount, payment_date
- rental: rental_id, rental_date, inventory_id, customer_id, return_date, staff_id

Rules for database mutations:
- Never invent tables or fields outside dvdrental.
- Never delete or update blindly. Always include at least one filter.
- If required fields are missing, explain what is missing instead of pretending success.
- If a referenced id does not exist, explain that reason.

If the user asks something outside the DVD Rental Dashboard scope, refuse to
answer and briefly say that you can only help with the DVD Rental dashboard.
Do NOT emit any dashboard action block for out-of-scope requests.

Always answer in the SAME LANGUAGE the user used.
Be concise. Cite specific numbers from the dashboard context when relevant.
"""

DASHBOARD_KEYWORDS = {
    "dashboard", "dvd", "film", "movie", "rental", "rent", "customer", "customers",
    "actor", "actors", "revenue", "genre", "rating", "payment", "payments", "store",
    "chart", "table", "prediction", "predict", "forecast", "machine", "learning",
    "ml", "kpi", "theme", "background", "color", "page", "website", "site", "ui",
    "filter", "scroll", "section", "overview", "popularity",
    "top", "least", "monthly", "language", "inventory", "disewa", "penyewaan",
    "database", "db", "record", "records", "delete", "remove", "insert", "add", "update", "modify",
    "purchase", "purchases", "transaction", "transactions",
    "pelanggan", "pendapatan", "aktor", "tema", "grafik", "tabel", "prediksi",
    "machine learning", "genre", "toko", "bagian", "pembelian", "transaksi", "hapus", "tambah", "ubah", "database dvdrental",
    "latar", "warna", "halaman", "situs", "film tersewa", "customer tertinggi",
    "revenue by genre", "rental rate", "background color", "page background",
    "bar", "line", "area", "scatter", "bar chart", "line chart", "area chart",
    "scatter chart", "batang", "garis", "kolom", "titik",
    "\u7535\u5f71", "\u5f71\u7247", "\u79df\u8d41", "\u51fa\u79df",
    "\u987e\u5ba2", "\u5ba2\u6237", "\u6536\u5165", "\u8425\u6536",
    "\u6f14\u5458", "\u7c7b\u578b", "\u8bc4\u5206", "\u4e3b\u9898",
    "\u56fe\u8868", "\u8868\u683c", "\u9884\u6d4b", "\u673a\u5668\u5b66\u4e60",
    "\u80cc\u666f", "\u80cc\u666f\u8272", "\u989c\u8272",
    "\u9875\u9762", "\u7f51\u7ad9", "\u7b5b\u9009", "\u8fc7\u6ee4",
    "\u4eea\u8868\u76d8", "\u770b\u677f", "\u6570\u636e", "\u6392\u884c",
    "\u7535\u5f71\u699c", "\u5ba2\u6237\u699c", "\u67f1\u72b6\u56fe",
    "\u67f1\u5f62\u56fe", "\u6761\u5f62\u56fe", "\u6298\u7ebf\u56fe",
    "\u7ebf\u56fe", "\u9762\u79ef\u56fe", "\u6563\u70b9\u56fe",
    "\u6570\u636e\u5e93", "\u8bb0\u5f55", "\u4ed8\u6b3e", "\u652f\u4ed8", "\u4ea4\u6613", "\u8d2d\u4e70", "\u5220\u9664", "\u6dfb\u52a0", "\u63d2\u5165", "\u66f4\u65b0"
}

ACTION_HINTS = {
    "show", "render", "display", "generate", "create", "change", "switch", "set", "make", "turn",
    "apply", "adjust", "update", "use", "paint", "choose", "filter", "scroll",
    "highlight", "delete", "remove", "insert", "add", "modify",
    "tampilkan", "ubah", "ganti", "jadikan", "buat", "atur", "hapus", "tambah",
    "setel", "terapkan", "gunakan", "pilih", "filter", "scroll", "sorot",
    "\u663e\u793a", "\u5c55\u793a", "\u5207\u6362", "\u6539\u6210",
    "\u6539\u4e3a", "\u7b5b\u9009", "\u8fc7\u6ee4", "\u6eda\u52a8",
    "\u9ad8\u4eae", "\u8bbe\u4e3a", "\u8bbe\u6210", "\u53d8\u6210",
    "\u8c03\u6210", "\u8c03\u4e3a", "\u8c03\u6574", "\u5f04\u6210",
    "\u6362\u6210", "\u6539\u4e3a", "\u5220\u9664", "\u63d2\u5165", "\u6dfb\u52a0", "\u66f4\u65b0"
}

FOLLOW_UP_HINTS = {
    "again", "same", "that", "those", "it", "them", "continue", "more", "another",
    "ulang", "lagi", "itu", "lanjut", "tambahkan", "\u518d\u6765",
    "\u7ee7\u7eed", "\u8fd8\u662f\u8fd9\u4e2a", "\u90a3\u4e2a", "\u5b83",
    "\u5b83\u4eec", "\u518d\u663e\u793a", "\u6362\u6210"
}

ASCII_DASHBOARD_TOKENS = {keyword for keyword in DASHBOARD_KEYWORDS if keyword.isascii() and " " not in keyword}
ASCII_DASHBOARD_PHRASES = {keyword for keyword in DASHBOARD_KEYWORDS if keyword.isascii() and " " in keyword}
NON_ASCII_DASHBOARD_KEYWORDS = {keyword for keyword in DASHBOARD_KEYWORDS if not keyword.isascii()}
ASCII_ACTION_HINTS = {hint for hint in ACTION_HINTS if hint.isascii()}
NON_ASCII_ACTION_HINTS = {hint for hint in ACTION_HINTS if not hint.isascii()}
ASCII_FOLLOW_UP_HINTS = {hint for hint in FOLLOW_UP_HINTS if hint.isascii()}
NON_ASCII_FOLLOW_UP_HINTS = {hint for hint in FOLLOW_UP_HINTS if not hint.isascii()}

DEVELOPER_KEYWORDS = {
    "source", "code", "coding", "file", "files", "edit", "modify", "update", "rewrite",
    "refactor", "fix", "bug", "implement", "feature", "frontend", "backend", "template",
    "style", "styles", "html", "css", "javascript", "js", "python", "fastapi",
    "title", "titles", "text", "copy", "label", "headline", "judul", "teks",
    "layout", "box", "boxes", "card", "cards", "widget", "widgets", "position",
    "order", "kpi box", "kpi boxes", "kpi card", "kpi cards", "ui layout",
    "page layout", "dashboard layout", "hero section", "button text",
    "swap", "reorder", "move", "relayout",
    "main.py", "index.html", "chat.js", "dashboard.js", "styles.css",
    "ubah kode", "ubah source", "ubah file", "edit file", "modify source", "source file",
    "source files", "update code", "change code", "implement feature", "fix bug",
    "page title", "site title", "website title", "dashboard title", "change title",
    "change text", "update text", "ubah judul", "ubah teks", "judul halaman",
    "tukar layout", "tukar box", "tukar posisi", "ubah layout", "ubah tata letak",
    "susun ulang", "ubah urutan", "pindah posisi", "pindahkan box", "kotak kpi",
    "tata letak", "posisi box", "urutan kpi",
    "\u4fee\u6539\u4ee3\u7801", "\u4fee\u6539\u6e90\u7801", "\u6e90\u6587\u4ef6",
    "\u4ee3\u7801", "\u6587\u4ef6", "\u524d\u7aef", "\u540e\u7aef", "\u5b9e\u73b0\u529f\u80fd",
    "\u4fee\u590d", "\u7f16\u8f91", "\u6837\u5f0f", "\u9875\u9762\u6587\u6848",
    "\u6807\u9898", "\u9875\u9762\u6807\u9898", "\u7f51\u7ad9\u6807\u9898", "\u4fee\u6539\u6807\u9898",
    "\u5e03\u5c40", "\u6392\u7248", "\u5361\u7247", "\u987a\u5e8f", "\u4ea4\u6362\u4f4d\u7f6e",
    "\u8c03\u6574\u5e03\u5c40", "\u4fee\u6539\u5e03\u5c40", "\u9875\u9762\u5e03\u5c40"
}

DEVELOPER_ACTION_HINTS = {
    "edit", "modify", "change", "update", "rewrite", "fix", "implement", "add",
    "remove", "create", "adjust", "refactor", "swap", "reorder", "move",
    "ubah", "ganti", "perbarui", "buat", "tukar", "pindah", "susun",
    "\u4fee\u6539", "\u6539", "\u66f4\u65b0", "\u65b0\u589e", "\u5220\u9664",
    "\u5b9e\u73b0", "\u8c03\u6574", "\u7f16\u8f91", "\u4ea4\u6362", "\u79fb\u52a8"
}

ASCII_DEVELOPER_TOKENS = {keyword for keyword in DEVELOPER_KEYWORDS if keyword.isascii() and " " not in keyword}
ASCII_DEVELOPER_PHRASES = {keyword for keyword in DEVELOPER_KEYWORDS if keyword.isascii() and " " in keyword}
NON_ASCII_DEVELOPER_KEYWORDS = {keyword for keyword in DEVELOPER_KEYWORDS if not keyword.isascii()}
ASCII_DEVELOPER_ACTION_HINTS = {hint for hint in DEVELOPER_ACTION_HINTS if hint.isascii()}
NON_ASCII_DEVELOPER_ACTION_HINTS = {hint for hint in DEVELOPER_ACTION_HINTS if not hint.isascii()}

PROJECT_DEVELOPER_CONTEXT = {
    "dashboard", "website", "site", "page", "ui", "frontend", "backend",
    "html", "css", "javascript", "js", "fastapi", "template", "theme",
    "background", "title", "layout", "card", "chart", "table",
    "main.py", "index.html", "chat.js", "dashboard.js", "styles.css",
    "halaman", "situs", "tema", "grafik", "tabel", "judul", "layout",
    "\u4eea\u8868\u76d8", "\u770b\u677f", "\u7f51\u7ad9", "\u9875\u9762",
    "\u524d\u7aef", "\u540e\u7aef", "\u6807\u9898", "\u5e03\u5c40",
    "\u80cc\u666f", "\u4e3b\u9898", "\u56fe\u8868", "\u8868\u683c",
}

ASCII_PROJECT_DEVELOPER_CONTEXT = {
    keyword for keyword in PROJECT_DEVELOPER_CONTEXT if keyword.isascii()
}
NON_ASCII_PROJECT_DEVELOPER_CONTEXT = {
    keyword for keyword in PROJECT_DEVELOPER_CONTEXT if not keyword.isascii()
}


def is_dashboard_related(message: str) -> bool:
    text = (message or "").strip()
    if not text:
        return False

    lowered = text.lower()
    tokens = set(re.findall(r"[a-zA-Z_]+", lowered))

    if any(keyword in lowered for keyword in ASCII_DASHBOARD_PHRASES):
        return True
    if ASCII_DASHBOARD_TOKENS.intersection(tokens):
        return True
    if any(keyword in text for keyword in NON_ASCII_DASHBOARD_KEYWORDS):
        return True

    has_action_hint = ASCII_ACTION_HINTS.intersection(tokens) or any(
        hint in text for hint in NON_ASCII_ACTION_HINTS
    )
    has_dashboard_noun = ASCII_DASHBOARD_TOKENS.intersection(tokens) or any(
        keyword in text for keyword in NON_ASCII_DASHBOARD_KEYWORDS
    )
    return bool(has_action_hint and has_dashboard_noun)


def is_developer_related(message: str) -> bool:
    text = (message or "").strip()
    if not text:
        return False

    lowered = text.lower()
    tokens = set(re.findall(r"[a-zA-Z_]+", lowered))

    if re.search(r"\b[\w./\\-]+\.(?:py|js|html|css|json|md|txt)\b", lowered):
        return True
    if any(keyword in lowered for keyword in ASCII_DEVELOPER_PHRASES):
        return True
    if ASCII_DEVELOPER_TOKENS.intersection(tokens):
        return True
    if any(keyword in text for keyword in NON_ASCII_DEVELOPER_KEYWORDS):
        return True

    has_dev_action = ASCII_DEVELOPER_ACTION_HINTS.intersection(tokens) or any(
        hint in text for hint in NON_ASCII_DEVELOPER_ACTION_HINTS
    )
    has_dev_noun = ASCII_DEVELOPER_TOKENS.intersection(tokens) or any(
        keyword in text for keyword in NON_ASCII_DEVELOPER_KEYWORDS
    )
    return bool(has_dev_action and has_dev_noun)


def is_project_developer_request(message: str) -> bool:
    if not is_developer_related(message):
        return False
    text = (message or "").strip()
    lowered = text.lower()
    if re.search(r"\b[\w./\\-]+\.(?:py|js|html|css|json|md|txt)\b", lowered):
        return True
    if any(keyword in lowered for keyword in ASCII_PROJECT_DEVELOPER_CONTEXT):
        return True
    if any(keyword in text for keyword in NON_ASCII_PROJECT_DEVELOPER_CONTEXT):
        return True
    return is_dashboard_related(message)


def choose_chat_mode(messages: List["ChatMessage"]) -> str:
    recent = messages[-6:]
    last_user = next((m.content for m in reversed(recent) if m.role == "user"), "")
    if is_project_developer_request(last_user):
        return "developer"
    if is_dashboard_related(last_user):
        return "dashboard"

    lowered = (last_user or "").lower()
    follow_up = ASCII_FOLLOW_UP_HINTS.intersection(set(re.findall(r"[a-zA-Z_]+", lowered))) or any(
        hint in last_user for hint in NON_ASCII_FOLLOW_UP_HINTS
    )
    if follow_up:
        for msg in reversed(recent[:-1]):
            if msg.role == "user" and is_project_developer_request(msg.content):
                return "developer"
            if msg.role == "user" and is_dashboard_related(msg.content):
                return "dashboard"

    return "general"


def build_system_prompt(mode: str) -> str:
    if mode == "general":
        return """
You are the AI assistant embedded inside a DVD Rental dashboard web app.

You must ONLY answer questions related to the DVD Rental dashboard, its data,
its charts/tables, its UI controls, or the dvdrental database actions exposed
through this app.

If the user asks anything outside that scope, refuse briefly in the SAME
LANGUAGE the user used, and do not emit any dashboard action block.
""".strip()

    if mode == "developer":
        return f"""
You are a coding assistant embedded inside a DVD Rental dashboard web app.
You can inspect and modify this project's source files to implement changes the
user requests for the current website.

Project root: {WORKSPACE_ROOT}
Allowed file types: {", ".join(sorted(EDITABLE_SOURCE_SUFFIXES))}

Rules:
- Use the available tools to inspect files before editing them.
- Prefer reading likely target files directly instead of calling list_source_files
  unless the user request is ambiguous.
- Stay strictly inside the project root.
- Make focused, minimal changes that satisfy the user's request.
- Preserve existing behavior unless the user asked to change it.
- If the user asks for code changes, you may write files directly.
- After finishing, explain what changed and name the files you modified.
- Always answer in the SAME LANGUAGE the user used.
- Do not emit any dashboard action block unless the user explicitly asks to
  control the live page right now.

Project context:
{build_developer_context()}
""".strip()

    return f"""
You are the AI assistant of a DVD Rental Interactive Dashboard. The user is
looking at a web page with KPIs, charts, and four sections (Overview,
Popularity, Actor, Revenue). You can ANSWER questions about the data AND
control the dashboard via the action protocol below.

{ACTION_SCHEMA}

Dashboard data snapshot (use these numbers, do not invent any):
{build_dashboard_context()}
""".strip()


def detect_user_language(text: str) -> str:
    raw = text or ""
    if re.search(r"[\u4e00-\u9fff]", raw):
        return "zh"
    lowered = raw.lower()
    indonesian_markers = {
        "apa", "siapa", "berapa", "tampilkan", "ubah", "ganti", "jadikan",
        "buat", "tolong", "dengan", "ke", "yang", "ini", "itu", "latar",
        "warna", "pelanggan", "pendapatan", "aktor", "grafik", "tabel",
    }
    tokens = set(re.findall(r"[a-zA-Z_]+", lowered))
    if indonesian_markers.intersection(tokens):
        return "id"
    return "en"


def out_of_scope_reply(text: str) -> Dict[str, Any]:
    lang = detect_user_language(text)
    if lang == "zh":
        reply = (
            "我只能回答和 DVD Rental 仪表盘相关的问题。你可以问："
            "按类型生成表格、显示收入图表、筛选 Action 电影、切换背景颜色。"
        )
    elif lang == "id":
        reply = (
            "Saya hanya bisa menjawab hal yang terkait dengan dashboard DVD Rental. "
            "Coba tanya: buat tabel top film, tampilkan grafik revenue, filter genre Action, atau ubah warna background."
        )
    else:
        reply = (
            "I can only answer questions related to the DVD Rental dashboard. "
            "Try: generate a top-films table, show a revenue chart, filter Action films, or change the background color."
        )
    return {"reply": reply, "actions": []}


def build_dashboard_context() -> str:
    """Compact JSON snapshot that the AI uses to ground its answers."""
    snap = {
        "kpi": {
            "overview": kpi_overview(),
            "popularity": kpi_popularity(),
            "actor": kpi_actor(),
            "revenue": kpi_revenue(),
        },
        "top_rented_films_top10": top_rented_films(10),
        "least_rented_films_top10": least_rented_films(10),
        "top_customers_top10": top_customers_by_spending(10),
        "revenue_by_genre": revenue_by_genre(),
        "genre_distribution": genre_distribution(),
        "monthly_revenue": monthly_revenue(),
        "actor_rental_count_top10": actor_rental_count(10),
        "actor_film_count_top10": actor_film_count(10),
        "rental_by_dow": rental_by_dow(),
    }
    return json.dumps(snap, ensure_ascii=False, separators=(",", ":"))


CUSTOM_CHART_DIMENSIONS: Dict[str, Dict[str, Any]] = {
    "genre": {"expr": "c.name", "label": "Genre", "kind": "category", "deps": ["film_category", "category"]},
    "rating": {"expr": "f.rating::text", "label": "Rating", "kind": "category", "deps": []},
    "film_title": {"expr": "f.title", "label": "Film Title", "kind": "category", "deps": []},
    "language": {"expr": "l.name", "label": "Language", "kind": "category", "deps": ["language"]},
    "store": {"expr": "'Store ' || i.store_id::text", "label": "Store", "kind": "category", "deps": ["inventory"]},
    "customer_name": {
        "expr": "TRIM(COALESCE(cu.first_name,'') || ' ' || COALESCE(cu.last_name,''))",
        "label": "Customer",
        "kind": "category",
        "deps": ["inventory", "rental", "customer"],
    },
    "actor_name": {
        "expr": "TRIM(COALESCE(a.first_name,'') || ' ' || COALESCE(a.last_name,''))",
        "label": "Actor",
        "kind": "category",
        "deps": ["film_actor", "actor"],
    },
    "rental_month": {
        "expr": "TO_CHAR(DATE_TRUNC('month', r.rental_date), 'YYYY-MM')",
        "label": "Rental Month",
        "kind": "time",
        "deps": ["inventory", "rental"],
    },
    "payment_month": {
        "expr": "TO_CHAR(DATE_TRUNC('month', p.payment_date), 'YYYY-MM')",
        "label": "Payment Month",
        "kind": "time",
        "deps": ["inventory", "rental", "payment"],
    },
    "rental_day_of_week": {
        "expr": "TRIM(TO_CHAR(r.rental_date, 'Day'))",
        "label": "Day of Week",
        "kind": "time",
        "deps": ["inventory", "rental"],
    },
}

CUSTOM_CHART_METRICS: Dict[str, Dict[str, Any]] = {
    "rental_count": {"expr": "COUNT(DISTINCT r.rental_id)", "label": "Rental Count", "deps": ["inventory", "rental"]},
    "revenue_sum": {"expr": "ROUND(COALESCE(SUM(p.amount), 0)::numeric, 2)", "label": "Revenue", "deps": ["inventory", "rental", "payment"]},
    "film_count": {"expr": "COUNT(DISTINCT f.film_id)", "label": "Film Count", "deps": []},
    "customer_count": {"expr": "COUNT(DISTINCT cu.customer_id)", "label": "Customer Count", "deps": ["inventory", "rental", "customer"]},
    "inventory_count": {"expr": "COUNT(DISTINCT i.inventory_id)", "label": "Inventory Count", "deps": ["inventory"]},
}

CUSTOM_CHART_ALLOWED_TYPES = {"bar", "line", "area", "scatter", "pie", "donut"}
CUSTOM_CHART_ALLOWED_FILTERS = {"genre", "rating", "language", "store"}

CUSTOM_CHART_JOIN_FRAGMENTS = {
    "film_category": "LEFT JOIN film_category fc ON f.film_id = fc.film_id",
    "category": "LEFT JOIN category c ON fc.category_id = c.category_id",
    "language": "LEFT JOIN language l ON f.language_id = l.language_id",
    "inventory": "LEFT JOIN inventory i ON f.film_id = i.film_id",
    "rental": "LEFT JOIN rental r ON i.inventory_id = r.inventory_id",
    "payment": "LEFT JOIN payment p ON r.rental_id = p.rental_id",
    "customer": "LEFT JOIN customer cu ON r.customer_id = cu.customer_id",
    "film_actor": "LEFT JOIN film_actor fa ON f.film_id = fa.film_id",
    "actor": "LEFT JOIN actor a ON fa.actor_id = a.actor_id",
}
CUSTOM_CHART_JOIN_ORDER = [
    "film_category",
    "category",
    "language",
    "inventory",
    "rental",
    "payment",
    "customer",
    "film_actor",
    "actor",
]

DAY_OF_WEEK_ORDER = {
    "Monday": 1,
    "Tuesday": 2,
    "Wednesday": 3,
    "Thursday": 4,
    "Friday": 5,
    "Saturday": 6,
    "Sunday": 7,
}


def custom_chart_field_description() -> str:
    dims = ", ".join(
        f'{key} ({meta["label"]})' for key, meta in CUSTOM_CHART_DIMENSIONS.items()
    )
    metrics = ", ".join(
        f'{key} ({meta["label"]})' for key, meta in CUSTOM_CHART_METRICS.items()
    )
    return f"Dimensions: {dims}. Metrics: {metrics}."


ACTION_SCHEMA = ACTION_SCHEMA.replace(
    "{custom_chart_field_description()}",
    custom_chart_field_description(),
)


def normalize_custom_chart_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    raw = spec or {}
    chart_type = str(raw.get("chart_type") or raw.get("type") or "bar").lower()
    chart_type = chart_type if chart_type in CUSTOM_CHART_ALLOWED_TYPES else "bar"

    dimension = str(raw.get("dimension") or "").strip()
    metric = str(raw.get("metric") or "").strip()
    series = str(raw.get("series") or "").strip() or None

    if dimension not in CUSTOM_CHART_DIMENSIONS:
        raise ValueError(f"Unsupported dimension: {dimension}")
    if metric not in CUSTOM_CHART_METRICS:
        raise ValueError(f"Unsupported metric: {metric}")
    if series:
        if series not in CUSTOM_CHART_DIMENSIONS:
            raise ValueError(f"Unsupported series: {series}")
        if series == dimension:
            raise ValueError("Series must be different from dimension")
    if chart_type in {"pie", "donut"}:
        series = None

    try:
        limit = int(raw.get("limit", 10))
    except Exception:
        limit = 10
    limit = max(3, min(30, limit))

    sort = str(raw.get("sort") or "").lower()
    if sort not in {"asc", "desc"}:
        sort = "asc" if CUSTOM_CHART_DIMENSIONS[dimension]["kind"] == "time" else "desc"

    filters = raw.get("filters") if isinstance(raw.get("filters"), dict) else {}
    safe_filters = {}
    for key, value in filters.items():
        if key not in CUSTOM_CHART_ALLOWED_FILTERS:
            continue
        if value is None:
            continue
        text_value = str(value).strip()
        if text_value:
            safe_filters[key] = text_value[:80]

    return {
        "chart_type": chart_type,
        "dimension": dimension,
        "metric": metric,
        "series": series,
        "limit": limit,
        "sort": sort,
        "filters": safe_filters,
    }


def custom_chart_order_expr(dimension: str) -> str:
    if dimension == "rental_day_of_week":
        return (
            "CASE x "
            "WHEN 'Monday' THEN 1 WHEN 'Tuesday' THEN 2 WHEN 'Wednesday' THEN 3 "
            "WHEN 'Thursday' THEN 4 WHEN 'Friday' THEN 5 WHEN 'Saturday' THEN 6 "
            "WHEN 'Sunday' THEN 7 ELSE 99 END"
        )
    return "x"


def build_custom_chart_title(spec: Dict[str, Any]) -> str:
    dim_label = CUSTOM_CHART_DIMENSIONS[spec["dimension"]]["label"]
    metric_label = CUSTOM_CHART_METRICS[spec["metric"]]["label"]
    title = f"{metric_label} by {dim_label}"
    if spec.get("series"):
        title += f" by {CUSTOM_CHART_DIMENSIONS[spec['series']]['label']}"
    if spec.get("filters"):
        filter_text = ", ".join(f"{k}={v}" for k, v in spec["filters"].items())
        title += f" ({filter_text})"
    return title


def custom_chart_from_sql(spec: Dict[str, Any]) -> str:
    needed = set(CUSTOM_CHART_DIMENSIONS[spec["dimension"]]["deps"])
    needed.update(CUSTOM_CHART_METRICS[spec["metric"]]["deps"])
    if spec.get("series"):
        needed.update(CUSTOM_CHART_DIMENSIONS[spec["series"]]["deps"])
    for filter_key in spec.get("filters", {}).keys():
        needed.update(CUSTOM_CHART_DIMENSIONS[filter_key]["deps"])

    joins = ["FROM film f"]
    for join_name in CUSTOM_CHART_JOIN_ORDER:
        if join_name in needed:
            joins.append(CUSTOM_CHART_JOIN_FRAGMENTS[join_name])
    return "\n        ".join(joins)


def custom_chart_payload(spec: Dict[str, Any], title: Optional[str] = None) -> Dict[str, Any]:
    normalized = normalize_custom_chart_spec(spec)
    dim_meta = CUSTOM_CHART_DIMENSIONS[normalized["dimension"]]
    metric_meta = CUSTOM_CHART_METRICS[normalized["metric"]]
    series_meta = CUSTOM_CHART_DIMENSIONS.get(normalized["series"]) if normalized.get("series") else None

    select_bits = [f"{dim_meta['expr']} AS x", f"{metric_meta['expr']} AS y"]
    group_bits = [dim_meta["expr"]]
    order_bits = []

    if series_meta:
        select_bits.append(f"{series_meta['expr']} AS series")
        group_bits.append(series_meta["expr"])
        order_bits.append("series ASC")
    else:
        select_bits.append("NULL::text AS series")

    where_bits = [f"{dim_meta['expr']} IS NOT NULL"]
    params: Dict[str, Any] = {}

    if normalized["metric"] in {"rental_count", "revenue_sum", "customer_count"}:
        where_bits.append("r.rental_id IS NOT NULL")
    if normalized["metric"] == "revenue_sum":
        where_bits.append("p.payment_id IS NOT NULL")

    filter_field_map = {
        "genre": "c.name",
        "rating": "f.rating::text",
        "language": "l.name",
        "store": "'Store ' || i.store_id::text",
    }
    for idx, (key, value) in enumerate(normalized["filters"].items(), start=1):
        where_bits.append(f"{filter_field_map[key]} ILIKE :filter_{idx}")
        params[f"filter_{idx}"] = value

    order_target = custom_chart_order_expr(normalized["dimension"])
    if normalized["sort"] == "desc":
        order_bits = [f"y DESC", f"{order_target} ASC", *order_bits]
    else:
        order_bits = [f"{order_target} ASC", *order_bits]

    limit_clause = ""
    if dim_meta["kind"] != "time":
        limit_clause = "LIMIT :limit_value"
        params["limit_value"] = normalized["limit"]

    sql = f"""
        SELECT {", ".join(select_bits)}
        {custom_chart_from_sql(normalized)}
        WHERE {" AND ".join(where_bits)}
        GROUP BY {", ".join(group_bits)}
        ORDER BY {", ".join(order_bits)}
        {limit_clause}
    """

    rows = df_to_records(query(sql, params))
    spec_out = {
        **normalized,
        "dimension_label": dim_meta["label"],
        "metric_label": metric_meta["label"],
        "series_label": series_meta["label"] if series_meta else None,
    }
    return {
        "chart": "custom",
        "spec": spec_out,
        "title": title or build_custom_chart_title(spec_out),
        "data": rows,
    }


def list_source_files() -> List[Dict[str, Any]]:
    files = []
    for path in sorted(WORKSPACE_ROOT.rglob("*")):
        if not path.is_file():
            continue
        if "__pycache__" in path.parts:
            continue
        if path.suffix.lower() not in EDITABLE_SOURCE_SUFFIXES:
            continue
        rel = path.relative_to(WORKSPACE_ROOT).as_posix()
        files.append({"path": rel, "size": path.stat().st_size})
    return files


def build_developer_context() -> str:
    files = [item["path"] for item in list_source_files()]
    common = ", ".join(COMMON_EDIT_TARGETS)
    compact_files = ", ".join(files)
    return (
        f"Common edit targets: {common}\n"
        f"Project files: {compact_files}"
    )


def resolve_source_path(path_str: str) -> Path:
    raw = (path_str or "").strip()
    if not raw:
        raise ValueError("path is required")

    candidate = Path(raw)
    if not candidate.is_absolute():
        candidate = (WORKSPACE_ROOT / candidate).resolve()
    else:
        candidate = candidate.resolve()

    try:
        candidate.relative_to(WORKSPACE_ROOT)
    except ValueError as exc:
        raise ValueError("path must stay inside the project root") from exc

    if candidate.suffix.lower() not in EDITABLE_SOURCE_SUFFIXES:
        raise ValueError(f"unsupported file type: {candidate.suffix or '<none>'}")

    return candidate


def read_source_file(path: str, start_line: int = 1, end_line: Optional[int] = None) -> Dict[str, Any]:
    target = resolve_source_path(path)
    if not target.exists():
        raise ValueError(f"file not found: {path}")

    content = target.read_text(encoding="utf-8")
    lines = content.splitlines()
    total_lines = len(lines)
    start = max(int(start_line or 1), 1)
    end = total_lines if end_line in (None, 0) else min(int(end_line), total_lines)
    if end < start:
        raise ValueError("end_line must be greater than or equal to start_line")

    snippet = "\n".join(lines[start - 1:end])
    if len(snippet) > MAX_SOURCE_FILE_CHARS:
        raise ValueError("requested range is too large")

    return {
        "path": target.relative_to(WORKSPACE_ROOT).as_posix(),
        "start_line": start,
        "end_line": end,
        "total_lines": total_lines,
        "content": snippet,
    }


def write_source_file(path: str, content: str) -> Dict[str, Any]:
    target = resolve_source_path(path)
    body = content or ""
    if len(body) > MAX_SOURCE_FILE_CHARS:
        raise ValueError("file content is too large")

    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(body, encoding="utf-8")
    return {
        "path": target.relative_to(WORKSPACE_ROOT).as_posix(),
        "bytes_written": len(body.encode("utf-8")),
        "status": "written",
    }


DEVELOPER_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "list_source_files",
            "description": "List editable source files inside the current dashboard project.",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_source_file",
            "description": "Read a source file or a specific line range from the current project.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Project-relative path such as templates/index.html"},
                    "start_line": {"type": "integer", "minimum": 1, "description": "1-based inclusive start line"},
                    "end_line": {"type": "integer", "minimum": 1, "description": "1-based inclusive end line"},
                },
                "required": ["path"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "write_source_file",
            "description": "Write the full contents of a source file inside the current project.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Project-relative path such as static/js/chat.js"},
                    "content": {"type": "string", "description": "Complete file content to write"},
                },
                "required": ["path", "content"],
                "additionalProperties": False,
            },
        },
    },
]


def execute_developer_tool(name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    if name == "list_source_files":
        return {"files": list_source_files()}
    if name == "read_source_file":
        return read_source_file(
            arguments.get("path", ""),
            arguments.get("start_line", 1),
            arguments.get("end_line"),
        )
    if name == "write_source_file":
        return write_source_file(arguments.get("path", ""), arguments.get("content", ""))
    raise ValueError(f"unknown tool: {name}")


def developer_chat_reply(client: Any, req: ChatRequest) -> Dict[str, Any]:
    msgs: List[Dict[str, Any]] = [{"role": "system", "content": build_system_prompt("developer")}]
    for m in req.messages[-MAX_DEVELOPER_HISTORY:]:
        msgs.append({"role": m.role, "content": m.content})

    for _ in range(MAX_TOOL_ROUNDS):
        response = client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=msgs,
            tools=DEVELOPER_TOOLS,
            tool_choice="auto",
            temperature=0.1,
            max_tokens=1400,
        )

        assistant_msg = response.choices[0].message
        tool_calls = getattr(assistant_msg, "tool_calls", None) or []
        assistant_payload: Dict[str, Any] = {
            "role": "assistant",
            "content": assistant_msg.content or "",
        }
        if tool_calls:
            assistant_payload["tool_calls"] = [
                {
                    "id": tool_call.id,
                    "type": "function",
                    "function": {
                        "name": tool_call.function.name,
                        "arguments": tool_call.function.arguments,
                    },
                }
                for tool_call in tool_calls
            ]
        msgs.append(assistant_payload)

        if not tool_calls:
            return {"reply": (assistant_msg.content or "").strip(), "actions": []}

        for tool_call in tool_calls:
            try:
                args = json.loads(tool_call.function.arguments or "{}")
            except json.JSONDecodeError:
                result = {"error": "invalid JSON arguments"}
            else:
                try:
                    result = execute_developer_tool(tool_call.function.name, args)
                except Exception as exc:
                    result = {"error": str(exc)}

            msgs.append({
                "role": "tool",
                "tool_call_id": tool_call.id,
                "content": json.dumps(result, ensure_ascii=False),
            })

    return {
        "reply": "I reached the tool-call limit before finishing the code task. Please ask me to continue.",
        "actions": [],
    }


def chart_payload(chart: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Resolve a chart action into actual data the frontend can render."""
    p = params or {}
    try:
        if chart == "top_rented_films":
            return {"chart": chart, "data": top_rented_films(p.get("limit", 10), p.get("genre"))}
        if chart == "top_customers":
            return {"chart": chart, "data": top_customers_by_spending(p.get("limit", 10))}
        if chart == "revenue_by_genre":
            return {"chart": chart, "data": revenue_by_genre()}
        if chart == "monthly_revenue":
            return {"chart": chart, "data": monthly_revenue()}
        if chart == "monthly_revenue_per_store":
            return {"chart": chart, "data": monthly_revenue_per_store()}
        if chart == "genre_distribution":
            return {"chart": chart, "data": genre_distribution()}
        if chart == "rating_distribution":
            return {"chart": chart, "data": rating_distribution()}
        if chart == "actor_rental_count":
            return {"chart": chart, "data": actor_rental_count(p.get("limit", 10))}
        if chart == "actor_film_count":
            return {"chart": chart, "data": actor_film_count(p.get("limit", 10))}
        if chart == "least_rented_films":
            return {"chart": chart, "data": least_rented_films(p.get("limit", 10))}
        if chart == "monthly_rental_trend":
            return {"chart": chart, "data": monthly_rental_trend()}
        if chart == "rental_by_dow":
            return {"chart": chart, "data": rental_by_dow()}
    except Exception as exc:
        return {"chart": chart, "error": str(exc)}
    return None


def table_payload(table: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    payload = chart_payload(table, params)
    if not payload:
        return None
    data = payload.get("data", [])
    return {
        "table": table,
        "data": data,
        "error": payload.get("error"),
    }


def requested_limit(text: str, default: int = 10) -> int:
    match = re.search(r"\b(?:top\s*)?(\d{1,2})\b", text or "", re.IGNORECASE)
    if not match:
        return default
    return max(5, min(30, int(match.group(1))))


INTENT_REPLACEMENTS = [
    (r"\bmovies?\b", " film "),
    (r"\bjudul\b", " film "),
    (r"\bpelanggan\b", " customer "),
    (r"\bcustomers?\b", " customer "),
    (r"\baktor\b", " actor "),
    (r"\bactors?\b", " actor "),
    (r"\bpendapatan\b", " revenue "),
    (r"\bincome\b", " revenue "),
    (r"\bpayments?\b", " revenue "),
    (r"\bpurchases?\b", " payment "),
    (r"\btransactions?\b", " payment "),
    (r"\bpembelian\b", " payment "),
    (r"\btransaksi\b", " payment "),
    (r"\bgrafik\b", " chart "),
    (r"\bgraphs?\b", " chart "),
    (r"\bplots?\b", " chart "),
    (r"\bdiagram\b", " chart "),
    (r"\bvisuali[sz]e\b", " chart "),
    (r"\btabel\b", " table "),
    (r"\btabular\b", " table "),
    (r"\bbatang\b", " bar "),
    (r"\bkolom\b", " bar "),
    (r"\bcolumn\b", " bar "),
    (r"\bgaris\b", " line "),
    (r"\btitik\b", " scatter "),
    (r"\bsebar\b", " scatter "),
    (r"\bbulanan\b", " monthly "),
    (r"\bbulan\b", " monthly "),
    (r"\bmingguan\b", " weekly "),
    (r"\bterakhir\b", " latest "),
    (r"\bthat\b", " existing "),
    (r"\bthis\b", " existing "),
    (r"\bitu\b", " existing "),
    (r"\bini\b", " existing "),
    (r"\bsekarang\b", " current "),
    (r"\byang ada\b", " existing "),
    (r"\bganti\b", " change "),
    (r"\bubah\b", " change "),
    (r"\bjadikan\b", " change "),
    (r"\brenta\b", " rental "),
    (r"\bhapus\b", " delete "),
    (r"\btambah\b", " insert "),
    (r"\bperbarui\b", " update "),
    (r"\bdelete\b", " delete "),
    (r"\bremove\b", " delete "),
    (r"\badd\b", " insert "),
    (r"\binsert\b", " insert "),
    (r"\bmodify\b", " update "),
    (r"\bbikin\b", " create "),
    (r"\btampilkan\b", " show "),
    (r"\bbuatkan\b", " create "),
    (r"\bgenerated?\b", " generate "),
]

INTENT_REPLACEMENTS.extend([
    (r"查询", " show "),
    (r"查一下", " show "),
    (r"查找", " show "),
    (r"查看", " show "),
    (r"查", " show "),
])


def normalize_intent_text(text: str) -> str:
    raw = str(text or "").lower()
    normalized = raw
    for pattern, replacement in INTENT_REPLACEMENTS:
        normalized = re.sub(pattern, replacement, normalized, flags=re.IGNORECASE)
    normalized = normalized.replace("图表", " chart ").replace("表格", " table ")
    normalized = normalized.replace("柱状图", " bar ").replace("条形图", " bar ")
    normalized = normalized.replace("折线图", " line ").replace("面积图", " area ").replace("散点图", " scatter ")
    normalized = normalized.replace("收入", " revenue ").replace("营收", " revenue ")
    normalized = normalized.replace("购买", " payment ").replace("购买记录", " payment ")
    normalized = normalized.replace("支付", " payment ").replace("支付记录", " payment ")
    normalized = normalized.replace("付款", " payment ").replace("付款记录", " payment ")
    normalized = normalized.replace("交易", " payment ").replace("交易记录", " payment ")
    normalized = normalized.replace("电影", " film ").replace("演员", " actor ").replace("客户", " customer ")
    normalized = normalized.replace("租赁", " rental ").replace("出租", " rental ").replace("每月", " monthly ")
    normalized = normalized.replace("数据库", " database ").replace("记录", " record ")
    normalized = normalized.replace("删除", " delete ").replace("添加", " insert ").replace("新增", " insert ").replace("更新", " update ").replace("修改", " update ")
    normalized = re.sub(r"[^a-z0-9\u4e00-\u9fff#]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def intent_tokens(text: str) -> set[str]:
    normalized = normalize_intent_text(text)
    return set(re.findall(r"[a-z0-9_]+", normalized))


def wants_table(text: str) -> bool:
    normalized = normalize_intent_text(text)
    return "table" in intent_tokens(normalized) or "表格" in (text or "")


def requested_chart_type(text: str) -> Optional[str]:
    raw = text or ""
    normalized = normalize_intent_text(text)
    tokens = intent_tokens(normalized)
    if "bar" in tokens:
        return "bar"
    if "line" in tokens:
        return "line"
    if "area" in tokens:
        return "area"
    if "scatter" in tokens or "point" in tokens or "points" in tokens:
        return "scatter"
    return None


def requested_unsupported_chart_type(text: str) -> Optional[str]:
    raw = text or ""
    normalized = normalize_intent_text(text)
    if re.search(r"\b(pie|donut|doughnut)\b", normalized) or "\u997c\u56fe" in raw or "\u73af\u5f62\u56fe" in raw:
        return "pie"
    return None


def requested_chart_target(text: str) -> str:
    tokens = intent_tokens(text)
    if "revenue" in tokens and "monthly" in tokens and "store" not in tokens:
        return "monthly_revenue"
    if "rental" in tokens:
        return "monthly_rental_trend"
    if "revenue" in tokens:
        return "monthly_revenue_per_store"
    return "monthly_revenue_per_store"


def wants_chart_type_switch(text: str) -> bool:
    normalized = normalize_intent_text(text)
    tokens = intent_tokens(normalized)
    if not requested_chart_type(text):
        return False
    return bool({"change", "switch", "set", "make", "turn", "convert", "modify", "edit", "use"}.intersection(tokens)) or any(
        hint in (text or "") for hint in ["切换", "改成", "改为", "调整", "换成"]
    )


def wants_chart_request(text: str) -> bool:
    raw = text or ""
    normalized = normalize_intent_text(text)
    tokens = intent_tokens(normalized)
    if wants_table(raw):
        return False
    chart_words = {"chart", "graph", "plot", "diagram"}
    action_words = {"show", "display", "render", "generate", "create", "draw", "build", "need", "want", "give", "list"}
    metric_words = {"revenue", "genre", "rating", "actor", "customer", "film", "rental", "store", "monthly", "trend", "distribution", "top", "least", "language"}
    has_chart_word = bool(chart_words.intersection(tokens)) or any(hint in raw for hint in ["图", "图表", "统计图"])
    has_action_word = bool(action_words.intersection(tokens)) or any(hint in raw for hint in ["生成", "显示", "做一个", "画一个"])
    has_metric_word = bool(metric_words.intersection(tokens)) or any(hint in raw for hint in ["收入", "电影", "演员", "客户", "租赁", "评分", "类型"])
    return has_chart_word or (has_action_word and has_metric_word)


def extract_requested_genre(text: str) -> Optional[str]:
    lowered = (text or "").lower().replace("-", " ")
    try:
        for row in genre_distribution():
            genre = str(row.get("genre") or "").strip()
            if genre and genre.lower().replace("-", " ") in lowered:
                return genre
    except Exception:
        return None
    return None


def requested_chart_params(chart_id: str, text: str) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    if chart_id in {"top_rented_films", "top_customers", "actor_rental_count", "actor_film_count", "least_rented_films"}:
        params["limit"] = requested_limit(text, 10)
    if chart_id == "top_rented_films":
        genre = extract_requested_genre(text)
        if genre:
            params["genre"] = genre
    return params


def requested_chart_title(chart_id: str, text: str) -> Optional[str]:
    limit = requested_limit(text, 10)
    genre = extract_requested_genre(text) if chart_id == "top_rented_films" else None
    titles = {
        "top_rented_films": f"Top {limit} Rented Films - {genre}" if genre else f"Top {limit} Rented Films",
        "top_customers": f"Top {limit} Customers by Spending",
        "revenue_by_genre": "Revenue by Genre",
        "monthly_revenue": "Monthly Revenue",
        "monthly_revenue_per_store": "Monthly Revenue per Store",
        "genre_distribution": "Genre Distribution",
        "rating_distribution": "Rating Distribution",
        "actor_rental_count": f"Top {limit} Actors by Rental Count",
        "actor_film_count": f"Top {limit} Actors by Film Count",
        "least_rented_films": f"Bottom {limit} Least Rented Films",
        "monthly_rental_trend": "Monthly Rental Trend",
        "rental_by_dow": "Rental by Day of Week",
    }
    return titles.get(chart_id)


def infer_explicit_chart_id(text: str) -> Optional[str]:
    raw = text or ""
    tokens = intent_tokens(text)

    if "revenue" in tokens:
        if "store" in tokens:
            return "monthly_revenue_per_store"
        if "monthly" in tokens or "trend" in tokens:
            return "monthly_revenue"
        if "genre" in tokens or "category" in tokens:
            return "revenue_by_genre"

    if "rental" in tokens:
        if "monthly" in tokens or "trend" in tokens:
            return "monthly_rental_trend"
        if "day" in tokens or "weekday" in tokens or "hari" in tokens:
            return "rental_by_dow"

    if ({"top", "most", "popular"}.intersection(tokens) or "paling" in tokens) and "film" in tokens:
        return "top_rented_films"
    if ({"least", "bottom", "terendah"}.intersection(tokens) or ("paling" in tokens and "sedikit" in tokens)) and "film" in tokens:
        return "least_rented_films"
    if "customer" in tokens:
        return "top_customers"
    if "actor" in tokens:
        if "film" in tokens:
            return "actor_film_count"
        return "actor_rental_count"
    if "rating" in tokens:
        return "rating_distribution"
    if "genre" in tokens or "category" in tokens:
        return "genre_distribution"

    return None


def wants_clear_ai_outputs_request(text: str) -> bool:
    tokens = intent_tokens(text)
    return bool({"clear", "remove", "delete", "hapus"}.intersection(tokens) and {"all", "semua"}.intersection(tokens) and {"chart", "table", "output"}.intersection(tokens))


def wants_delete_ai_output_request(text: str) -> bool:
    tokens = intent_tokens(text)
    return bool({"remove", "delete", "hapus", "close"}.intersection(tokens) and {"chart", "table", "output"}.intersection(tokens))


def requested_ai_output_target(text: str) -> str:
    tokens = intent_tokens(text)
    if {"all", "semua"}.intersection(tokens):
        return "all"
    if {"first", "pertama"}.intersection(tokens):
        return "first"
    if {"this", "ini", "current", "existing", "latest", "last", "terakhir"}.intersection(tokens):
        return "latest"
    return "latest"


def wants_modify_ai_chart_request(text: str) -> bool:
    raw = text or ""
    tokens = intent_tokens(text)
    requested_type = requested_chart_type(text) or requested_unsupported_chart_type(text)
    if not requested_type:
        return False

    has_change_word = bool({"change", "switch", "set", "make", "turn", "convert", "modify", "edit", "use"}.intersection(tokens)) or any(
        hint in raw for hint in ["切换", "改成", "改为", "调整", "换成"]
    )
    has_reference = bool({"latest", "last", "existing", "current", "terakhir", "ini", "this"}.intersection(tokens)) or any(
        hint in raw for hint in ["这张图", "那个图", "這張圖", "那張圖"]
    )
    has_chart_word = bool({"chart", "graph", "plot"}.intersection(tokens)) or any(
        hint in raw for hint in ["图", "图表", "chart"]
    )
    return has_change_word and (has_reference or has_chart_word)


def infer_db_mutation_operation(text: str) -> Optional[str]:
    raw = normalize_intent_text(text)
    tokens = intent_tokens(raw)
    for operation, aliases in DB_MUTATION_OPERATION_ALIASES.items():
        if aliases.intersection(tokens) or any(alias in raw for alias in aliases):
            return operation
    return None


def infer_db_mutation_table(text: str) -> Optional[str]:
    raw = normalize_intent_text(text)
    tokens = intent_tokens(raw)
    for table_name, aliases in DB_MUTATION_TABLE_ALIASES.items():
        if table_name in tokens or aliases.intersection(tokens) or any(alias in raw for alias in aliases):
            return table_name
    for table_name, field_map in DB_MUTATION_FIELD_ALIASES.items():
        for aliases in field_map.values():
            if aliases.intersection(tokens) or any(alias in raw for alias in aliases):
                return table_name
    return None


def wants_db_mutation_request(text: str) -> bool:
    return bool(infer_db_mutation_operation(text) and infer_db_mutation_table(text))


def extract_datetime_from_text(text: str, aliases: set[str]) -> Optional[str]:
    raw = str(text or "")
    alias_pattern = "|".join(re.escape(alias) for alias in sorted(aliases, key=len, reverse=True))
    patterns = [
        rf"(?:{alias_pattern})\s*(?:=|:|is|at|to|for|为|成|jadi|menjadi)?\s*(\d{{4}}-\d{{1,2}}-\d{{1,2}}(?:[ T]\d{{1,2}}:\d{{2}}(?::\d{{2}})?)?)",
        rf"(\d{{4}}-\d{{1,2}}-\d{{1,2}}(?:[ T]\d{{1,2}}:\d{{2}}(?::\d{{2}})?)?)\s*(?:for|as|on)?\s*(?:{alias_pattern})",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return None


def extract_number_from_text(text: str, aliases: set[str], *, allow_decimal: bool = False) -> Optional[float]:
    raw = str(text or "")
    alias_pattern = "|".join(re.escape(alias) for alias in sorted(aliases, key=len, reverse=True))
    number_pattern = r"(-?\d+(?:\.\d+)?)" if allow_decimal else r"(-?\d+)"
    patterns = [
        rf"(?:{alias_pattern})\s*(?:=|:|is|to|for|as|为|成|jadi|menjadi)?\s*{number_pattern}",
        rf"{number_pattern}\s*(?:for|as)?\s*(?:{alias_pattern})",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw, flags=re.IGNORECASE)
        if match:
            value = match.group(1).strip()
            return float(value) if allow_decimal else int(value)
    return None


def extract_number_list_from_text(text: str, aliases: set[str]) -> List[int]:
    raw = str(text or "")
    alias_pattern = "|".join(re.escape(alias) for alias in sorted(aliases, key=len, reverse=True))
    if not alias_pattern:
        return []
    range_patterns = [
        rf"(?:{alias_pattern})s?\s*(?:=|:|is|are|for|in)?\s*(\d+)\s*(?:-|to|through|until|sampai|hingga|到|至)\s*(\d+)",
        rf"(\d+)\s*(?:-|to|through|until|sampai|hingga|到|至)\s*(\d+)\s*(?:for|as)?\s*(?:{alias_pattern})s?",
    ]
    for pattern in range_patterns:
        match = re.search(pattern, raw, flags=re.IGNORECASE)
        if not match:
            continue
        start = int(match.group(1))
        end = int(match.group(2))
        if start == end:
            return [start]
        if end < start:
            start, end = end, start
        if end - start > 200:
            return []
        return list(range(start, end + 1))
    patterns = [
        rf"(?:{alias_pattern})s?\s*(?:=|:|is|are|for|in)?\s*((?:\d+\s*(?:,|，|、|;|/|\bor\b|\band\b|\s+))+?\d+)",
        rf"((?:\d+\s*(?:,|，|、|;|/|\bor\b|\band\b|\s+))+?\d+)\s*(?:for|as)?\s*(?:{alias_pattern})s?",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw, flags=re.IGNORECASE)
        if not match:
            continue
        numbers = re.findall(r"\d+", match.group(1))
        if len(numbers) >= 2:
            return [int(value) for value in numbers]
    return []


def extract_record_range_from_text(text: str, table_name: str) -> List[int]:
    raw = str(text or "")
    aliases = DB_MUTATION_TABLE_ALIASES.get(table_name, set()) | {table_name, "record", "records", "号", "条"}
    alias_pattern = "|".join(re.escape(alias) for alias in sorted(aliases, key=len, reverse=True))
    if not alias_pattern:
        return []
    patterns = [
        rf"(?:第\s*)?(\d+)\s*(?:-|到|至|to|through|until|sampai|hingga)\s*(?:第\s*)?(\d+)\s*(?:号|条)?\s*(?:{alias_pattern})",
        rf"(?:{alias_pattern})\s*(?:id\s*)?(?:第\s*)?(\d+)\s*(?:-|到|至|to|through|until|sampai|hingga)\s*(?:第\s*)?(\d+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw, flags=re.IGNORECASE)
        if not match:
            continue
        start = int(match.group(1))
        end = int(match.group(2))
        if end < start:
            start, end = end, start
        if end - start > 200:
            return []
        return list(range(start, end + 1))
    return []


def extract_text_value_from_text(text: str, aliases: set[str]) -> Optional[str]:
    raw = str(text or "")
    alias_pattern = "|".join(re.escape(alias) for alias in sorted(aliases, key=len, reverse=True))
    quoted_patterns = [
        rf"(?:{alias_pattern})\s*(?:=|:|is|for|as|named|titled|为|叫|jadi|bernama)?\s*[\"“']([^\"”']+)[\"”']",
        rf"[\"“']([^\"”']+)[\"”']\s*(?:for|as|on)?\s*(?:{alias_pattern})",
    ]
    for pattern in quoted_patterns:
        match = re.search(pattern, raw, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()

    plain_patterns = [
        rf"(?:{alias_pattern})\s*(?:=|:|is|for|as|named|titled|为|叫|jadi|bernama)\s*([A-Za-z][A-Za-z0-9 .,'-]{{1,80}})",
    ]
    for pattern in plain_patterns:
        match = re.search(pattern, raw, flags=re.IGNORECASE)
        if match:
            candidate = match.group(1).strip()
            candidate = re.split(r"\b(and|with|where|yang|dan)\b", candidate, maxsplit=1, flags=re.IGNORECASE)[0].strip()
            if candidate:
                return candidate
    return None


def extract_title_guess_from_text(text: str) -> Optional[str]:
    raw = str(text or "")
    quoted = re.search(r"[\"“']([^\"”']{2,80})[\"”']", raw)
    if quoted:
        return quoted.group(1).strip()
    patterns = [
        r"(?:show|query|find|search|display|list|查询|查看|查找)\s+([A-Za-z][A-Za-z0-9 .,'-]{2,80})\s*(?:record|records|记录)",
        r"([A-Za-z][A-Za-z0-9 .,'-]{2,80})\s*(?:的)?\s*(?:record|records|记录)",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw, flags=re.IGNORECASE)
        if not match:
            continue
        candidate = match.group(1).strip()
        candidate = re.sub(
            r"\b(in|inside|within|from|for|payment|payments|rental|rentals|film|films|inventory|customer|customers)\b",
            "",
            candidate,
            flags=re.IGNORECASE,
        ).strip()
        candidate = re.sub(r"\s+", " ", candidate).strip(" ,.-")
        if len(candidate) >= 2:
            return candidate
    return None


def infer_db_mutation_action(text: str) -> Optional[Dict[str, Any]]:
    operation = infer_db_mutation_operation(text)
    table_name = infer_db_mutation_table(text)
    if not operation or not table_name:
        return None

    filters: Dict[str, Any] = {}
    values: Dict[str, Any] = {}
    field_meta = DB_MUTATION_TABLES[table_name]["allowed_fields"]
    field_aliases = DB_MUTATION_FIELD_ALIASES[table_name]
    primary_key = DB_MUTATION_TABLES[table_name]["pk"]

    pk_aliases = set(field_aliases.get(primary_key, set())) | {"id", f"{table_name} id", f"{table_name}_id"}
    pk_values = extract_number_list_from_text(text, pk_aliases)
    if not pk_values:
        pk_values = extract_record_range_from_text(text, table_name)
    if len(pk_values) >= 2:
        filters[primary_key] = pk_values
    else:
        pk_value = extract_number_from_text(text, pk_aliases, allow_decimal=False)
        if pk_value is not None:
            filters[primary_key] = int(pk_value)

    for field_name, meta in field_meta.items():
        aliases = field_aliases.get(field_name, set())
        if not aliases:
            continue
        if meta["type"] == "datetime":
            extracted = extract_datetime_from_text(text, aliases)
        else:
            extracted = extract_number_from_text(text, aliases, allow_decimal=meta["type"] == "float")
        if extracted is None:
            continue

        if operation == "insert":
            if not meta.get("filter_only"):
                values[field_name] = extracted
            continue

        if field_name == primary_key:
            filters[field_name] = extracted
        elif operation == "delete":
            filters[field_name] = extracted
        elif operation == "update":
            values[field_name] = extracted

    if operation == "update" and not filters and primary_key in values:
        filters[primary_key] = values.pop(primary_key)

    action = {
        "type": "mutate_records",
        "operation": operation,
        "table": table_name,
        "filters": filters,
        "values": values,
    }
    return action


def wants_db_query_request(text: str) -> bool:
    if wants_db_mutation_request(text):
        return False
    raw = normalize_intent_text(text)
    tokens = intent_tokens(raw)
    has_op = (
        bool(DB_QUERY_OPERATION_ALIASES.intersection(tokens))
        or any(alias in raw for alias in DB_QUERY_OPERATION_ALIASES)
        or "database" in tokens
        or any(marker in (text or "") for marker in ["数据库", "数据行"])
    )
    has_table = infer_db_query_table(text) is not None
    return has_op and has_table and not wants_chart_request(text)


def infer_db_query_table(text: str) -> Optional[str]:
    raw = normalize_intent_text(text)
    tokens = intent_tokens(raw)
    for table_name, aliases in DB_QUERY_TABLE_ALIASES.items():
        if table_name in tokens or aliases.intersection(tokens) or any(alias in raw for alias in aliases):
            return table_name
    for table_name, field_map in DB_QUERY_FIELD_ALIASES.items():
        for aliases in field_map.values():
            if aliases.intersection(tokens) or any(alias in raw for alias in aliases):
                return table_name
    return None


def infer_db_query_action(text: str) -> Optional[Dict[str, Any]]:
    table_name = infer_db_query_table(text)
    if not table_name:
        return None

    try:
        limit = requested_limit(text, 10)
    except Exception:
        limit = 10
    limit = max(1, min(50, int(limit)))

    filters: Dict[str, Any] = {}
    for field_name, meta in DB_QUERY_SOURCES[table_name]["fields"].items():
        aliases = DB_QUERY_FIELD_ALIASES[table_name].get(field_name, set())
        if not aliases:
            continue
        extracted = None
        if meta["type"] == "int":
            multi_numbers = extract_number_list_from_text(text, aliases)
            if len(multi_numbers) >= 2:
                extracted = multi_numbers
            else:
                extracted = extract_number_from_text(text, aliases, allow_decimal=False)
        elif meta["type"] == "datetime":
            extracted = extract_datetime_from_text(text, aliases)
        elif meta["type"] == "float":
            extracted = extract_number_from_text(text, aliases, allow_decimal=meta["type"] == "float")
        else:
            extracted = extract_text_value_from_text(text, aliases)
        if extracted is not None:
            filters[field_name] = extracted

    if table_name in {"inventory", "film"} and "title" not in filters:
        title_guess = extract_text_value_from_text(text, {"title", "film", "movie", "judul", "电影", "影片"})
        if title_guess:
            filters["title"] = title_guess

    selected_fields = list(DB_QUERY_SOURCES[table_name]["fields"].keys())
    return {
        "type": "query_records",
        "table": table_name,
        "filters": filters,
        "limit": limit,
        "fields": selected_fields,
        "title": f"{DB_QUERY_SOURCES[table_name]['label'].title()} Query",
    }


def infer_db_query_action(text: str) -> Optional[Dict[str, Any]]:
    table_name = infer_db_query_table(text)
    if not table_name:
        return None

    try:
        limit = requested_limit(text, 10)
    except Exception:
        limit = 10
    limit = max(1, min(50, int(limit)))

    filters: Dict[str, Any] = {}
    for field_name, meta in DB_QUERY_SOURCES[table_name]["fields"].items():
        aliases = DB_QUERY_FIELD_ALIASES[table_name].get(field_name, set())
        if not aliases:
            continue
        extracted = None
        if meta["type"] == "int":
            multi_numbers = extract_number_list_from_text(text, aliases)
            if len(multi_numbers) >= 2:
                extracted = multi_numbers
            else:
                extracted = extract_number_from_text(text, aliases, allow_decimal=False)
        elif meta["type"] == "datetime":
            extracted = extract_datetime_from_text(text, aliases)
        elif meta["type"] == "float":
            extracted = extract_number_from_text(text, aliases, allow_decimal=True)
        else:
            extracted = extract_text_value_from_text(text, aliases)
        if extracted is not None:
            filters[field_name] = extracted

    if table_name in {"payment", "rental", "inventory", "film"} and "title" not in filters:
        title_guess = extract_text_value_from_text(text, {"title", "film", "movie", "judul", "电影", "影片", "鐢靛奖", "褰辩墖"})
        if not title_guess:
            title_guess = extract_title_guess_from_text(text)
        if title_guess:
            filters["title"] = title_guess

    selected_fields = list(DB_QUERY_SOURCES[table_name]["fields"].keys())
    return {
        "type": "query_records",
        "table": table_name,
        "filters": filters,
        "limit": limit,
        "fields": selected_fields,
        "title": f"{DB_QUERY_SOURCES[table_name]['label'].title()} Query",
    }


def execute_db_query(action: Dict[str, Any]) -> Dict[str, Any]:
    try:
        table_name = canonicalize_query_table(action.get("table"))
        if not table_name:
            raise ValueError("Only whitelisted dvdrental tables can be queried.")

        source = DB_QUERY_SOURCES[table_name]
        requested_fields = action.get("fields") if isinstance(action.get("fields"), list) else list(source["fields"].keys())
        selected_fields = []
        for field_name in requested_fields:
            canonical = canonicalize_query_field(table_name, field_name)
            if canonical and canonical not in selected_fields:
                selected_fields.append(canonical)
        if not selected_fields:
            selected_fields = list(source["fields"].keys())

        filters = action.get("filters") if isinstance(action.get("filters"), dict) else {}
        normalized_filters: Dict[str, Any] = {}
        where_clauses: List[str] = []
        params: Dict[str, Any] = {}
        for idx, (raw_key, raw_value) in enumerate(filters.items(), start=1):
            field_name = canonicalize_query_field(table_name, raw_key)
            if not field_name:
                continue
            field_meta = source["fields"][field_name]
            if field_meta["type"] == "text":
                param_name = f"filter_{idx}"
                normalized_filters[field_name] = str(raw_value).strip()
                where_clauses.append(f"{field_meta['expr']} ILIKE :{param_name}")
                params[param_name] = f"%{normalized_filters[field_name]}%"
            elif isinstance(raw_value, list):
                normalized_filters[field_name] = [coerce_query_value(field_meta["type"], field_name, item) for item in raw_value]
                placeholders = []
                for list_idx, item in enumerate(normalized_filters[field_name]):
                    param_name = f"filter_{idx}_{list_idx}"
                    placeholders.append(f":{param_name}")
                    params[param_name] = item
                where_clauses.append(f"{field_meta['expr']} IN ({', '.join(placeholders)})")
            else:
                param_name = f"filter_{idx}"
                normalized_filters[field_name] = coerce_query_value(field_meta["type"], field_name, raw_value)
                where_clauses.append(f"{field_meta['expr']} = :{param_name}")
                params[param_name] = normalized_filters[field_name]

        try:
            limit = max(1, min(50, int(action.get("limit") or 10)))
        except Exception:
            limit = 10
        params["limit_value"] = limit

        select_sql = ", ".join(
            f"{source['fields'][field]['expr']} AS {field}" for field in selected_fields
        )
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        sql = f"""
            SELECT {select_sql}
            {source['from_sql']}
            {where_sql}
            ORDER BY {source['fields'][source['pk']]['expr']} ASC
            LIMIT :limit_value
        """
        df = query(sql, params)
        rows = df_to_records(df)
        return {
            **action,
            "type": "query_records",
            "ok": True,
            "table": table_name,
            "columns": selected_fields,
            "data": rows,
            "affected_rows": len(rows),
            "summary": f"Queried {len(rows)} {table_name} record(s).",
        }
    except Exception as exc:
        return {
            **action,
            "type": "query_records",
            "ok": False,
            "error": str(exc),
        }


def infer_custom_chart_spec(text: str) -> Optional[Dict[str, Any]]:
    raw = text or ""
    lowered = raw.lower()

    metric = None
    if re.search(r"\b(revenue|income|pendapatan|payment)\b", lowered) or "鏀跺叆" in raw or "钀ユ敹" in raw:
        metric = "revenue_sum"
    elif re.search(r"\b(customer count|jumlah customer|jumlah pelanggan)\b", lowered):
        metric = "customer_count"
    elif re.search(r"\b(stock|stok|inventory)\b", lowered):
        metric = "inventory_count"
    elif re.search(r"\b(film count|jumlah film|number of films)\b", lowered):
        metric = "film_count"
    elif re.search(r"\b(rental|rentals|rented|popularity|popular|sewa|disewa|penyewaan)\b", lowered) or "绉熻祦" in raw:
        metric = "rental_count"

    dimension = None
    if re.search(r"\b(language|bahasa)\b", lowered):
        dimension = "language"
    elif re.search(r"\b(day of week|weekday|hari)\b", lowered):
        dimension = "rental_day_of_week"
    elif re.search(r"\b(month|monthly|bulan|trend)\b", lowered):
        dimension = "payment_month" if metric == "revenue_sum" else "rental_month"
    elif re.search(r"\b(store|stores|cabang|toko)\b", lowered):
        dimension = "store"
    elif re.search(r"\b(customer|customers|pelanggan)\b", lowered):
        dimension = "customer_name"
    elif re.search(r"\b(actor|actors|aktor)\b", lowered):
        dimension = "actor_name"
    elif re.search(r"\b(rating)\b", lowered) or "璇勭骇" in raw or "璇勫垎" in raw:
        dimension = "rating"
    elif re.search(r"\b(genre|category|kategori)\b", lowered) or "绫诲瀷" in raw:
        dimension = "genre"
    elif re.search(r"\b(film|films|movie|movies|judul|title)\b", lowered) or "鐢靛奖" in raw:
        dimension = "film_title"

    if not metric or not dimension:
        return None

    series = None
    if dimension in {"rental_month", "payment_month"} and re.search(r"\b(store|stores|cabang|toko)\b", lowered):
        series = "store"

    chart_type = requested_chart_type(text) or ("line" if CUSTOM_CHART_DIMENSIONS[dimension]["kind"] == "time" else "bar")
    sort = "asc" if CUSTOM_CHART_DIMENSIONS[dimension]["kind"] == "time" else "desc"
    return {
        "chart_type": chart_type,
        "dimension": dimension,
        "metric": metric,
        "series": series,
        "limit": requested_limit(text, 10),
        "sort": sort,
        "filters": {},
    }


def infer_data_id(text: str) -> str:
    lowered = (text or "").lower()
    if (
        re.search(r"\b(predict|prediction|forecast|machine learning|ml|next month|bulan depan)\b", lowered)
        or "预测" in text
        or "机器学习" in text
        or "下个月" in text
    ):
        return "monthly_rental_trend"
    if re.search(r"\b(customer|customers|pelanggan)\b", lowered) or "客户" in text or "顾客" in text:
        return "top_customers"
    if re.search(r"\b(revenue|income|pendapatan)\b", lowered) or "收入" in text or "营收" in text:
        return "revenue_by_genre"
    if re.search(r"\b(actor|actors|aktor)\b", lowered) or "演员" in text:
        return "actor_rental_count"
    if re.search(r"\b(least|bottom|terendah)\b", lowered) or "最低" in text or "最少" in text:
        return "least_rented_films"
    if re.search(r"\b(month|monthly|bulan)\b", lowered) or "月份" in text or "每月" in text:
        return "monthly_revenue" if ("revenue" in lowered or "收入" in text or "营收" in text) else "monthly_rental_trend"
    if re.search(r"\b(rating|评级)\b", lowered) or "评级" in text or "评分" in text:
        return "rating_distribution"
    if re.search(r"\b(genre|category|类型|类别)\b", lowered) or "类型" in text or "类别" in text:
        return "genre_distribution"
    return "top_rented_films"


def fallback_actions_for_request(text: str) -> List[Dict[str, Any]]:
    if wants_chart_type_switch(text):
        return [{
            "type": "set_chart_type",
            "chart": requested_chart_target(text),
            "chart_type": requested_chart_type(text),
        }]
    if not wants_table(text):
        return []
    table_id = infer_data_id(text)
    action = {
        "type": "render_table",
        "table": table_id,
        "params": {"limit": requested_limit(text, 10)},
        "title": None,
    }
    return hydrate_actions([action])


def infer_data_id(text: str) -> str:
    raw = text or ""
    lowered = raw.lower()

    if (
        re.search(r"\b(predict|prediction|forecast|machine learning|ml|next month|bulan depan)\b", lowered)
        or "棰勬祴" in raw
        or "鏈哄櫒瀛︿範" in raw
        or "涓嬩釜鏈" in raw
    ):
        return "monthly_rental_trend"

    if (
        re.search(r"\b(revenue|income|pendapatan|payment)\b", lowered)
        or "鏀跺叆" in raw
        or "钀ユ敹" in raw
    ) and (
        re.search(r"\b(store|stores|per store|cabang|toko)\b", lowered)
        or "闂ㄥ簵" in raw
    ):
        return "monthly_revenue_per_store"

    if re.search(r"\b(customer|customers|pelanggan)\b", lowered) or "瀹㈡埛" in raw or "椤惧" in raw:
        return "top_customers"

    if re.search(r"\b(revenue|income|pendapatan|payment)\b", lowered) or "鏀跺叆" in raw or "钀ユ敹" in raw:
        if re.search(r"\b(month|monthly|trend|bulan)\b", lowered) or "鏈堜唤" in raw or "姣忔湀" in raw or "瓒嬪娍" in raw:
            return "monthly_revenue"
        return "revenue_by_genre"

    if re.search(r"\b(actor|actors|aktor)\b", lowered) or "婕斿憳" in raw:
        if re.search(r"\b(film|films|movie|movies)\b", lowered) or "鐢靛奖" in raw:
            return "actor_film_count"
        return "actor_rental_count"

    if re.search(r"\b(least|bottom|terendah)\b", lowered) or "鏈€浣" in raw or "鏈€灏" in raw:
        return "least_rented_films"

    if re.search(r"\b(month|monthly|bulan)\b", lowered) or "鏈堜唤" in raw or "姣忔湀" in raw:
        if re.search(r"\b(revenue|income|pendapatan|payment)\b", lowered) or "鏀跺叆" in raw or "钀ユ敹" in raw:
            return "monthly_revenue"
        return "monthly_rental_trend"

    if re.search(r"\b(rating)\b", lowered) or "璇勭骇" in raw or "璇勫垎" in raw:
        return "rating_distribution"

    if re.search(r"\b(genre|category)\b", lowered) or "绫诲瀷" in raw or "绫诲埆" in raw:
        return "genre_distribution"

    return "top_rented_films"


def fallback_actions_for_request(text: str) -> List[Dict[str, Any]]:
    if wants_db_query_request(text):
        query_action = infer_db_query_action(text)
        if query_action:
            return hydrate_actions([query_action])

    if wants_db_mutation_request(text):
        mutation_action = infer_db_mutation_action(text)
        if mutation_action:
            return hydrate_actions([mutation_action])

    if wants_clear_ai_outputs_request(text):
        return [{"type": "clear_ai_outputs"}]

    if wants_delete_ai_output_request(text):
        target = requested_ai_output_target(text)
        if target == "all":
            return [{"type": "clear_ai_outputs"}]
        return [{"type": "delete_ai_output", "target": target}]

    if wants_modify_ai_chart_request(text):
        next_type = requested_chart_type(text) or requested_unsupported_chart_type(text)
        if not next_type:
            return []
        return [{
            "type": "update_ai_chart",
            "target": requested_ai_output_target(text),
            "chart_type": next_type,
        }]

    if wants_chart_type_switch(text):
        chart = requested_chart_target(text)
        chart_id = infer_data_id(text)
        actions: List[Dict[str, Any]] = [{
            "type": "set_chart_type",
            "chart": chart,
            "chart_type": requested_chart_type(text),
        }]
        if wants_chart_request(text):
            actions.append({
                "type": "render_chart",
                "chart": chart_id,
                "params": requested_chart_params(chart_id, text),
                "title": requested_chart_title(chart_id, text),
            })
        return hydrate_actions(actions)

    if wants_chart_request(text):
        explicit_chart_id = infer_explicit_chart_id(text)
        if explicit_chart_id:
            actions: List[Dict[str, Any]] = []
            chart_type = requested_chart_type(text)
            if chart_type and explicit_chart_id in {"monthly_revenue", "monthly_revenue_per_store", "monthly_rental_trend"}:
                actions.append({
                    "type": "set_chart_type",
                    "chart": explicit_chart_id,
                    "chart_type": chart_type,
                })
            actions.append({
                "type": "render_chart",
                "chart": explicit_chart_id,
                "params": requested_chart_params(explicit_chart_id, text),
                "title": requested_chart_title(explicit_chart_id, text),
            })
            return hydrate_actions(actions)

        custom_spec = infer_custom_chart_spec(text)
        if custom_spec:
            return hydrate_actions([{
                "type": "render_custom_chart",
                "title": build_custom_chart_title(normalize_custom_chart_spec(custom_spec)),
                "spec": custom_spec,
            }])
        return []

    if not wants_table(text):
        return []

    table_id = infer_data_id(text)
    action = {
        "type": "render_table",
        "table": table_id,
        "params": {"limit": requested_limit(text, 10)},
        "title": None,
    }
    return hydrate_actions([action])


def parse_actions(text_reply: str):
    """Extract the trailing ```action ...``` JSON block, if any."""
    if "```action" not in text_reply:
        return text_reply, []
    head, _, rest = text_reply.partition("```action")
    block, _, _ = rest.partition("```")
    try:
        parsed = json.loads(block.strip())
        actions = parsed.get("actions", []) if isinstance(parsed, dict) else []
    except Exception:
        actions = []
    return head.rstrip(), actions


def hydrate_actions(actions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Attach real datasets to render actions so the frontend can draw them."""
    out = []
    for act in actions:
        if not isinstance(act, dict):
            continue
        if act.get("type") == "render_chart":
            payload = chart_payload(act.get("chart", ""), act.get("params", {}))
            if payload:
                merged = {**act, **payload}
                out.append(merged)
            else:
                out.append({**act, "error": "unknown chart"})
        elif act.get("type") == "render_custom_chart":
            try:
                payload = custom_chart_payload(act.get("spec", {}), act.get("title"))
                merged = {**act, **payload}
                out.append(merged)
            except Exception as exc:
                out.append({**act, "chart": "custom", "error": str(exc)})
        elif act.get("type") == "render_table":
            payload = table_payload(act.get("table") or act.get("chart", ""), act.get("params", {}))
            if payload:
                merged = {**act, **payload}
                out.append(merged)
            else:
                out.append({**act, "error": "unknown table"})
        elif act.get("type") == "query_records":
            out.append(execute_db_query(act))
        elif act.get("type") == "mutate_records":
            out.append(execute_db_mutation(act))
        else:
            out.append(act)
    return out


def action_ack_reply(user_text: str, actions: List[Dict[str, Any]]) -> Optional[str]:
    if not actions:
        return None

    lang = detect_user_language(user_text)
    action_type = actions[0].get("type")

    if action_type == "set_chart_type":
        if lang == "zh":
            return "\u56fe\u8868\u7c7b\u578b\u5df2\u5207\u6362\uff0c\u5237\u65b0\u540e\u4e5f\u4f1a\u4fdd\u7559\u3002"
        if lang == "id":
            return "Tipe chart sudah diganti dan akan tetap tersimpan setelah refresh."
        return "The chart type has been changed and will stay saved after refresh."

    if action_type in {"render_chart", "render_custom_chart"}:
        if lang == "zh":
            return "\u56fe\u8868\u5df2\u6309\u4f60\u7684\u8981\u6c42\u751f\u6210\uff0c\u5e76\u4e14\u5237\u65b0\u540e\u4f1a\u7ee7\u7eed\u4fdd\u7559\u3002"
        if lang == "id":
            return "Chart sudah dibuat sesuai permintaanmu dan akan tetap tampil setelah refresh."
        return "The chart has been generated as requested and will stay visible after refresh."

    if action_type == "render_table":
        if lang == "zh":
            return "\u8868\u683c\u5df2\u6309\u4f60\u7684\u8981\u6c42\u751f\u6210\uff0c\u5e76\u4e14\u5237\u65b0\u540e\u4f1a\u7ee7\u7eed\u4fdd\u7559\u3002"
        if lang == "id":
            return "Tabel sudah dibuat sesuai permintaanmu dan akan tetap tampil setelah refresh."
        return "The table has been generated as requested and will stay visible after refresh."

    if action_type == "query_records":
        if not actions[0].get("ok"):
            reason = actions[0].get("error") or "The requested database query could not be completed."
            if lang == "zh":
                return f"这次数据库查询没有执行成功：{reason}"
            if lang == "id":
                return f"Query database tidak berhasil dijalankan: {reason}"
            return f"The database query was not executed: {reason}"
        table_name = actions[0].get("table", "records")
        count = int(actions[0].get("affected_rows") or 0)
        if lang == "zh":
            return f"数据库查询已完成：已从 {table_name} 中取回 {count} 条记录，并显示在 dashboard 上。"
        if lang == "id":
            return f"Query database selesai: {count} record dari {table_name} sudah ditampilkan di dashboard."
        return f"The database query is complete: {count} record(s) from {table_name} are now shown on the dashboard."

    if action_type == "delete_ai_output":
        if lang == "zh":
            return "\u5df2\u5220\u9664\u4f60\u8981\u79fb\u9664\u7684\u56fe\u8868\u6216\u8868\u683c\u3002"
        if lang == "id":
            return "Chart atau tabel yang diminta sudah dihapus."
        return "The requested chart or table has been removed."

    if action_type == "clear_ai_outputs":
        if lang == "zh":
            return "\u5df2\u6e05\u7a7a\u6240\u6709 AI \u751f\u6210\u7684\u56fe\u8868\u548c\u8868\u683c\u3002"
        if lang == "id":
            return "Semua chart dan tabel AI sudah dihapus."
        return "All AI-generated charts and tables have been removed."

    if action_type == "update_ai_chart":
        if lang == "zh":
            return "\u6211\u4f1a\u5c1d\u8bd5\u4fee\u6539\u5f53\u524d AI \u751f\u6210\u7684\u56fe\u8868\uff1b\u5982\u679c\u627e\u4e0d\u5230\u53ef\u4fee\u6539\u7684\u56fe\u8868\u6216\u683c\u5f0f\u4e0d\u652f\u6301\uff0c\u4f1a\u76f4\u63a5\u544a\u8bc9\u4f60\u539f\u56e0\u3002"
        if lang == "id":
            return "Saya akan mencoba mengubah chart AI yang sudah ada. Kalau tidak ada chart yang cocok atau formatnya tidak didukung, alasannya akan ditampilkan."
        return "I'll try to update the existing AI-generated chart. If no matching chart is found or the format is unsupported, I'll tell you why."

    if action_type == "mutate_records":
        if not actions[0].get("ok"):
            reason = actions[0].get("error") or "The requested database change could not be completed."
            if lang == "zh":
                return f"这次数据库修改没有执行成功：{reason}"
            if lang == "id":
                return f"Perubahan database tidak dijalankan: {reason}"
            return f"The database change was not executed: {reason}"

        table_name = actions[0].get("table", "record")
        count = int(actions[0].get("affected_rows") or 0)
        operation = actions[0].get("operation", "updated")
        related_deleted = actions[0].get("related_deleted") or {}
        related_text = ", ".join(f"{name}={value}" for name, value in related_deleted.items() if value)
        if lang == "zh":
            extra = f"；同时级联删除了关联记录：{related_text}" if related_text else ""
            return f"数据库已修改：{table_name} 已执行 {operation}，影响了 {count} 条记录{extra}，并将刷新 dashboard 数据。"
        if lang == "id":
            extra = f", sekaligus menghapus relasi terkait: {related_text}" if related_text else ""
            return f"Database berhasil diubah: operasi {operation} pada {table_name} memengaruhi {count} record{extra}, dan dashboard akan disegarkan."
        extra = f", and cascade deleted related records: {related_text}" if related_text else ""
        return f"The database was updated: {operation} on {table_name} affected {count} record(s){extra}, and the dashboard will refresh."

    return None


def should_force_intended_actions(user_text: str) -> bool:
    return (
        wants_chart_request(user_text)
        or wants_table(user_text)
        or wants_chart_type_switch(user_text)
        or wants_delete_ai_output_request(user_text)
        or wants_clear_ai_outputs_request(user_text)
        or wants_modify_ai_chart_request(user_text)
        or wants_db_query_request(user_text)
        or wants_db_mutation_request(user_text)
    )


def unavailable_data_reply(user_text: str, artifact: str = "chart") -> str:
    lang = detect_user_language(user_text)
    if lang == "zh":
        kind = "\u56fe\u8868" if artifact == "chart" else "\u8868\u683c"
        return f"\u6211\u4e0d\u80fd\u751f\u6210\u8fd9\u4e2a{kind}\uff0c\u56e0\u4e3a\u8fd9\u4e2a\u8bf7\u6c42\u4e0d\u5728 dvdrental \u6570\u636e\u5e93\u5df2\u6709\u7684\u6570\u636e\u8303\u56f4\u5185\u3002"
    if lang == "id":
        kind = "chart" if artifact == "chart" else "tabel"
        return f"Saya tidak bisa membuat {kind} itu karena datanya tidak ada atau tidak terpetakan di database dvdrental."
    kind = "chart" if artifact == "chart" else "table"
    return f"I can't generate that {kind} because the data is not available or not mapped in the dvdrental database."


def unsupported_chart_type_reply(user_text: str, chart_type: str) -> str:
    lang = detect_user_language(user_text)
    if lang == "zh":
        return f"这个图目前不能直接改成 {chart_type}。像 monthly revenue 这类时间序列图，目前只支持切换为 bar、line、area 或 scatter；不适合直接改成 pie 或 donut。"
    if lang == "id":
        return f"Chart itu saat ini tidak bisa langsung diubah menjadi {chart_type}. Untuk chart time-series seperti monthly revenue, saat ini hanya didukung bar, line, area, atau scatter."
    return f"That chart can't be directly changed into {chart_type}. For time-series charts like monthly revenue, the supported formats are bar, line, area, and scatter."


def mutation_detail_reply(user_text: str) -> str:
    lang = detect_user_language(user_text)
    operation = infer_db_mutation_operation(user_text)
    table_name = infer_db_mutation_table(user_text)
    action = infer_db_mutation_action(user_text)

    if not table_name:
        if lang == "zh":
            return "目前只能修改 dvdrental 里的 payment 或 rental 记录，其他表或外部数据不能直接操作。"
        if lang == "id":
            return "Saat ini saya hanya bisa mengubah record payment atau rental di dvdrental, bukan tabel atau data di luar itu."
        return "Right now I can only modify payment or rental records inside dvdrental, not other tables or outside data."

    if not operation:
        if lang == "zh":
            return "我需要你明确是要新增、更新，还是删除记录。"
        if lang == "id":
            return "Saya perlu tahu apakah kamu ingin insert, update, atau delete record."
        return "I need to know whether you want to insert, update, or delete records."

    if not action:
        if lang == "zh":
            return "我暂时还不能从这句话里提取出可执行的数据库操作。请至少给出记录类型和关键字段。"
        if lang == "id":
            return "Saya belum bisa mengekstrak operasi database yang aman dari kalimat itu. Tolong sertakan tipe record dan field pentingnya."
        return "I still can't extract a safe database operation from that sentence. Please include the record type and key fields."

    if operation in {"delete", "update"} and not action.get("filters"):
        if lang == "zh":
            return f"{operation} {table_name} 记录时，我至少需要一个筛选条件，例如 {DB_MUTATION_TABLES[table_name]['pk']}、customer_id 或日期。否则我不会盲删或盲改。如果你要删多条，请明确写出多个 id。"
        if lang == "id":
            return f"Untuk {operation} record {table_name}, saya butuh minimal satu filter seperti {DB_MUTATION_TABLES[table_name]['pk']}, customer_id, atau tanggal. Saya tidak akan mengubah data secara buta. Kalau ingin hapus beberapa baris, tulis id-nya dengan jelas."
        return f"To {operation} {table_name} records, I need at least one filter such as {DB_MUTATION_TABLES[table_name]['pk']}, customer_id, or a date. I won't change data blindly. If you want to delete multiple rows, list the ids explicitly."

    if operation in {"insert", "update"} and not action.get("values"):
        if lang == "zh":
            return f"{operation} {table_name} 记录时，我需要明确要写入哪些字段和值。"
        if lang == "id":
            return f"Untuk {operation} record {table_name}, saya perlu field dan nilai yang ingin ditulis."
        return f"To {operation} {table_name} records, I need the fields and values you want to write."

    return unavailable_data_reply(user_text, "table")


def query_detail_reply(user_text: str) -> str:
    lang = detect_user_language(user_text)
    table_name = infer_db_query_table(user_text)
    action = infer_db_query_action(user_text)

    if not table_name:
        if lang == "zh":
            return "目前只能直接查询 dvdrental 白名单里的 payment、rental、inventory、customer 或 film 记录。"
        if lang == "id":
            return "Saat ini saya hanya bisa query record payment, rental, inventory, customer, atau film yang sudah di-whitelist dari dvdrental."
        return "Right now I can only directly query whitelisted payment, rental, inventory, customer, or film records from dvdrental."

    if not action:
        if lang == "zh":
            return "我暂时还不能从这句话里提取出可执行的数据库查询。请至少说明要查哪类记录，或者给一个筛选条件。"
        if lang == "id":
            return "Saya belum bisa mengekstrak query database yang aman dari kalimat itu. Tolong sebutkan tipe record atau filter yang ingin dicari."
        return "I still can't extract a safe database query from that sentence. Please mention the record type or a filter to search for."

    return unavailable_data_reply(user_text, "table")


def should_replace_reply_with_action_ack(actions: List[Dict[str, Any]]) -> bool:
    if not actions:
        return False
    return actions[0].get("type") in {
        "render_chart",
        "render_custom_chart",
        "render_table",
        "query_records",
        "set_chart_type",
        "delete_ai_output",
        "clear_ai_outputs",
        "update_ai_chart",
        "mutate_records",
    }


@app.post("/api/chat")
def api_chat(req: ChatRequest):
    mode = choose_chat_mode(req.messages)
    last_user = next((m.content for m in reversed(req.messages) if m.role == "user"), "")

    if mode == "general":
        return out_of_scope_reply(last_user)

    if not DEEPSEEK_API_KEY:
        raise HTTPException(500, "DEEPSEEK_API_KEY is not configured.")

    try:
        from openai import OpenAI
    except ImportError:
        raise HTTPException(500, "Please `pip install openai`.")

    client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_BASE_URL)

    if mode == "developer":
        try:
            return developer_chat_reply(client, req)
        except Exception as exc:
            raise HTTPException(500, f"Developer mode error: {exc}")

    system_prompt = build_system_prompt(mode)

    msgs = [{"role": "system", "content": system_prompt}]
    for m in req.messages[-12:]:
        msgs.append({"role": m.role, "content": m.content})

    try:
        response = client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=msgs,
            temperature=0.2,
            max_tokens=900,
        )
    except Exception as exc:
        raise HTTPException(500, f"DeepSeek error: {exc}")

    raw = (response.choices[0].message.content or "").strip()
    text_only, actions = parse_actions(raw)
    actions = hydrate_actions(actions)
    intended_actions = fallback_actions_for_request(last_user)
    if should_force_intended_actions(last_user) and intended_actions:
        return {
            "reply": action_ack_reply(last_user, intended_actions) or text_only,
            "actions": intended_actions,
        }
    if wants_db_query_request(last_user) and not intended_actions and not any(
        act.get("type") == "query_records" for act in actions
    ):
        return {"reply": query_detail_reply(last_user), "actions": []}
    if any(act.get("type") == "query_records" for act in actions) and not any(
        act.get("type") == "query_records" for act in intended_actions
    ):
        return {"reply": query_detail_reply(last_user), "actions": []}
    if wants_db_mutation_request(last_user) and not intended_actions and not any(
        act.get("type") == "mutate_records" for act in actions
    ):
        return {"reply": mutation_detail_reply(last_user), "actions": []}
    if any(act.get("type") == "mutate_records" for act in actions) and not any(
        act.get("type") == "mutate_records" for act in intended_actions
    ):
        return {"reply": mutation_detail_reply(last_user), "actions": []}
    if requested_unsupported_chart_type(last_user) and re.search(r"\b(change|switch|set|make|turn|convert|ubah|ganti|jadikan|buat|atur)\b", (last_user or "").lower()):
        return {"reply": unsupported_chart_type_reply(last_user, requested_unsupported_chart_type(last_user) or "that format"), "actions": []}
    if wants_chart_request(last_user) and not intended_actions:
        return {"reply": unavailable_data_reply(last_user, "chart"), "actions": []}
    if wants_table(last_user) and not intended_actions:
        return {"reply": unavailable_data_reply(last_user, "table"), "actions": []}
    if not actions and intended_actions:
        return {
            "reply": action_ack_reply(last_user, intended_actions) or text_only,
            "actions": intended_actions,
        }
    if not actions:
        actions = fallback_actions_for_request(last_user)
        if actions and not text_only and actions[0].get("type") == "set_chart_type":
            lang = detect_user_language(last_user)
            if lang == "zh":
                text_only = "\u56fe\u8868\u7c7b\u578b\u5df2\u5207\u6362\uff0c\u5237\u65b0\u540e\u4e5f\u4f1a\u4fdd\u7559\u3002"
            elif lang == "id":
                text_only = "Tipe chart sudah diganti dan akan tetap tersimpan setelah refresh."
            else:
                text_only = "The chart type has been changed and will stay saved after refresh."
        if actions and not text_only:
            lang = detect_user_language(last_user)
            if lang == "zh":
                text_only = "已生成表格，并会在刷新后继续保留。"
            elif lang == "id":
                text_only = "Tabel sudah dibuat dan akan tetap tampil setelah refresh."
            else:
                text_only = "The table has been generated and will stay visible after refresh."
    if actions and actions[0].get("type") in {"render_chart", "render_custom_chart"}:
        lang = detect_user_language(last_user)
        if not text_only or text_only == "Tabel sudah dibuat dan akan tetap tampil setelah refresh.":
            if lang == "zh":
                text_only = "\u56fe\u8868\u5df2\u751f\u6210\uff0c\u5e76\u4e14\u5237\u65b0\u540e\u4f1a\u7ee7\u7eed\u4fdd\u7559\u3002"
            elif lang == "id":
                text_only = "Chart sudah dibuat dan akan tetap tampil setelah refresh."
            else:
                text_only = "The chart has been generated and will stay visible after refresh."
    if should_replace_reply_with_action_ack(actions):
        text_only = action_ack_reply(last_user, actions) or text_only
    return {"reply": text_only, "actions": actions}


# Local dev entry-point
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
