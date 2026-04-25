# -*- coding: utf-8 -*-

# ======================================================
# СОЗДАНИЕ ТАБЛИЦЫ БРЕНДОВ
# Скрипт: create_dim_brands.py
# Таблица: dim_brands
#
# 1. Создаёт таблицу с 24 брендами
# 2. По каждому ищет в MIT API первый рег.номер
# 3. Запрашивает detail — достаёт страну/город
# 4. Если у первого нет — пробует следующий
#
# Запускается ДО producer_main2.py
# ======================================================

import time
import ssl
import re
import random
from datetime import datetime

import requests
import certifi
import psycopg2
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===================== НАСТРОЙКИ =====================

BASE = "https://fgis.gost.ru/fundmetrology/eapi"
MIT_URL = f"{BASE}/mit"

PG = dict(host="localhost", port=5432, dbname="postgres", user="postgres", password="1234")

BASE_DELAY = 1.0
JITTER = 0.1
MAX_TRIES = 3
MAX_REG_NUMBERS_TO_TRY = 5  # сколько рег.номеров пробовать для поиска страны/города

BRANDS = [
   ("Baylan",         "Baylan",        "Baylan, БАЙЛАН"),
    ("Valtec",         "Валтек",        "ВАЛТЕК, Валтек"),
   ("Аква-С",         "Аква-С",        "Аква-С, АКВА-С"),
    ("Арзамас",        "Арзамас",       "Арзамас, АРЗАМАС, АПЗ"),
    ("Байкал",         "Байкал",        "Байкал, БАЙКАЛ"),
    ("Бетар",          "Бетар",         "БЕТАР, Бетар"),
    ("Вавиот",         "Вавиот",        "Вавиот, ВАВИОТ, Телематические"),
    ("Водомер",        "Водомер",       "Водомер, ВОДОМЕР"),
    ("Геррида",        "Геррида",       "ГЕРРИДА, Геррида"),
    #("Гроен",          "Гроен",         "Гроен, ГРОЕН"),
    ("Декаст",         "Декаст",        "Декаст, ДЕКАСТ"),
    ("Ителма",         "ИТЭЛМА",        'ИТЭЛМА, Итэлма, ИТЕЛМА, Ителма, НПП"ИБС", НПП ИБС'),
    ("Карат",          "Карат",         "Карат, КАРАТ"),
    ("Метер",          "Метер",         "Метер, МЕТЕР"),
    ("Норма",          "Норма",         "Норма, НОРМА"),
    ("Санекст",        "Санекст",       "Санекст, САНЕКСТ, SaNext"),
    ("Спецтехприбор",  "Спецтехприбор", "Спецтехприбор, СПЕЦТЕХПРИБОР"),
    ("Тепловодомер",   "Тепловодомер",  "Тепловодомер, ТЕПЛОВОДОМЕР"),
    ("Тепловодохран",  "Тепловодохран", "Тепловодохран, ТЕПЛОВОДОХРАН"),
    ("Ценнер",         "Ценнер",        "Ценнер, ЦЕННЕР, Zenner"),
    ("Эквател",        "Эквател",       "Эквател, ЭКВАТЕЛ"),
    ("Экомера",        "Экомера",       "Экомера, ЭКОМЕРА"),
    ("Эконом",         "Эконом",        "Эконом, ЭКОНОМ"),
]

# ===================== ЦВЕТА =====================

class C:
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    CYAN = "\033[96m"
    MAGENTA = "\033[95m"
    GRAY = "\033[90m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    RESET = "\033[0m"

def ok(msg):    print(f"  {C.GREEN}v  {msg}{C.RESET}")
def warn(msg):  print(f"  {C.YELLOW}!  {msg}{C.RESET}")
def err(msg):   print(f"  {C.RED}x  {msg}{C.RESET}")
def info(msg):  print(f"  {C.CYAN}i  {msg}{C.RESET}")
def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"  {C.GRAY}   [{ts}] {msg}{C.RESET}")

# ===================== TLS / СЕССИЯ =====================

class TLSAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        ctx = ssl.create_default_context(cafile=certifi.where())
        pool_kwargs["ssl_context"] = ctx
        super().init_poolmanager(connections, maxsize, block, **pool_kwargs)

def build_session():
    s = requests.Session()
    retry = Retry(total=3, connect=3, read=3, backoff_factor=0.3, status_forcelist=[429, 502, 503, 504], allowed_methods=["GET"])
    s.mount("https://", TLSAdapter(max_retries=retry))
    s.headers.update({"Accept": "application/json"})
    return s

def polite_sleep():
    time.sleep(BASE_DELAY + random.uniform(0, JITTER))

# ===================== MIT API =====================

def search_mit(session, search_str):
    """Ищем рег.номера по бренду в MIT API"""
    for attempt in range(1, MAX_TRIES + 1):
        try:
            polite_sleep()
            r = session.get(MIT_URL, params={"search": f"*{search_str}*", "rows": MAX_REG_NUMBERS_TO_TRY}, timeout=60)
            r.encoding = "utf-8"
            if r.status_code != 200:
                time.sleep(3 * attempt)
                continue
            items = (r.json().get("result") or {}).get("items") or []
            return items
        except Exception as e:
            warn(f"MIT search: {e}")
            time.sleep(3 * attempt)
    return []


def get_country_city(session, mit_uuid):
    """Достаём страну и город из detail карточки"""
    for attempt in range(1, MAX_TRIES + 1):
        try:
            polite_sleep()
            r = session.get(f"{MIT_URL}/{mit_uuid}", timeout=60)
            r.encoding = "utf-8"
            if r.status_code != 200:
                time.sleep(3 * attempt)
                continue
            data = r.json()
            manufacturers = data.get("manufacturer") or []
            if manufacturers:
                mfr = manufacturers[0]
                country = (mfr.get("country") or "").strip()
                address = (mfr.get("address") or "").strip()
                city = ""
                m = re.search(r'г\.\s*([^,]+)', address)
                if m:
                    city = m.group(1).strip()
                if country:
                    return country, city
            return "", ""
        except Exception as e:
            warn(f"MIT detail: {e}")
            time.sleep(3 * attempt)
    return "", ""


def find_country_city_for_brand(session, search_str):
    """Перебираем рег.номера бренда пока не найдём страну/город"""
    items = search_mit(session, search_str)
    if not items:
        return "", ""

    for item in items[:MAX_REG_NUMBERS_TO_TRY]:
        mit_uuid = item.get("mit_uuid")
        num = item.get("number", "?")
        if not mit_uuid:
            continue

        log(f"Пробуем {num} (mit_uuid={mit_uuid[:20]}...)")
        country, city = get_country_city(session, mit_uuid)
        if country:
            return country, city

    return "", ""

# ===================== MAIN =====================

def main():
    print(f"\n{C.MAGENTA}{C.BOLD}")
    print("  ============================================")
    print("  СОЗДАНИЕ ТАБЛИЦЫ БРЕНДОВ")
    print("  Скрипт: create_dim_brands.py")
    print("  Таблица: dim_brands")
    print("  Страна/город из MIT API")
    print(f"  ============================================{C.RESET}\n")

    started = datetime.now()
    session = build_session()
    ok("HTTP сессия")

    conn = psycopg2.connect(**PG)
    cur = conn.cursor()

    # Создаём таблицу
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_brands (
            id              SERIAL PRIMARY KEY,
            бренд           TEXT UNIQUE,
            поиск_mit       TEXT,
            ключевые_слова  TEXT,
            страна          TEXT DEFAULT '',
            город           TEXT DEFAULT '',
            updated_at      TIMESTAMPTZ DEFAULT NOW()
        );
    """)
    conn.commit()
    ok("Таблица dim_brands создана")

    # Заполняем
    found_geo = 0
    for i, (бренд, поиск, ключевые) in enumerate(BRANDS, 1):
        print(f"\n  {C.BOLD}[{i}/{len(BRANDS)}] {бренд}{C.RESET}")
        info(f"Поиск MIT: *{поиск}*")

        # Ищем страну/город через MIT API
        страна, город = find_country_city_for_brand(session, поиск)

        if страна:
            ok(f"{страна} / {город}")
            found_geo += 1
        else:
            warn("Страна/город не найдены")

        # Записываем
        cur.execute("""
            INSERT INTO dim_brands (бренд, поиск_mit, ключевые_слова, страна, город, updated_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (бренд) DO UPDATE SET
                поиск_mit = EXCLUDED.поиск_mit,
                ключевые_слова = EXCLUDED.ключевые_слова,
                страна = EXCLUDED.страна,
                город = EXCLUDED.город,
                updated_at = NOW();
        """, (бренд, поиск, ключевые, страна, город))
        conn.commit()

    # ========== ИТОГ ==========
    print(f"\n  {'=' * 100}")
    print(f"  {C.BOLD}ИТОГ{C.RESET}")
    print(f"  {'=' * 100}\n")

    cur.execute("SELECT бренд, поиск_mit, ключевые_слова, страна, город FROM dim_brands ORDER BY бренд;")
    rows = cur.fetchall()

    print(f"  {C.BOLD}{'Бренд':<16} {'Поиск MIT':<14} {'Ключевые слова':<40} {'Страна':<10} {'Город'}{C.RESET}")
    print(f"  {'-' * 95}")
    for бренд, поиск, ключевые, страна, город in rows:
        geo_color = C.GREEN if страна else C.RED
        print(f"  {бренд:<16} {поиск:<14} {(ключевые or '')[:38]:<40} {geo_color}{(страна or ''):<10} {город or ''}{C.RESET}")

    print(f"\n  {C.BOLD}Всего: {len(rows)} брендов | С гео: {found_geo} | Без гео: {len(rows) - found_geo}{C.RESET}")

    cur.close()
    conn.close()

    ended = datetime.now()
    mins = (ended - started).total_seconds() / 60
    info(f"Время: {mins:.1f} мин")

    print(f"\n  {C.MAGENTA}{C.BOLD}Готово!{C.RESET}")
    print(f"  {C.GRAY}SELECT * FROM dim_brands ORDER BY бренд;")
    print(f"  Пустые гео дозаполните вручную в pgAdmin.{C.RESET}\n")


if __name__ == "__main__":
    main()
