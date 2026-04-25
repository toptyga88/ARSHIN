# -*- coding: utf-8 -*-

# ======================================================

# ЭТАП 1: ОПРЕДЕЛЕНИЕ ПЕРВИЧНЫХ ПОВЕРИТЕЛЕЙ (УМНАЯ ВЫБОРКА)

# Скрипт: новыйпервич.py

# Таблица: manufacturers_verifiers

#

# Запуск:

# python новыйпервич.py 2025

# python новыйпервич.py 2025 --pings 75 --positions 5

#

# НОВАЯ ЛОГИКА:

# - Равномерная выборка из N точек в общем списке поверок за год

# - Пингуем каждую, определяем vriType ('1' первичная / '2' периодическая)

# - Если доля первичных >= THRESHOLD%, поверитель — производитель

# ======================================================

import sys
import time
import ssl
import random
import argparse
from collections import defaultdict
from datetime import datetime

import requests
import certifi
import psycopg2
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://fgis.gost.ru/fundmetrology/eapi"
MIT_URL = f"{BASE}/mit"
VRI_URL = f"{BASE}/vri"

PG = dict(host="localhost", port=5432, dbname="postgres", user="postgres", password="1234")

ROWS = 100
BASE_DELAY = 1.0
JITTER = 0.1
MAX_TRIES = 5

DEFAULT_YEAR = 2025
DEFAULT_PINGS = 75
DEFAULT_POSITIONS = 5
DEFAULT_THRESHOLD = 70

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("year", nargs="?", type=int, default=DEFAULT_YEAR)
    p.add_argument("--pings", type=int, default=DEFAULT_PINGS)
    p.add_argument("--positions", type=int, default=DEFAULT_POSITIONS)
    p.add_argument("--threshold", type=int, default=DEFAULT_THRESHOLD)
    return p.parse_args()

ARGS = parse_args()
YEAR = ARGS.year
PINGS_TOTAL = ARGS.pings
SAMPLE_POSITIONS = ARGS.positions
THRESHOLD = ARGS.threshold

BRAND_MFR_FILTER = {
    "Тепловодохран": lambda mfr: ("Тепловодохран" in mfr or "ТЕПЛОВОДОХРАН" in mfr)
    and "Тепловодомер" not in mfr and "ТЕПЛОВОДОМЕР" not in mfr,
    "Тепловодомер": lambda mfr: ("Тепловодомер" in mfr or "ТЕПЛОВОДОМЕР" in mfr)
    and "Тепловодохран" not in mfr and "ТЕПЛОВОДОХРАН" not in mfr,
    "Водомер": lambda mfr: ("Водомер" in mfr or "ВОДОМЕР" in mfr)
    and "Тепловодомер" not in mfr and "ТЕПЛОВОДОМЕР" not in mfr,
}

def is_water_meter(title):
    t = title.lower()
    if "счетчик" not in t and "счётчик" not in t and "теплосчет" not in t and "теплосчёт" not in t:
        return False
    if "газ" in t:
        return False
    if "электрич" in t or "электроэнерг" in t:
        return False
    return True

class C:
    GREEN = "\033[92m"; YELLOW = "\033[93m"; CYAN = "\033[96m"
    MAGENTA = "\033[95m"; WHITE = "\033[97m"; GRAY = "\033[90m"
    RED = "\033[91m"; BOLD = "\033[1m"; RESET = "\033[0m"

def ok(msg):   print(f"  {C.GREEN}v  {msg}{C.RESET}")
def warn(msg): print(f"  {C.YELLOW}!  {msg}{C.RESET}")
def err(msg):  print(f"  {C.RED}x  {msg}{C.RESET}")
def info(msg): print(f"  {C.CYAN}i  {msg}{C.RESET}")
def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"  {C.GRAY}   [{ts}] {msg}{C.RESET}")

def box(title, color=C.WHITE):
    print(f"\n  {color}{C.BOLD}{'=' * 60}")
    print(f"  {title}")
    print(f"  {'=' * 60}{C.RESET}")

def divider():
    print(f"  {C.GRAY}{'-' * 60}{C.RESET}")

class TLSAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        ctx = ssl.create_default_context(cafile=certifi.where())
        pool_kwargs["ssl_context"] = ctx
        super().init_poolmanager(connections, maxsize, block, **pool_kwargs)

def build_session():
    s = requests.Session()
    retry = Retry(total=3, connect=3, read=3, backoff_factor=0.3,
                  status_forcelist=[429, 502, 503, 504], allowed_methods=["GET"])
    s.mount("https://", TLSAdapter(max_retries=retry))
    s.headers.update({"Accept": "application/json"})
    return s

def polite_sleep():
    time.sleep(BASE_DELAY + random.uniform(0, JITTER))

def load_brands():
    conn = psycopg2.connect(**PG)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT бренд, поиск_mit, ключевые_слова FROM dim_brands ORDER BY бренд;")
            rows = cur.fetchall()
    finally:
        conn.close()
    brands = {}
    for бренд, поиск, ключевые in rows:
        kw_list = [k.strip() for k in (ключевые or "").split(",") if k.strip()]
        brands[бренд] = (поиск, kw_list)
    return brands

def ensure_table():
    log("Подключение к БД...")
    conn = psycopg2.connect(**PG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
CREATE TABLE IF NOT EXISTS manufacturers_verifiers (
id             SERIAL PRIMARY KEY,
brand          TEXT,
reg_number     TEXT,
mit_title      TEXT,
verifier       TEXT,
year           INTEGER,
primary_count  INTEGER DEFAULT 0,
periodic_count INTEGER DEFAULT 0,
share_pct      REAL DEFAULT 0,
is_producer    BOOLEAN DEFAULT FALSE,
is_new         BOOLEAN DEFAULT FALSE,
checked_at     TIMESTAMPTZ DEFAULT NOW(),
UNIQUE(reg_number, verifier, year)
);
""")
            cur.execute("ALTER TABLE manufacturers_verifiers ADD COLUMN IF NOT EXISTS year INTEGER;")
            cur.execute("ALTER TABLE manufacturers_verifiers ADD COLUMN IF NOT EXISTS is_producer BOOLEAN DEFAULT FALSE;")
            cur.execute("ALTER TABLE manufacturers_verifiers ADD COLUMN IF NOT EXISTS is_new BOOLEAN DEFAULT FALSE;")
            conn.commit()
    finally:
        conn.close()
    ok("Таблица manufacturers_verifiers готова")

def is_first_run():
    conn = psycopg2.connect(**PG)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM manufacturers_verifiers;")
            count = cur.fetchone()[0]
    finally:
        conn.close()
    log(f"Записей в таблице: {count}")
    return count == 0

def is_reg_number_known(reg_number):
    conn = psycopg2.connect(**PG)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM manufacturers_verifiers WHERE reg_number = %s LIMIT 1", (reg_number,))
            result = cur.fetchone() is not None
    finally:
        conn.close()
    return result

def save_verifiers(brand, reg_number, mit_title, verifier_primary, verifier_periodic, year, first_run):
    if not verifier_primary:
        return 0
    top_verifier = max(verifier_primary, key=verifier_primary.get)
    known = is_reg_number_known(reg_number)
    mark_new = (not first_run) and (not known)
    saved = 0
    conn = psycopg2.connect(**PG)
    try:
        with conn.cursor() as cur:
            for v, p in verifier_primary.items():
                per = verifier_periodic.get(v, 0)
                tot = p + per
                share = (p / tot * 100) if tot > 0 else 0
                if share < THRESHOLD:
                    continue
                is_prod = (v == top_verifier)
                cur.execute("""
INSERT INTO manufacturers_verifiers
(brand, reg_number, mit_title, verifier, year,
primary_count, periodic_count, share_pct, is_producer, is_new, checked_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (reg_number, verifier, year) DO UPDATE SET
brand=EXCLUDED.brand, mit_title=EXCLUDED.mit_title,
primary_count=EXCLUDED.primary_count, periodic_count=EXCLUDED.periodic_count,
share_pct=EXCLUDED.share_pct, is_producer=EXCLUDED.is_producer,
checked_at=NOW();
""", (brand, reg_number, mit_title, v, year, p, per, share, is_prod, mark_new))
                saved += 1
        conn.commit()
    finally:
        conn.close()
    return saved

def get_reg_numbers(session, brand, search_str, keywords):
    mfr_filter = BRAND_MFR_FILTER.get(brand)
    result = []
    skipped = []
    start = 0
    while True:
        try:
            polite_sleep()
            params = {"search": f"*{search_str}*", "rows": 100, "start": start}
            r = session.get(MIT_URL, params=params, timeout=60)
            r.encoding = "utf-8"
            if r.status_code != 200:
                warn(f"MIT API: {r.status_code}")
                break
            data = r.json()
            items = (data.get("result") or {}).get("items") or []
            if not items:
                break
            for item in items:
                mfr = item.get("manufacturers") or ""
                title = (item.get("title") or "").strip()
                num = (item.get("number") or "").strip()
                if not any(kw in mfr for kw in keywords):
                    continue
                if mfr_filter and not mfr_filter(mfr):
                    continue
                if not is_water_meter(title):
                    skipped.append({"number": num, "title": title})
                    continue
                if num:
                    result.append({"number": num, "title": title, "manufacturers": mfr})
            if len(items) < 100:
                break
            start += 100
        except Exception as e:
            warn(f"MIT API: {e}")
            break
    if skipped:
        warn(f"Отфильтровано (не счётчики воды): {len(skipped)}")
    return result

def get_vri_page(session, reg_number, year, start=0, rows=ROWS):
    for attempt in range(1, MAX_TRIES + 1):
        try:
            polite_sleep()
            params = {"mit_number": reg_number, "rows": rows, "start": start, "year": year}
            r = session.get(VRI_URL, params=params, timeout=60)
            r.encoding = "utf-8"
            if r.status_code in (408, 500):
                time.sleep(5 * attempt)
                continue
            if r.status_code != 200:
                time.sleep(3 * attempt)
                continue
            data = r.json()
            result = data.get("result") or {}
            return result.get("items") or [], result.get("count", 0)
        except Exception as e:
            warn(f"VRI: {e}")
            time.sleep(3 * attempt)
    return [], 0

def ping_detail(session, vri_id):
    for attempt in range(1, MAX_TRIES + 1):
        try:
            polite_sleep()
            r = session.get(f"{VRI_URL}/{vri_id}", timeout=60)
            r.encoding = "utf-8"
            if r.status_code != 200:
                time.sleep(3 * attempt)
                continue
            data = r.json()
            return ((data.get("result") or {}).get("vriInfo") or {}).get("vriType")
        except Exception:
            time.sleep(3 * attempt)
    return None

def collect_sample(session, reg_number, year, total_count, sample_size, positions):
    items_per_position = max(1, sample_size // positions)
    items_per_position = min(items_per_position, 100)
    collected = []
    max_safe_start = min(total_count - ROWS, 99000) if total_count > ROWS else 0
    for i in range(positions):
        if total_count <= ROWS:
            start = 0
        elif positions == 1:
            start = 0
        else:
            start = int(i * max_safe_start / (positions - 1))
        items, _ = get_vri_page(session, reg_number, year, start=start, rows=items_per_position)
        collected.extend(items)
        if len(collected) >= sample_size:
            break
    return collected[:sample_size]

def process_reg_number(session, brand, reg_num, mit_title):
    info(f"  Умная выборка: {PINGS_TOTAL} пингов из {SAMPLE_POSITIONS} точек, год {YEAR}")
    _, total_count = get_vri_page(session, reg_num, YEAR, start=0, rows=1)
    info(f"  Всего поверок за {YEAR}: {total_count}")
    if total_count == 0:
        warn("  Нет поверок за этот год")
        return defaultdict(int), defaultdict(int)
    items = collect_sample(session, reg_num, YEAR, total_count, PINGS_TOTAL, SAMPLE_POSITIONS)
    info(f"  Собрано записей в выборку: {len(items)}")
    vp = defaultdict(int)
    vper = defaultdict(int)
    pinged = 0
    none_count = 0
    for idx, item in enumerate(items, 1):
        vid = item.get("vri_id") or item.get("id")
        org = (item.get("org_title") or "").strip()
        if not vid or not org:
            continue
        vt = ping_detail(session, vid)
        pinged += 1
        if vt == "1":
            vp[org] += 1
        elif vt == "2":
            vper[org] += 1
        else:
            none_count += 1
        if idx % 20 == 0:
            p = sum(vp.values())
            per = sum(vper.values())
            print(f"    {C.GRAY}{idx}/{len(items)}: перв={p} период={per} none={none_count}{C.RESET}")
    total_primary = sum(vp.values())
    total_periodic = sum(vper.values())
    total_ok = total_primary + total_periodic
    share = (total_primary / total_ok * 100) if total_ok > 0 else 0
    info(f"  Итог: пропинговано {pinged}, первичных {total_primary}, периодических {total_periodic}, none {none_count}")
    info(f"  Общая доля первичных: {share:.1f}%")
    divider()
    all_v = set(list(vp.keys()) + list(vper.keys()))
    if all_v:
        top = max(vp, key=vp.get) if vp else None
        print(f"    {C.BOLD}{'Поверитель':<45} {'Перв.':>6} {'Пер.':>6} {'Всего':>7} {'Доля':>6}  Статус{C.RESET}")
        print(f"    {'-' * 90}")
        for v in sorted(all_v, key=lambda x: vp.get(x, 0), reverse=True):
            p, per = vp.get(v, 0), vper.get(v, 0)
            tot = p + per
            sh = (p / tot * 100) if tot > 0 else 0
            s = f"{sh:.0f}%" if tot > 0 else "-"
            if sh >= THRESHOLD:
                st = f"{C.GREEN}* ПРОИЗВОДИТЕЛЬ{C.RESET}" if v == top else f"{C.GREEN}>={THRESHOLD}%{C.RESET}"
            elif p > 0:
                st = f"{C.YELLOW}< {THRESHOLD}%{C.RESET}"
            else:
                st = ""
            print(f"    {v[:45]:<45} {C.GREEN}{p:>6}{C.RESET} {C.YELLOW}{per:>6}{C.RESET} {tot:>7} {s:>6}  {st}")
        print()
        if top:
            tt = vp[top] + vper.get(top, 0)
            ts = (vp[top] / tt * 100) if tt > 0 else 0
            if ts >= THRESHOLD:
                ok(f"ПРОИЗВОДИТЕЛЬ: {top} ({vp[top]} перв., {ts:.0f}%)")
            else:
                warn(f"Доля < {THRESHOLD}%: {top} ({ts:.0f}%)")
    else:
        warn("Нет данных")
    return vp, vper

def main():
    print(f"\n{C.MAGENTA}{C.BOLD}")
    print("  ============================================")
    print("  ЭТАП 1: ОПРЕДЕЛЕНИЕ ПРОИЗВОДИТЕЛЕЙ (УМНАЯ ВЫБОРКА)")
    print(f"  ГОД: {YEAR} | Пингов: {PINGS_TOTAL} | Точек: {SAMPLE_POSITIONS} | Порог: >={THRESHOLD}%")
    print("  ============================================\n{C.RESET}")

    started = datetime.now()
    info(f"Старт: {started:%Y-%m-%d %H:%M:%S}")

    session = build_session()
    ok("HTTP сессия")
    ensure_table()

    try:
        brands = load_brands()
    except Exception as e:
        err(f"Не удалось загрузить dim_brands: {e}")
        return
    if not brands:
        err("dim_brands пуста!")
        return
    ok(f"Загружено брендов: {len(brands)}")

    first_run = is_first_run()
    info("Первый запуск" if first_run else "Повторный запуск")

    all_results = []
    for bi, (brand, (ss, kw)) in enumerate(brands.items(), 1):
        box(f"[{bi}/{len(brands)}] {brand}", C.CYAN)
        info(f"MIT API: *{ss}*")
        rns = get_reg_numbers(session, brand, ss, kw)
        if not rns:
            warn("Не найдено")
            continue
        ok(f"Рег.номеров (счётчики воды): {len(rns)}")
        for i, rn in enumerate(rns, 1):
            num, title = rn["number"], rn["title"]
            known = is_reg_number_known(num)
            is_new_reg = (not first_run) and (not known)
            nm = f"  {C.MAGENTA}[НОВЫЙ]{C.RESET}" if is_new_reg else ""
            box(f"[{i}/{len(rns)}]  {num}  -  {title}{nm}", C.WHITE)
            vp, vper = process_reg_number(session, brand, num, title)
            producer = None
            pp, ps = 0, 0
            status = "нет первичных поверок"
            if vp:
                top = max(vp, key=vp.get)
                tp = vp[top]
                tt = tp + vper.get(top, 0)
                ts = (tp / tt * 100) if tt > 0 else 0
                if ts >= THRESHOLD:
                    producer, pp, ps = top, tp, ts
                    status = "ПРОИЗВОДИТЕЛЬ"
            sc = save_verifiers(brand, num, title, vp, vper, YEAR, first_run)
            if sc > 0:
                ok(f"Записано {sc} поверителей с year={YEAR}")
            if producer:
                ok(f"Производитель: {producer} ({pp} перв., {ps:.0f}%)")
            all_results.append({
                "brand": brand, "reg_number": num, "title": title,
                "producer": producer or "-", "primary": pp, "share": ps,
                "status": status, "is_new": is_new_reg
            })

    box(f"ИТОГ (year={YEAR}, pings={PINGS_TOTAL}, pos={SAMPLE_POSITIONS})", C.MAGENTA)
    print()
    print(f"    {C.BOLD}{'Бренд':<14} {'Рег.номер':<14} {'Тип СИ':<28} {'Производитель':<35} {'Перв.':>6} {'Доля':>6}  Статус{C.RESET}")
    print(f"    {'=' * 120}")
    for r in all_results:
        c = C.GREEN if r["status"] == "ПРОИЗВОДИТЕЛЬ" else C.GRAY
        sh = f"{r['share']:.0f}%" if r["share"] > 0 else "-"
        n = " [НОВЫЙ]" if r["is_new"] else ""
        print(f"    {r['brand']:<14} {C.CYAN}{r['reg_number']:<14}{C.RESET} {r['title'][:28]:<28} {c}{r['producer'][:35]:<35}{C.RESET} {r['primary']:>6} {sh:>6}  {c}{'*' if r['status'] == 'ПРОИЗВОДИТЕЛЬ' else '-'} {r['status']}{n}{C.RESET}")
    print(f"    {'=' * 120}\n")

    found = sum(1 for r in all_results if r["status"] == "ПРОИЗВОДИТЕЛЬ")
    new = sum(1 for r in all_results if r["is_new"])
    ok(f"Всего: {len(all_results)} | Производитель: {found} | Новых: {new}")
    ended = datetime.now()
    mins = (ended - started).total_seconds() / 60
    info(f"Время: {mins:.0f} мин")
    print(f"\n  {C.MAGENTA}{C.BOLD}Готово! Данные записаны с year={YEAR}{C.RESET}\n")

if __name__ == "__main__":
    main()
