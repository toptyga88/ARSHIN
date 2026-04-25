# -*- coding: utf-8 -*-

# ======================================================

# ПОЛНОЕ ОБОГАЩЕНИЕ ДАННЫХ

# Скрипт: enrichment_full.py

# Таблица: dim_reference

#

# Колонки:

# рег_номер, организация_поверитель, бренд, наименование_си,

# обозначение, модификация (=диаметры), категория,

# тип_счетчика, умный, мокроходный, многоструйный,

# источник, ручное_исправление

# ======================================================

import time
import ssl
import re
import random
import io
from collections import defaultdict
from datetime import datetime

import requests
import certifi
import psycopg2
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import fitz
    PDF_LIB = "pymupdf"
except ImportError:
    try:
        from pdfminer.high_level import extract_text_from_fileobj
        PDF_LIB = "pdfminer"
    except ImportError:
        PDF_LIB = None

# ===================== НАСТРОЙКИ =====================

BASE = "https://fgis.gost.ru/fundmetrology/eapi"
MIT_URL = f"{BASE}/mit"
VRI_URL = f"{BASE}/vri"

PG = dict(host="localhost", port=5432, dbname="postgres", user="postgres", password="1234")

BASE_DELAY = 1.0
JITTER = 0.1
MAX_TRIES = 5
VRI_SAMPLE_ROWS = 50
VRI_MAX_PAGES = 5

VALID_DIAMETERS = {15, 20, 25, 32, 40, 50, 65, 80, 100, 150, 200}

CATEGORY_MAP = {
    15: "бытовой", 20: "бытовой",
    25: "домовой", 32: "домовой", 40: "домовой",
    50: "промышленный", 65: "промышленный", 80: "промышленный",
    100: "промышленный", 150: "промышленный", 200: "промышленный",
}

CATEGORY_NAMES = {
    "бытовой": "Счетчики воды бытовые",
    "домовой": "Счетчики воды домовые",
    "промышленный": "Счетчики воды промышленные",
}

HEAT_METER_CATEGORY = "Теплосчетчики"

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

def box(title, color=C.CYAN):
    print(f"\n  {color}{C.BOLD}{'=' * 60}")
    print(f"  {title}")
    print(f"  {'=' * 60}{C.RESET}")

# ===================== TLS / СЕССИЯ =====================

class TLSAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        ctx = ssl.create_default_context(cafile=certifi.where())
        pool_kwargs["ssl_context"] = ctx
        super().init_poolmanager(connections, maxsize, block, **pool_kwargs)

def build_session():
    s = requests.Session()
    retry = Retry(
        total=3, connect=3, read=3, backoff_factor=0.3,
        status_forcelist=[429, 502, 503, 504], allowed_methods=["GET"]
    )
    s.mount("https://", TLSAdapter(max_retries=retry))
    s.headers.update({"Accept": "application/json"})
    return s

def polite_sleep():
    time.sleep(BASE_DELAY + random.uniform(0, JITTER))

# ===================== ОПРЕДЕЛЕНИЕ ТЕПЛОСЧЕТЧИКА =====================

def is_heat_meter(mit_title):
    """Определяет, является ли СИ теплосчётчиком по названию."""
    t = (mit_title or "").lower()
    return (
        "теплосчет" in t or "теплосчёт" in t
        or "тепловычислитель" in t or "счетчик тепл" in t
        or "счётчик тепл" in t
    )

# ===================== ПАРСИНГ ХАРАКТЕРИСТИК ИЗ НАЗВАНИЯ =====================

def parse_meter_properties(mit_title):
    t = (mit_title or "").lower()
    тип_счетчика = ""

    if "крыльчат" in t:
        тип_счетчика = "крыльчатый"
    elif "турбин" in t:
        тип_счетчика = "турбинный"
    elif "ультразвук" in t:
        тип_счетчика = "ультразвуковой"

    умный = ""
    if "электрон" in t or "цифров" in t:
        умный = "да"

    мокроходный = ""
    if "мокроход" in t:
        мокроходный = "да"
    elif t:
        мокроходный = "нет"

    многоструйный = ""
    if "многоструйн" in t:
        многоструйный = "да"
    elif t:
        многоструйный = "нет"

    return тип_счетчика, умный, мокроходный, многоструйный

# ===================== БАЗА ДАННЫХ =====================

def ensure_table():
    conn = psycopg2.connect(**PG)
    deleted = 0
    try:
        with conn.cursor() as cur:
            cur.execute("""
CREATE TABLE IF NOT EXISTS dim_reference (
id                      SERIAL PRIMARY KEY,
рег_номер               TEXT,
организация_поверитель  TEXT,
бренд                   TEXT,
наименование_си         TEXT,
обозначение             TEXT,
модификация             TEXT,
категория               TEXT,
тип_счетчика            TEXT DEFAULT '',
умный                   TEXT DEFAULT '',
мокроходный             TEXT DEFAULT '',
многоструйный           TEXT DEFAULT '',
источник                TEXT DEFAULT '',
ручное_исправление      TEXT DEFAULT '',
updated_at              TIMESTAMPTZ DEFAULT NOW(),
UNIQUE(рег_номер, категория)
);
""")
            cur.execute("""
DELETE FROM dim_reference
WHERE ручное_исправление = '' OR ручное_исправление IS NULL;
""")
            deleted = cur.rowcount
            conn.commit()
    finally:
        conn.close()
    ok(f"Таблица dim_reference готова (удалено автозаполненных: {deleted})")

def get_reg_numbers_with_verifiers():
    conn = psycopg2.connect(**PG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
SELECT DISTINCT reg_number, brand, mit_title, verifier
FROM manufacturers_verifiers
WHERE is_producer = TRUE
ORDER BY brand, reg_number;
""")
            rows = cur.fetchall()
    finally:
        conn.close()
    return [
        {"reg_number": r[0], "brand": r[1], "mit_title": r[2], "verifier": r[3]}
        for r in rows
    ]

def save_reference(data):
    conn = psycopg2.connect(**PG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
INSERT INTO dim_reference
(рег_номер, организация_поверитель, бренд, наименование_си, обозначение,
модификация, категория, тип_счетчика, умный, мокроходный, многоструйный,
источник, ручное_исправление, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '', NOW())
ON CONFLICT (рег_номер, категория) DO UPDATE SET
организация_поверитель = EXCLUDED.организация_поверитель,
бренд = EXCLUDED.бренд,
наименование_си = EXCLUDED.наименование_си,
обозначение = EXCLUDED.обозначение,
модификация = EXCLUDED.модификация,
тип_счетчика = EXCLUDED.тип_счетчика,
умный = EXCLUDED.умный,
мокроходный = EXCLUDED.мокроходный,
многоструйный = EXCLUDED.многоструйный,
источник = EXCLUDED.источник,
updated_at = NOW()
WHERE dim_reference.ручное_исправление = '' OR dim_reference.ручное_исправление IS NULL;
""", (
                data["рег_номер"], data["организация_поверитель"], data["бренд"],
                data["наименование_си"], data["обозначение"], data["модификация"],
                data["категория"], data["тип_счетчика"], data["умный"],
                data["мокроходный"], data["многоструйный"], data["источник"]
            ))
            conn.commit()
    finally:
        conn.close()

def save_manual_needed(data):
    conn = psycopg2.connect(**PG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
INSERT INTO dim_reference
(рег_номер, организация_поверитель, бренд, наименование_си, обозначение,
модификация, категория, тип_счетчика, умный, мокроходный, многоструйный,
источник, ручное_исправление, updated_at)
VALUES (%s, %s, %s, %s, %s, '', '', %s, %s, %s, %s, '', %s, NOW())
ON CONFLICT (рег_номер, категория) DO NOTHING;
""", (
                data["рег_номер"], data["организация_поверитель"], data["бренд"],
                data["наименование_си"], data["обозначение"],
                data["тип_счетчика"], data["умный"], data["мокроходный"],
                data["многоструйный"], "ручное исправление"
            ))
            conn.commit()
    finally:
        conn.close()

# ===================== MIT API =====================

def get_mit_uuid(session, reg_number):
    for attempt in range(1, MAX_TRIES + 1):
        try:
            polite_sleep()
            r = session.get(MIT_URL, params={"number": reg_number, "rows": 1}, timeout=60)
            r.encoding = "utf-8"
            if r.status_code != 200:
                time.sleep(3 * attempt)
                continue
            items = (r.json().get("result") or {}).get("items") or []
            return items[0].get("mit_uuid") if items else None
        except Exception as e:
            warn(f"MIT: {e}")
            time.sleep(3 * attempt)
    return None

def get_mit_detail(session, mit_uuid):
    for attempt in range(1, MAX_TRIES + 1):
        try:
            polite_sleep()
            r = session.get(f"{MIT_URL}/{mit_uuid}", timeout=60)
            r.encoding = "utf-8"
            if r.status_code != 200:
                time.sleep(3 * attempt)
                continue
            return r.json()
        except Exception as e:
            warn(f"MIT DETAIL: {e}")
            time.sleep(3 * attempt)
    return None

def parse_notation(data):
    general = data.get("general") or {}
    notation_raw = general.get("notation") or []
    return ", ".join(str(n) for n in notation_raw) if isinstance(notation_raw, list) else str(notation_raw)

def get_pdf_url(session, mit_uuid):
    for attempt in range(1, MAX_TRIES + 1):
        try:
            polite_sleep()
            r = session.get(f"{MIT_URL}/{mit_uuid}", timeout=60)
            r.encoding = "utf-8"
            if r.status_code != 200:
                time.sleep(3 * attempt)
                continue
            data = r.json()
            for s in (data.get("spec") or []):
                if s.get("doc_url"):
                    return s["doc_url"]
            for m in (data.get("meth") or []):
                if m.get("doc_url"):
                    return m["doc_url"]
            return None
        except Exception:
            time.sleep(3 * attempt)
    return None

def download_pdf(session, pdf_url):
    for attempt in range(1, MAX_TRIES + 1):
        try:
            polite_sleep()
            r = session.get(pdf_url, timeout=120)
            if r.status_code == 200:
                return r.content
            time.sleep(3 * attempt)
        except Exception:
            time.sleep(3 * attempt)
    return None

# ===================== ПАРСИНГ ДИАМЕТРОВ =====================

def extract_text_from_pdf(pdf_bytes):
    if PDF_LIB == "pymupdf":
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        text = "".join(page.get_text() for page in doc)
        doc.close()
        return text
    elif PDF_LIB == "pdfminer":
        return extract_text_from_fileobj(io.BytesIO(pdf_bytes))
    return ""

def parse_diameters_from_pdf(text):
    found = set()

    def _add_int(m):
        if isinstance(m, tuple):
            for x in m:
                if x:
                    try:
                        v = int(x)
                        if v in VALID_DIAMETERS:
                            found.add(v)
                    except (ValueError, TypeError):
                        pass
        else:
            try:
                v = int(m)
                if v in VALID_DIAMETERS:
                    found.add(v)
            except (ValueError, TypeError):
                pass

    for m in re.findall(r"DN\s*(\d{2,3})", text, re.IGNORECASE):
        _add_int(m)
    for m in re.findall(r"\((\d{2,3})\)", text):
        _add_int(m)
    for m in re.findall(r"[Дд][Уу]\s*(\d{2,3})", text):
        _add_int(m)
    for m in re.findall(r"(\d{2,3})\s*/\s*(\d{2,3})", text):
        for vs in m:
            _add_int(vs)

    section = re.search(r"(?:диаметр|DN|Ду|номинальный).*?(?:\n.*?){0,3}", text, re.IGNORECASE)
    if section:
        for m in re.findall(r"(\d{2,3})", section.group()):
            _add_int(m)

    return sorted(found)

def collect_modifications_from_vri(session, reg_number):
    modifications = set()
    start = 0

    for page in range(VRI_MAX_PAGES):
        for attempt in range(1, MAX_TRIES + 1):
            try:
                polite_sleep()
                r = session.get(
                    VRI_URL,
                    params={"mit_number": reg_number, "rows": VRI_SAMPLE_ROWS, "start": start},
                    timeout=60
                )
                r.encoding = "utf-8"
                if r.status_code == 408:
                    time.sleep(5 * attempt)
                    continue
                if r.status_code != 200:
                    time.sleep(3 * attempt)
                    continue

                items = (r.json().get("result") or {}).get("items") or []
                if not items:
                    return modifications

                for item in items:
                    mod = (item.get("mi_modification") or "").strip()
                    if mod:
                        modifications.add(mod)

                if len(items) < VRI_SAMPLE_ROWS:
                    return modifications

                start += VRI_SAMPLE_ROWS
                break
            except Exception:
                time.sleep(3 * attempt)
        else:
            break

    return modifications

def parse_diameters_from_modifications(modifications):
    found = set()
    for mod in modifications:
        for m in re.findall(r"(\d{2,3})\s*/\s*(\d{2,3})", mod):
            for vs in m:
                v = int(vs)
                if v in VALID_DIAMETERS:
                    found.add(v)

        for m in re.findall(r"[А-Яа-яA-Za-z][-](\d{2,3})", mod):
            v = int(m)
            if v in VALID_DIAMETERS:
                found.add(v)

        for m in re.findall(r"[А-Яа-яA-Za-z]\s(\d{2,3})", mod):
            v = int(m)
            if v in VALID_DIAMETERS:
                found.add(v)

        for m in re.findall(r"(?:DN|[Дд][Уу])\s*(\d{2,3})", mod, re.IGNORECASE):
            v = int(m)
            if v in VALID_DIAMETERS:
                found.add(v)

        m = re.match(r"^(\d{2,3})", mod.strip())
        if m:
            v = int(m.group(1))
            if v in VALID_DIAMETERS:
                found.add(v)

    return sorted(found)

def parse_diameters_from_title(mit_title):
    if not mit_title:
        return []
    t = mit_title.lower()
    found = set()

    for m in re.findall(r"(?:DN|[Дд][Уу])\s*(\d{2,3})", mit_title, re.IGNORECASE):
        v = int(m)
        if v in VALID_DIAMETERS:
            found.add(v)

    if found:
        return sorted(found)

    if any(kw in t for kw in ["малогабарит", "бытов", "квартир"]):
        return [15, 20]
    elif any(kw in t for kw in ["турбин", "домов", "общедомов"]):
        return [25, 32, 40]
    elif any(kw in t for kw in ["промышлен", "фланц"]):
        return [50, 65, 80, 100, 150, 200]
    elif "комбинирован" in t:
        return [15, 50]
    return []

def group_by_category(diameters):
    by_cat = defaultdict(list)
    for dn in diameters:
        cat = CATEGORY_MAP.get(dn)
        if cat:
            dn_str = str(dn)
            if dn_str not in by_cat[cat]:
                by_cat[cat].append(dn_str)

    result = {}
    for cat, dns in by_cat.items():
        try:
            dns_sorted = sorted(dns, key=lambda x: int(x))
        except Exception:
            dns_sorted = sorted(dns)
        cat_name = CATEGORY_NAMES.get(cat, cat)
        result[cat_name] = ", ".join(dns_sorted)
    return result

def diameters_to_string(diameters):
    """Просто соединяет диаметры через запятую (для теплосчётчиков)."""
    try:
        dns_sorted = sorted(diameters, key=lambda x: int(x))
    except Exception:
        dns_sorted = sorted(diameters)
    return ", ".join(str(d) for d in dns_sorted)

# ===================== MAIN =====================

def main():
    print(f"\n{C.MAGENTA}{C.BOLD}")
    print("  ============================================")
    print("  ПОЛНОЕ ОБОГАЩЕНИЕ ДАННЫХ")
    print("  Скрипт: enrichment_full.py")
    print("  Таблица: dim_reference")
    print("  ============================================\n" + C.RESET)

    if PDF_LIB is None:
        warn("Нет библиотеки PDF - шаг PDF будет пропущен")
    else:
        info(f"PDF библиотека: {PDF_LIB}")

    started = datetime.now()
    session = build_session()
    ok("HTTP сессия")

    ensure_table()

    rows = get_reg_numbers_with_verifiers()
    if not rows:
        err("Нет данных в manufacturers_verifiers!")
        return

    reg_map = {}
    for r in rows:
        num = r["reg_number"]
        if num not in reg_map:
            reg_map[num] = r

    ok(f"Рег.номеров: {len(reg_map)}")

    stats = {"total": 0, "pdf": 0, "vri": 0, "mit_title": 0, "manual": 0, "heat": 0}

    for i, (num, rn) in enumerate(reg_map.items(), 1):
        brand = rn["brand"]
        mit_title = rn["mit_title"] or ""
        verifier = rn["verifier"] or ""

        box(f"[{i}/{len(reg_map)}] {brand} | {num}", C.CYAN)
        info(f"{mit_title[:60]}")
        info(f"Производитель: {verifier[:50]}")

        тип_счетчика, умный, мокроходный, многоструйный = parse_meter_properties(mit_title)
        if тип_счетчика:
            info(f"Тип: {тип_счетчика}")
        if умный:
            info(f"Умный: {умный}")

        notation = ""
        mit_uuid = get_mit_uuid(session, num)
        if mit_uuid:
            detail = get_mit_detail(session, mit_uuid)
            if detail:
                notation = parse_notation(detail)
                ok(f"Обозначение: {notation[:50]}")

        diameters = []
        source = ""

        if mit_uuid and PDF_LIB:
            pdf_url = get_pdf_url(session, mit_uuid)
            if pdf_url:
                pdf_bytes = download_pdf(session, pdf_url)
                if pdf_bytes:
                    text = extract_text_from_pdf(pdf_bytes)
                    if text:
                        diameters = parse_diameters_from_pdf(text)
                        if diameters:
                            source = "pdf"
                            ok(f"PDF -> {len(diameters)} диаметров")

        if not diameters:
            info("PDF не дал -> пробуем VRI...")
            mods = collect_modifications_from_vri(session, num)
            if mods:
                diameters = parse_diameters_from_modifications(mods)
                if diameters:
                    source = "vri"
                    ok(f"VRI -> {len(diameters)} диаметров")

        if not diameters:
            info("VRI не дал -> пробуем mit_title...")
            diameters = parse_diameters_from_title(mit_title)
            if diameters:
                source = "mit_title"
                warn(f"mit_title -> {len(diameters)} диаметров (приблизительно)")

        if is_heat_meter(mit_title):
            diams_str = diameters_to_string(diameters) if diameters else ""
            save_reference({
                "рег_номер": num,
                "организация_поверитель": verifier,
                "бренд": brand,
                "наименование_си": mit_title,
                "обозначение": notation,
                "модификация": diams_str,
                "категория": HEAT_METER_CATEGORY,
                "тип_счетчика": тип_счетчика,
                "умный": умный,
                "мокроходный": мокроходный,
                "многоструйный": многоструйный,
                "источник": source or "mit_title",
            })
            stats["total"] += 1
            stats["heat"] += 1
            if source:
                stats[source] += 1
            print(f"      {C.YELLOW}{HEAT_METER_CATEGORY}: {diams_str or '(диаметры не найдены)'}{C.RESET}")
            continue

        if diameters:
            grouped = group_by_category(diameters)
            for cat_name, diams_str in grouped.items():
                save_reference({
                    "рег_номер": num,
                    "организация_поверитель": verifier,
                    "бренд": brand,
                    "наименование_си": mit_title,
                    "обозначение": notation,
                    "модификация": diams_str,
                    "категория": cat_name,
                    "тип_счетчика": тип_счетчика,
                    "умный": умный,
                    "мокроходный": мокроходный,
                    "многоструйный": многоструйный,
                    "источник": source,
                })
                stats["total"] += 1
                color = C.GREEN if "бытов" in cat_name else (C.CYAN if "домов" in cat_name else C.MAGENTA)
                print(f"      {color}{cat_name}: {diams_str}{C.RESET}")
            if source:
                stats[source] += 1
        else:
            save_manual_needed({
                "рег_номер": num,
                "организация_поверитель": verifier,
                "бренд": brand,
                "наименование_си": mit_title,
                "обозначение": notation,
                "тип_счетчика": тип_счетчика,
                "умный": умный,
                "мокроходный": мокроходный,
                "многоструйный": многоструйный,
            })
            stats["manual"] += 1
            stats["total"] += 1
            err("Не найдено -> ручное исправление")

    box("ИТОГ", C.MAGENTA)
    ok(f"Строк в dim_reference: {stats['total']}")
    if stats["heat"]:
        ok(f"  Теплосчётчиков: {stats['heat']}")
    if stats["pdf"]:
        ok(f"  Из PDF:        {stats['pdf']} рег.номеров")
    if stats["vri"]:
        ok(f"  Из VRI:        {stats['vri']} рег.номеров")
    if stats["mit_title"]:
        warn(f"  Из mit_title:  {stats['mit_title']} (приблизительно)")
    if stats["manual"]:
        err(f"  Ручное исправление: {stats['manual']}")

    print()
    conn = psycopg2.connect(**PG)
    try:
        cur = conn.cursor()
        cur.execute("""
SELECT рег_номер, организация_поверитель, бренд, наименование_си,
       модификация, категория, тип_счетчика, умный, многоструйный, мокроходный, ручное_исправление
FROM dim_reference ORDER BY бренд, рег_номер, категория;
""")
        rows = cur.fetchall()
        print(f"  {C.BOLD}{'Рег.номер':<12} {'Поверитель':<25} {'Бренд':<12} {'Тип СИ':<24} {'Модиф.':<18} {'Категория':<24} {'Тип':<14} {'Умн':<4} {'Мн.стр':<7} {'Мокр':<5} {'Ручное'}{C.RESET}")
        print(f"  {'-' * 155}")
        for reg, ver, brand, mit, mod, cat, typ, smart, multi, wet, manual in rows:
            manual_mark = f"{C.RED}{manual}{C.RESET}" if manual else ""
            if "бытов" in (cat or ""):
                color = C.GREEN
            elif "домов" in (cat or ""):
                color = C.CYAN
            elif "промышл" in (cat or ""):
                color = C.MAGENTA
            elif "епло" in (cat or ""):
                color = C.YELLOW
            else:
                color = C.GRAY
            print(f"  {reg:<12} {(ver or '')[:23]:<25} {(brand or '')[:12]:<12} {(mit or '')[:22]:<24} {(mod or '')[:16]:<18} {color}{(cat or '')[:22]:<24}{C.RESET} {(typ or '')[:12]:<14} {(smart or ''):<4} {(multi or ''):<7} {(wet or ''):<5} {manual_mark}")
    finally:
        conn.close()

    ended = datetime.now()
    mins = (ended - started).total_seconds() / 60
    print()
    info(f"Время: {mins:.1f} мин")
    print(f"\n  {C.MAGENTA}{C.BOLD}Готово!{C.RESET}")
    print(f"  {C.GRAY}SELECT * FROM dim_reference ORDER BY бренд, рег_номер;")
    print(f"  SELECT * FROM dim_reference WHERE ручное_исправление != '';{C.RESET}\n")

if __name__ == "__main__":
    main()
