# -*- coding: utf-8 -*-

# ======================================================
# ЭТАП 3 (ТОЧНАЯ ВЕРСИЯ): ПОДСЧЁТ ОБЪЁМОВ ПО МЕСЯЦАМ
# Скрипт: volumes_exact.py
# Таблица: production_volumes_exact (отдельная от production_volumes)
#
# Запуск: python volumes_exact.py 2024
#
# ОТЛИЧИЯ ОТ volumes2.py:
# 1. Убран лимит min(5, ...) — читаем ВСЕ страницы месяца до конца.
# 2. Классифицируем каждую запись напрямую по mi_modification.
# 3. Если sample_size == month_count — раскладываем точно, без пропорций.
# 4. Добавлена колонка `точность` для контроля:
#    - 'точно'                    — все страницы прочитаны без ошибок
#    - 'пропорция_из_выборки'     — пагинация прервалась, остаток распределён
#                                   пропорционально по уже посчитанным долям
#    - 'из_выборки'              — fallback на упавший месяц (HTTP 500)
#    - 'теплосчётчик'            — без деления (А)
#    - 'одна_категория'          — без деления (Б)
#    - 'не_определено'           — нет диаметров вообще (В)
#
# Можно запускать параллельно с volumes2.py — таблицы разные.
# Потом сравнить production_volumes vs production_volumes_exact.
# ======================================================

import sys
import time
import ssl
import re
import random
from collections import defaultdict
from datetime import datetime

import requests
import certifi
import psycopg2
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===================== НАСТРОЙКИ =====================
BASE = "https://fgis.gost.ru/fundmetrology/eapi"
VRI_URL = f"{BASE}/vri"

PG = dict(host="localhost", port=5432, dbname="postgres", user="postgres", password="1234")

ROWS = 100
BASE_DELAY = 0.6
JITTER = 0.1
MAX_TRIES = 5
MAX_START = 99900

YEAR = int(sys.argv[1]) if len(sys.argv) > 1 else 2025

MONTHS = list(range(1, 13))

VALID_DIAMETERS = {15, 20, 25, 32, 40, 50, 65, 80, 100, 150, 200}

CATEGORY_MAP = {
    15: "Счетчики воды бытовые", 20: "Счетчики воды бытовые",
    25: "Счетчики воды домовые", 32: "Счетчики воды домовые", 40: "Счетчики воды домовые",
    50: "Счетчики воды промышленные", 65: "Счетчики воды промышленные", 80: "Счетчики воды промышленные",
    100: "Счетчики воды промышленные", 150: "Счетчики воды промышленные", 200: "Счетчики воды промышленные",
}

HEAT_METER_CATEGORY = "Теплосчетчики"

MONTH_NAMES = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}

MONTH_LAST_DAY = {
    1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
    7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31,
}

# ===================== ЦВЕТА =====================
class C:
    GREEN = "\033[92m"; YELLOW = "\033[93m"; CYAN = "\033[96m"
    MAGENTA = "\033[95m"; GRAY = "\033[90m"; RED = "\033[91m"
    BOLD = "\033[1m"; RESET = "\033[0m"

def ok(msg):   print(f"  {C.GREEN}v  {msg}{C.RESET}")
def warn(msg): print(f"  {C.YELLOW}!  {msg}{C.RESET}")
def err(msg):  print(f"  {C.RED}x  {msg}{C.RESET}")
def info(msg): print(f"  {C.CYAN}i  {msg}{C.RESET}")
def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"  {C.GRAY}   [{ts}] {msg}{C.RESET}")

def box(title, color=C.CYAN):
    print(f"\n  {color}{C.BOLD}{'=' * 60}\n  {title}\n  {'=' * 60}{C.RESET}")

# ===================== TLS / СЕССИЯ =====================
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

# ===================== БАЗА ДАННЫХ =====================
def ensure_table():
    """Создаёт production_volumes_exact с колонкой точность."""
    conn = psycopg2.connect(**PG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS production_volumes_exact (
                    id                      SERIAL PRIMARY KEY,
                    рег_номер               TEXT,
                    организация_поверитель  TEXT,
                    бренд                   TEXT,
                    наименование_си         TEXT,
                    год                     INTEGER,
                    месяц                   INTEGER,
                    месяц_название          TEXT,
                    категория               TEXT,
                    количество              INTEGER DEFAULT 0,
                    возможные_диаметры      TEXT DEFAULT '',
                    точность                TEXT DEFAULT '',
                    updated_at              TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conname = 'production_volumes_exact_uniq_full'
                    ) THEN
                        ALTER TABLE production_volumes_exact
                        ADD CONSTRAINT production_volumes_exact_uniq_full
                        UNIQUE (рег_номер, год, месяц, категория, организация_поверитель);
                    END IF;
                END$$;
            """)
            conn.commit()
    finally:
        conn.close()
    ok("Таблица production_volumes_exact готова")

def get_reference_data():
    conn = psycopg2.connect(**PG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT dr.рег_номер, dr.бренд, dr.наименование_си,
                       dr.модификация, dr.категория
                FROM dim_reference dr
                WHERE dr.ручное_исправление = '' OR dr.ручное_исправление IS NULL
                ORDER BY dr.бренд, dr.рег_номер, dr.категория;
            """)
            ref_rows = cur.fetchall()

            cur.execute("""
                SELECT reg_number, verifier
                FROM manufacturers_verifiers
                WHERE year = %s;
            """, (YEAR,))
            verifier_rows = cur.fetchall()
    finally:
        conn.close()

    verifier_map = defaultdict(list)
    for reg, ver in verifier_rows:
        verifier_map[reg].append(ver)

    ref_rows = [row for row in ref_rows if row[0] in verifier_map]

    reg_map = {}
    for reg, brand, mit, mod, cat in ref_rows:
        if reg not in reg_map:
            reg_map[reg] = {
                "рег_номер": reg,
                "бренд": brand or "",
                "наименование_си": mit or "",
                "категории": {},
                "все_диаметры": [],
                "теплосчётчик": False,
                "поверители": verifier_map[reg],
            }

        diams = []
        if mod:
            for d in mod.split(","):
                d = d.strip()
                if d:
                    diams.append(d)

        cat_str = str(cat) if cat else ""
        if cat_str:
            reg_map[reg]["категории"][cat_str] = diams
            reg_map[reg]["все_диаметры"].extend(diams)
            if cat_str == HEAT_METER_CATEGORY:
                reg_map[reg]["теплосчётчик"] = True

    return list(reg_map.values())

def save_volume(рег_номер, поверитель, бренд, наименование_си, год, месяц,
                категория, количество, возможные_диаметры, точность):
    conn = psycopg2.connect(**PG)
    try:
        with conn.cursor() as cur:
            месяц_название = MONTH_NAMES.get(месяц, str(месяц))
            cur.execute("""
                INSERT INTO production_volumes_exact
                (рег_номер, организация_поверитель, бренд, наименование_си,
                 год, месяц, месяц_название, категория, количество,
                 возможные_диаметры, точность, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (рег_номер, год, месяц, категория, организация_поверитель) DO UPDATE SET
                    бренд = EXCLUDED.бренд,
                    наименование_си = EXCLUDED.наименование_си,
                    месяц_название = EXCLUDED.месяц_название,
                    количество = EXCLUDED.количество,
                    возможные_диаметры = EXCLUDED.возможные_диаметры,
                    точность = EXCLUDED.точность,
                    updated_at = NOW();
            """, (рег_номер, поверитель, бренд, наименование_си, год, месяц,
                  месяц_название, категория, количество, возможные_диаметры, точность))
            conn.commit()
    finally:
        conn.close()

# ===================== VRI API =====================
def get_vri_page(session, reg_number, year, start=0, rows=ROWS,
                 org_title=None, date_start=None, date_end=None, verbose=False):
    for attempt in range(1, MAX_TRIES + 1):
        try:
            polite_sleep()
            params = {"mit_number": reg_number, "rows": rows, "start": start, "year": year}
            if org_title:
                params["org_title"] = org_title
            if date_start:
                params["verification_date_start"] = date_start
            if date_end:
                params["verification_date_end"] = date_end

            if verbose:
                prepared = requests.Request("GET", VRI_URL, params=params).prepare()
                print(f"    {C.GRAY}-> GET {prepared.url}{C.RESET}")

            r = session.get(VRI_URL, params=params, timeout=60)
            r.encoding = "utf-8"

            if verbose:
                print(f"    {C.GRAY}<- HTTP {r.status_code}{C.RESET}")

            if r.status_code == 500:
                return [], 0, 500
            if r.status_code in (408, 503, 502, 504):
                time.sleep(5 * attempt)
                continue
            if r.status_code != 200:
                time.sleep(3 * attempt)
                continue

            data = r.json()
            result = data.get("result") or {}
            items = result.get("items") or []
            cnt = result.get("count", 0)

            if verbose:
                print(f"    {C.GRAY}  count = {cnt:,}, items = {len(items)}{C.RESET}".replace(",", " "))

            return items, cnt, 200

        except Exception as e:
            warn(f"VRI: {e}")
            time.sleep(3 * attempt)

    return [], 0, 0

def parse_vri_date(date_str):
    try:
        parts = date_str.split(".")
        if len(parts) == 3:
            return int(parts[2]), int(parts[1])
    except:
        pass
    return None, None

def extract_diameter(mod):
    if not mod:
        return None
    nums = re.findall(r"\d+", mod)
    for n in nums:
        d = int(n)
        if d in VALID_DIAMETERS:
            return d
    return None

def get_category(dn):
    if dn is None:
        return "не определено"
    return CATEGORY_MAP.get(dn, "не определено")

def classify_item(item, теплосчётчик, single_category, has_diameters, категории):
    if теплосчётчик:
        return HEAT_METER_CATEGORY
    elif single_category and has_diameters:
        return list(категории.keys())[0]
    elif has_diameters:
        mod = (item.get("mi_modification") or "").strip()
        dn = extract_diameter(mod)
        return get_category(dn)
    else:
        return "не определено"

# ===================== СТРАТЕГИЯ 1: ТОЧНЫЙ ПОДСЧЁТ ПО МЕСЯЦАМ =====================
def strategy_exact_by_months(session, reg_number, year, org_title,
                              теплосчётчик, single_category, has_diameters, категории):
    """
    ТОЧНАЯ ВЕРСИЯ: для каждого месяца читаем ВСЕ страницы (не лимитом 5).
    Возвращает (by_month_cat, accuracy_per_month, failed_months):
      - by_month_cat[мес][cat] = количество
      - accuracy_per_month[мес] = 'точно' / 'остаток_в_последнюю' / 'теплосчётчик' / 'одна_категория' / 'не_определено'
      - failed_months — упавшие месяцы (HTTP 500)
    """
    by_month_cat = defaultdict(lambda: defaultdict(int))
    accuracy_per_month = {}
    failed_months = []
    total = 0

    for мес in MONTHS:
        date_start = f"{year}-{мес:02d}-01"
        date_end = f"{year}-{мес:02d}-{MONTH_LAST_DAY[мес]:02d}"

        items, month_count, status = get_vri_page(
            session, reg_number, year,
            start=0, rows=ROWS,
            org_title=org_title,
            date_start=date_start,
            date_end=date_end,
            verbose=True
        )

        mn_name = MONTH_NAMES[мес][:3]

        if status != 200:
            warn(f"    {mn_name}: HTTP {status} -> месяц помечен failed")
            failed_months.append(мес)
            continue

        log(f"    {mn_name}: count = {month_count:,}".replace(",", " "))

        if month_count == 0:
            continue

        total += month_count

        # === Случай А: теплосчётчик ===
        if теплосчётчик:
            by_month_cat[мес][HEAT_METER_CATEGORY] = month_count
            accuracy_per_month[мес] = "теплосчётчик"
            continue

        # === Случай Б: одна категория у модели ===
        if single_category and has_diameters:
            cat = list(категории.keys())[0]
            by_month_cat[мес][cat] = month_count
            accuracy_per_month[мес] = "одна_категория"
            continue

        # === Случай В: нет диаметров ===
        if not has_diameters:
            by_month_cat[мес]["не определено"] = month_count
            accuracy_per_month[мес] = "не_определено"
            continue

        # === Случай Г: несколько категорий — ТОЧНЫЙ подсчёт ===
        cat_counts = defaultdict(int)
        sample_size = 0

        # Сначала классифицируем уже полученную страницу 0
        for item in items:
            cat = classify_item(item, теплосчётчик, single_category, has_diameters, категории)
            cat_counts[cat] += 1
            sample_size += 1

        # Дочитываем ВСЕ оставшиеся страницы (без лимита min(5, ...))
        pagination_complete = True
        if month_count > ROWS:
            pages_to_read = (month_count + ROWS - 1) // ROWS  # все страницы
            for pg in range(1, pages_to_read):
                start_offset = pg * ROWS
                # Защита от лимита API: start не может быть > 99900
                if start_offset > MAX_START:
                    warn(f"      Пагинация уперлась в лимит API (start > {MAX_START})")
                    pagination_complete = False
                    break

                more_items, _, st = get_vri_page(
                    session, reg_number, year,
                    start=start_offset, rows=ROWS,
                    org_title=org_title,
                    date_start=date_start, date_end=date_end
                )
                if st != 200:
                    warn(f"      Пагинация прервана на странице {pg} (HTTP {st})")
                    pagination_complete = False
                    break
                if not more_items:
                    break
                for item in more_items:
                    cat = classify_item(item, теплосчётчик, single_category, has_diameters, категории)
                    cat_counts[cat] += 1
                    sample_size += 1

        # Если все страницы прочитаны и sample_size == month_count -> точно
        if sample_size == month_count:
            for cat, cnt in cat_counts.items():
                by_month_cat[мес][cat] = cnt
            accuracy_per_month[мес] = "точно"
            log(f"      Точно: {dict(cat_counts)}")
        else:
            # Не дочитали (API оборвал или лимит API).
            # Распределяем month_count пропорционально по уже посчитанным долям.
            cats_list = list(cat_counts.keys())

            if not cats_list or sample_size == 0:
                # Совсем ничего не классифицировали — кладём всё в "не определено"
                by_month_cat[мес]["не определено"] = month_count
                accuracy_per_month[мес] = "пропорция_из_выборки"
                warn(f"      Не дочитали и нечего классифицировать -> всё в 'не определено'")
                continue

            # Доли каждой категории в прочитанной выборке
            distributed = 0
            for idx, cat in enumerate(cats_list):
                if idx == len(cats_list) - 1:
                    # Последней категории отдаём остаток — чтобы сумма точно совпала с month_count
                    add = month_count - distributed
                else:
                    add = round(month_count * cat_counts[cat] / sample_size)
                by_month_cat[мес][cat] = add
                distributed += add
            accuracy_per_month[мес] = "пропорция_из_выборки"
            warn(f"      Прочитано {sample_size}/{month_count} -> остаток {month_count - sample_size} распределён пропорционально")

    if failed_months:
        warn(f"    Упавшие месяцы: {failed_months}")
    ok(f"    Сумма по успешным месяцам: {total:,}".replace(",", " "))
    return by_month_cat, accuracy_per_month, failed_months

# ===================== MAIN =====================
def main():
    box(f"ЭТАП 3 (ТОЧНО): ОБЪЁМЫ (YEAR={YEAR}) — все страницы, без пропорций", C.MAGENTA)
    started = datetime.now()
    info(f"Старт: {started:%Y-%m-%d %H:%M:%S}")
    info(f"Таблица: production_volumes_exact (отдельно от production_volumes)")

    session = build_session()
    ok("HTTP сессия")

    ensure_table()

    reg_data = get_reference_data()
    if not reg_data:
        err("Нет данных в dim_reference (нет рег.номеров в manufacturers_verifiers за этот год)")
        return
    ok(f"Рег.номеров на обработку: {len(reg_data)}")

    total_saved = 0
    total_verifiers_seen = set()
    total_fallback_used = 0
    accuracy_summary = defaultdict(int)

    for i, rd in enumerate(reg_data, 1):
        num = rd["рег_номер"]
        бренд = rd["бренд"]
        наименование = rd["наименование_си"]
        категории = rd["категории"]
        все_диаметры = rd["все_диаметры"]
        теплосчётчик = rd["теплосчётчик"]
        поверители = rd["поверители"]

        box(f"[{i}/{len(reg_data)}] {num} | {бренд} | {наименование[:40]}")

        has_diameters = len(все_диаметры) > 0
        single_category = len(категории) == 1
        возможные = ", ".join(sorted(set(все_диаметры),
                                     key=lambda x: int(re.search(r"\d+", x).group() if re.search(r"\d+", x) else 0)))

        if категории:
            info("Категории из dim_reference:")
            for cat_name, diams in категории.items():
                diams_str = ", ".join(diams) if diams else "—"
                print(f"       {cat_name}: DN {diams_str}")

        info(f"  Поверителей из manufacturers_verifiers: {len(поверители)}")
        for p in поверители:
            print(f"       - {p}")

        # Страховочная выборка для fallback на упавшие месяцы (как в оригинале)
        sample_collected = False
        sample_by_month_cat_org = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

        def ensure_sample_collected():
            nonlocal sample_collected
            if sample_collected:
                return
            info(f"  Собираю общую выборку (для fallback на упавшие месяцы)")
            _, total_count, st = get_vri_page(session, num, YEAR, start=0, rows=1)
            if total_count == 0:
                sample_collected = True
                return

            if total_count <= MAX_START:
                start = 0
                while start < total_count:
                    items, _, st = get_vri_page(session, num, YEAR, start=start)
                    if not items:
                        break
                    for item in items:
                        vd = item.get("verification_date", "")
                        y, m_ = parse_vri_date(vd)
                        if y != YEAR or not m_:
                            continue
                        org_it = (item.get("org_title") or "").strip() or "(неизв)"
                        cat = classify_item(item, теплосчётчик, single_category, has_diameters, категории)
                        sample_by_month_cat_org[m_][cat][org_it] += 1
                    start += ROWS
            else:
                SAMPLE_POINTS = 5
                ROWS_PER_POINT = MAX_START // SAMPLE_POINTS
                max_safe_start = min(total_count - ROWS_PER_POINT, MAX_START - ROWS_PER_POINT)
                sample_starts = [int(ip * max_safe_start / (SAMPLE_POINTS - 1)) for ip in range(SAMPLE_POINTS)]
                log(f"    Точки выборки: {sample_starts}")
                for pt_start in sample_starts:
                    s_ = pt_start
                    end_s = pt_start + ROWS_PER_POINT
                    while s_ < end_s:
                        items, _, st = get_vri_page(session, num, YEAR, start=s_)
                        if not items:
                            break
                        for item in items:
                            vd = item.get("verification_date", "")
                            y, m_ = parse_vri_date(vd)
                            if y != YEAR or not m_:
                                continue
                            org_it = (item.get("org_title") or "").strip() or "(неизв)"
                            cat = classify_item(item, теплосчётчик, single_category, has_diameters, категории)
                            sample_by_month_cat_org[m_][cat][org_it] += 1
                        s_ += ROWS
            sample_collected = True
            ok(f"    Выборка собрана")

        saved = 0
        ver_totals = defaultdict(int)

        for v_idx, org in enumerate(поверители, 1):
            log(f"  [{v_idx}/{len(поверители)}] Поверитель: {org[:60]}")

            # Быстрая проверка — есть ли вообще данные за год
            _, quick_year_count, quick_st = get_vri_page(
                session, num, YEAR, start=0, rows=1,
                org_title=org, verbose=True
            )
            if quick_st == 200 and quick_year_count < 100:
                warn(f"    Годовой count = {quick_year_count} < 100 -> пропускаю поверителя")
                continue

            by_month_cat, accuracy_per_month, failed_months = strategy_exact_by_months(
                session, num, YEAR, org,
                теплосчётчик, single_category, has_diameters, категории
            )

            # Fallback на упавшие месяцы
            if failed_months:
                total_fallback_used += 1
                warn(f"    Есть упавшие месяцы: {failed_months} -> добираю через выборку")
                ensure_sample_collected()

                _, exact_year_count, st = get_vri_page(
                    session, num, YEAR, start=0, rows=1,
                    org_title=org, verbose=True
                )

                if st == 200 and exact_year_count > 0:
                    success_sum = 0
                    for мес in MONTHS:
                        if мес in failed_months:
                            continue
                        for cat_cnt in by_month_cat.get(мес, {}).values():
                            success_sum += cat_cnt

                    remainder = exact_year_count - success_sum
                    info(f"    Годовой count={exact_year_count:,}, успешные={success_sum:,}, остаток={remainder:,} на {len(failed_months)} мес".replace(",", " "))

                    if remainder > 0:
                        failed_sample_month = defaultdict(lambda: defaultdict(int))
                        failed_sample_total = 0
                        for мес in failed_months:
                            for cat in sample_by_month_cat_org.get(мес, {}):
                                val = sample_by_month_cat_org[мес][cat].get(org, 0)
                                if val > 0:
                                    failed_sample_month[мес][cat] = val
                                    failed_sample_total += val

                        if failed_sample_total > 0:
                            ratio = remainder / failed_sample_total
                            for мес in failed_sample_month:
                                for cat in failed_sample_month[мес]:
                                    by_month_cat[мес][cat] = round(failed_sample_month[мес][cat] * ratio)
                                    accuracy_per_month[мес] = "из_выборки"
                            ok(f"    Упавшие месяцы распределены через выборку (x{ratio:.2f})")
                        else:
                            per_month = remainder // len(failed_months)
                            for мес in failed_months:
                                if категории:
                                    cat = list(категории.keys())[0]
                                else:
                                    cat = "не определено"
                                by_month_cat[мес][cat] = per_month
                                accuracy_per_month[мес] = "из_выборки"
                            warn(f"    Выборка не содержит упавшие месяцы — распределено равномерно")
                else:
                    warn(f"    Не получили годовой count — упавшие месяцы пропущены")

            if not by_month_cat:
                warn(f"    Нет данных для {org[:40]}")
                continue

            for месяц in MONTHS:
                acc = accuracy_per_month.get(месяц, "")
                for cat, kol in by_month_cat.get(месяц, {}).items():
                    if kol > 0:
                        save_volume(num, org, бренд, наименование, YEAR,
                                    месяц, str(cat), kol, возможные, acc)
                        saved += 1
                        ver_totals[org] += kol
                        total_verifiers_seen.add(org)
                        accuracy_summary[acc] += 1

        total_saved += saved
        ok(f"  Сохранено строк: {saved} | Поверителей: {len(ver_totals)}")

        if ver_totals:
            print(f"\n    {C.BOLD}Итоги по поверителям:{C.RESET}")
            for org, cnt in sorted(ver_totals.items(), key=lambda x: x[1], reverse=True):
                print(f"      {org[:55]:<55} {cnt:>10,}".replace(",", " "))

    box("ИТОГ", C.MAGENTA)
    ok(f"Всего строк в production_volumes_exact: {total_saved}")
    ok(f"Рег.номеров обработано: {len(reg_data)}")
    ok(f"Уникальных поверителей: {len(total_verifiers_seen)}")
    ok(f"Fallback использован раз: {total_fallback_used}")

    print(f"\n  {C.BOLD}Распределение по точности:{C.RESET}")
    for acc, cnt in sorted(accuracy_summary.items(), key=lambda x: x[1], reverse=True):
        print(f"    {acc:<30} {cnt:>8}")

    ok(f"Год: {YEAR}")

    ended = datetime.now()
    mins = (ended - started).total_seconds() / 60
    info(f"Время: {mins:.1f} мин")

    print(f"\n  {C.MAGENTA}{C.BOLD}Готово! Точные данные за {YEAR} в production_volumes_exact.{C.RESET}\n")

if __name__ == "__main__":
    main()
