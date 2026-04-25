# -*- coding: utf-8 -*-

# ============================================

# ОРКЕСТРАТОР: запускает весь пайплайн

# Скрипт: runall.py

# ============================================

#

# ЛОГИКА:

# 1. dimbrand.py      -> dim_brands

# 2. новыйпервич.py   -> manufacturers_verifiers (по каждому году)

# 3. fullenrich.py    -> dim_reference

# 4. volumes2.py      -> production_volumes (по каждому году)

#

# НАСТРОЙКИ в начале файла – меняй там и всё поедет автоматически.

import subprocess
import sys
from datetime import datetime

# ============ НАСТРОЙКИ ============

# Годы за которые прогонять (новыйпервич.py и volumes2.py)

YEARS = [2023,2024,2025]

# Параметры для новыйпервич.py

PINGS_TOTAL = 90         # пингов на рег.номер (умная выборка)
SAMPLE_POSITIONS = 6     # точек выборки
THRESHOLD = 70           # порог доли первичных, %

# Какие этапы запускать (True = запустить, False = пропустить)

RUN_DIMBRAND = True      # Шаг 0: справочник брендов (редко меняется)
RUN_PERVICH = True       # Шаг 1: производители
RUN_ENRICH = True         # Шаг 2: справочник рег.номеров
RUN_VOLUMES = True        # Шаг 3: объёмы по месяцам

# ============ КОНСТАНТЫ ============

PYTHON = sys.executable or "python"

SCRIPTS = {
    "dimbrand": "dimbrand.py",
    "pervich": "Обновлпервич.py",
    "enrich": "енрич.py",
    "volumes": "вольюмс.py",
}

# ============ ЦВЕТА ============

class C:
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    CYAN = "\033[96m"
    MAGENTA = "\033[95m"
    GRAY = "\033[90m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    RESET = "\033[0m"

def banner(title, color=C.MAGENTA):
    print(f"\n{color}{C.BOLD}{'#' * 70}")
    print(f"#  {title}")
    print(f"{'#' * 70}{C.RESET}\n")

def ok(msg):   print(f"  {C.GREEN}v  {msg}{C.RESET}")
def warn(msg): print(f"  {C.YELLOW}!  {msg}{C.RESET}")
def err(msg):  print(f"  {C.RED}x  {msg}{C.RESET}")
def info(msg): print(f"  {C.CYAN}i  {msg}{C.RESET}")

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"  {C.GRAY}   [{ts}] {msg}{C.RESET}")

# ============ ЗАПУСК СКРИПТА ============

def run_script(script_name, args=None):
    args = args or []
    cmd = [PYTHON, script_name] + [str(a) for a in args]
    log(f"Команда: {' '.join(cmd)}")
    started = datetime.now()
    try:
        result = subprocess.run(cmd, check=False)
        ended = datetime.now()
        mins = (ended - started).total_seconds() / 60
        if result.returncode == 0:
            ok(f"{script_name} завершён успешно за {mins:.1f} мин")
            return True
        else:
            err(f"{script_name} завершён с ошибкой (код {result.returncode}), {mins:.1f} мин")
            return False
    except FileNotFoundError:
        err(f"Не найден файл: {script_name}")
        return False
    except Exception as e:
        err(f"Ошибка при запуске {script_name}: {e}")
        return False

# ============ MAIN ============

def main():
    banner("ОРКЕСТРАТОР ПАЙПЛАЙНА")
    started = datetime.now()
    info(f"Старт: {started:%Y-%m-%d %H:%M:%S}")
    info(f"Годы: {YEARS}")
    info(f"Пингов на рег.номер: {PINGS_TOTAL}")
    info(f"Точек выборки: {SAMPLE_POSITIONS}")
    info(f"Порог: >={THRESHOLD}%")
    print()

    # ============ ШАГ 0: dim_brands ============
    if RUN_DIMBRAND:
        banner("ШАГ 0: dim_brands (dimbrand.py)", C.CYAN)
        if not run_script(SCRIPTS["dimbrand"]):
            err("Шаг 0 провалился — прерываю пайплайн")
            return
    else:
        warn("Шаг 0 пропущен (RUN_DIMBRAND=False)")

    # ============ ШАГ 1: manufacturers_verifiers ============
    if RUN_PERVICH:
        for year in YEARS:
            banner(f"ШАГ 1: manufacturers_verifiers — ГОД {year}", C.CYAN)
            args = [
                year,
                "--pings", PINGS_TOTAL,
                "--positions", SAMPLE_POSITIONS,
                "--threshold", THRESHOLD,
            ]
            if not run_script(SCRIPTS["pervich"], args):
                err(f"Шаг 1 за {year} провалился — прерываю пайплайн")
                return
    else:
        warn("Шаг 1 пропущен (RUN_PERVICH=False)")

    # ============ ШАГ 2: dim_reference ============
    if RUN_ENRICH:
        banner("ШАГ 2: dim_reference (fullenrich.py)", C.CYAN)
        if not run_script(SCRIPTS["enrich"]):
            err("Шаг 2 провалился — прерываю пайплайн")
            return
    else:
        warn("Шаг 2 пропущен (RUN_ENRICH=False)")

    # ============ ШАГ 3: production_volumes ============
    if RUN_VOLUMES:
        for year in YEARS:
            banner(f"ШАГ 3: production_volumes — ГОД {year}", C.CYAN)
            if not run_script(SCRIPTS["volumes"], [year]):
                err(f"Шаг 3 за {year} провалился — прерываю пайплайн")
                return
    else:
        warn("Шаг 3 пропущен (RUN_VOLUMES=False)")

    # ============ ИТОГ ============
    ended = datetime.now()
    mins = (ended - started).total_seconds() / 60
    banner(f"ПАЙПЛАЙН ЗАВЕРШЁН за {mins:.0f} мин ({mins/60:.1f} ч.)", C.GREEN)
    info(f"Финиш: {ended:%Y-%m-%d %H:%M:%S}")
    print()

if __name__ == "__main__":
    main()
