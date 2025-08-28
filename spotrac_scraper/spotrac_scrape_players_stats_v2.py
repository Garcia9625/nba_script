import os
import re
import csv
import time
import random
from datetime import datetime
from typing import Optional, Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from lxml import html as lxml_html

# ---------- Config ----------
SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY") or "aaa492ea5514911b40ac2e7679e21da7"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0"
REQUEST_TIMEOUT = 45
MAX_RETRIES = 5          # single retry system
BACKOFF_BASE = 1.0

USE_PREMIUM  = False
USE_RENDER   = False
COUNTRY_CODE = None

MAX_WORKERS = 10         # number of threads

PLAYERS_IN_FILE = "data/spotrac_players.txt"
OUT_CSV_FILE = "spotrac_scraper/output_data/spotrac_player_details.csv"

CAP_HIT_SEASON = "2025-26"


# ---------- Helpers ----------
def ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def scraperapi_get(url: str, **params) -> requests.Response:
    endpoint = "https://api.scraperapi.com/"
    q = {"api_key": SCRAPERAPI_KEY, "url": url, "keep_headers": "true"}
    if USE_PREMIUM:
        q["premium"] = "true"
    if USE_RENDER:
        q["render"] = "true"
    if COUNTRY_CODE:
        q["country_code"] = COUNTRY_CODE
    q.update(params)

    headers = {"User-Agent": USER_AGENT}
    return requests.get(endpoint, params=q, headers=headers, timeout=REQUEST_TIMEOUT)

def fetch_html(url: str) -> str:
    backoff = BACKOFF_BASE
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = scraperapi_get(url)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff + random.uniform(0, 0.5))
                backoff = min(backoff * 2, 60)
                continue
            r.raise_for_status()
            return r.text
        except requests.RequestException:
            if attempt == MAX_RETRIES:
                raise
            time.sleep(backoff + random.uniform(0, 0.5))
            backoff = min(backoff * 2, 60)
    raise RuntimeError("Unreachable")

def parse_player_id(url: str) -> Optional[str]:
    m = re.search(r"/id/(\d+)/", url)
    return m.group(1) if m else None

def clean_text(s: str) -> str:
    return " ".join(s.split()).strip()

def to_mm_dd_yyyy(date_str: str) -> Optional[str]:
    try:
        dt = datetime.strptime(date_str.strip(), "%b %d, %Y")
        return dt.strftime("%m-%d-%Y")
    except Exception:
        return None

def extract_fields(html_text: str, url: str) -> Dict[str, str]:
    doc = lxml_html.fromstring(html_text)
    name_candidates = doc.xpath("//h1[@id='team-name-logo']//div[contains(@class,'text-white')]//text()")
    player_name = clean_text(" ".join([t for t in name_candidates if clean_text(t)]))
    age_texts = doc.xpath("//div[strong[contains(normalize-space(.),'Age:')]]/span[contains(@class,'text-yellow')]/text()")
    birthday_mmddyyyy = ""
    if age_texts:
        m = re.search(r"\(([^)]+)\)", age_texts[0])
        if m:
            birthday_mmddyyyy = to_mm_dd_yyyy(m.group(1)) or ""
    team_texts = doc.xpath("//div[contains(@class,'text-yellow') and contains(@class,'fw-bold')]//a/text()")
    team_name = clean_text(team_texts[0]) if team_texts else ""
    contract_years = ""
    contract_type = ""
    years_nodes = doc.xpath("//h2//span[contains(@class,'years')]/text()")
    if years_nodes:
        contract_years = clean_text(years_nodes[0])
        type_nodes = doc.xpath("//h2[.//span[contains(@class,'years')]]//small[contains(@class,'ms-2')]/text()")
        if type_nodes:
            raw = clean_text(type_nodes[0])
            contract_type = clean_text(re.sub(r"\(CURRENT\)\s*$", "", raw, flags=re.IGNORECASE))
    cap_hit_val = ""
    cap_hit_nodes = doc.xpath(f"//h5[contains(normalize-space(.), '{CAP_HIT_SEASON} Cap Hit')]/following-sibling::p[1]/text()")
    if cap_hit_nodes:
        cap_hit_val = clean_text(cap_hit_nodes[0])
    return {
        "player_id": parse_player_id(url) or "",
        "player_name": player_name,
        "birthday": birthday_mmddyyyy,
        "team": team_name,
        "current_contract_years": contract_years,
        "current_contract_type": contract_type,
        "cap_hit_" + CAP_HIT_SEASON.replace("-", "_"): cap_hit_val,
        "source_url": url,
    }

def load_player_urls(path: str) -> List[str]:
    with open(path, "r") as f:
        urls = [line.strip() for line in f if line.strip()]
    return [u for u in urls if "spotrac.com/nba/" in u]

def write_csv_header(path: str, fieldnames: List[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

def append_csv_row(path: str, fieldnames: List[str], row: Dict[str, str]) -> None:
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writerow(row)


# ---------- Worker ----------
def _process_one(url: str, index: int, total: int) -> Tuple[str, Optional[Dict[str, str]], Optional[str], int]:
    try:
        html_text = fetch_html(url)
        data = extract_fields(html_text, url)
        return url, data, None, index
    except Exception as e:
        return url, None, str(e), index


# ---------- Orchestrator ----------
def spotrac_all_players_stats() -> Tuple[int, int, float]:
    start_ts = time.time()
    ensure_dir(OUT_CSV_FILE)
    player_urls = load_player_urls(PLAYERS_IN_FILE)

    fields = [
        "player_id","player_name","birthday","team",
        "current_contract_years","current_contract_type",
        "cap_hit_" + CAP_HIT_SEASON.replace("-", "_"),
        "source_url",
    ]
    write_csv_header(OUT_CSV_FILE, fields)

    successes = 0
    failures = 0
    total = len(player_urls)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        future_map = {ex.submit(_process_one, url, i, total): url for i, url in enumerate(player_urls, 1)}
        for fut in as_completed(future_map):
            url = future_map[fut]
            try:
                _, row, err, index = fut.result()
                if err:
                    failures += 1
                    print(f"[{index}/{total}] ⚠️ Error for {url}: {err}")
                else:
                    append_csv_row(OUT_CSV_FILE, fields, row)
                    successes += 1
                    print(f"[{index}/{total}] ✅ {row['player_name'] or row['player_id']} -> saved")
            except Exception as e:
                failures += 1
                print(f"💥 Unexpected error for {url}: {e}")

    elapsed_total = time.time() - start_ts
    return successes, failures, elapsed_total


if __name__ == "__main__":
    succ, fail, secs = spotrac_all_players_stats()
    #mm, ss = divmod(int(secs), 60)
    #hh, mm = divmod(mm, 60)
    #print(f"\n✅ Successes: {succ}")
    print(f"💥 Failures : {fail}")
    #print(f"⏱  Total    : {hh:02d}:{mm:02d}:{ss:02d}")
