# bbref_conference_standings_csv.py
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup, Comment
import os

BR_BASE = "https://www.basketball-reference.com"
EAST_TABLE_ID = "confs_standings_E"
WEST_TABLE_ID = "confs_standings_W"
HEADERS = {"User-Agent": "Mozilla/5.0 (standings-scraper)"}


# get absolute path of the script directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_CSV = os.path.join(BASE_DIR, "output_data", "all_nba_teams_standings.csv")



def fetch_html(url: str, timeout: int = 30, retries: int = 3, backoff: float = 2.0) -> str:
    last = None
    for i in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            if r.status_code == 200 and r.text:
                return r.text
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r.text
        except Exception as e:
            last = e
            time.sleep(min(backoff ** i, 15))
    raise RuntimeError(f"Failed GET {url}: {last}")


def uncomment_html(html_text: str) -> str:
    soup = BeautifulSoup(html_text, "lxml")
    for c in soup.find_all(string=lambda t: isinstance(t, Comment)):
        if "<table" in c or "<thead" in c or "<tbody" in c:
            c.replace_with(BeautifulSoup(c, "lxml"))
    return str(soup)


def extract_table(table):
    header_ths = table.find("thead").find_all("th")
    columns = [th.get("data-stat") for th in header_ths if th.get("data-stat")]
    rows = []
    for tr in table.find("tbody").find_all("tr"):
        if "class" in tr.attrs and "thead" in tr["class"]:
            continue
        row = {}
        for cell in tr.find_all(["th", "td"]):
            key = cell.get("data-stat")
            if not key:
                continue
            txt = cell.get_text(strip=True)
            row[key] = txt
            if key == "team_name":
                a = cell.find("a")
                href = a["href"] if a and a.get("href") else None
                row["team_url"] = BR_BASE + href if href else None
                if href:
                    m = re.search(r"/teams/([A-Z]{2,3})/", href)
                    row["team_abbr"] = m.group(1) if m else None
                row["clinched_flag"] = "*" in cell.get_text()
        if row:
            rows.append(row)
    return columns, rows


def coerce_numeric(df: pd.DataFrame) -> pd.DataFrame:
    def parse_gb(x: str):
        if not x or x in {"—", "-"}:   # em dash or lone dash means leader/NA
            return 0.0
        try:
            return float(x)
        except Exception:
            return pd.NA

    for col in df.columns:
        if col in {"team_name", "team_url", "team_abbr", "conference", "season"}:
            continue
        if col == "gb":
            df[col] = df[col].map(parse_gb)
        else:
            df[col] = (
                df[col].astype(str)
                .str.replace(",", "", regex=False)    # remove thousands separator
                .str.replace("—", "", regex=False)    # remove em-dash (NA)
                # ⚠️ do NOT strip "-" here → keep negative numbers intact
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def basketball_ref_teams_stats(year: int):
    # ensure parent folder exists
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    url = f"{BR_BASE}/leagues/NBA_{year}_standings.html"
    html = uncomment_html(fetch_html(url))
    soup = BeautifulSoup(html, "lxml")

    tables = {
        "East": soup.find("table", id=EAST_TABLE_ID),
        "West": soup.find("table", id=WEST_TABLE_ID),
    }

    frames = []
    for conf, tb in tables.items():
        if tb is None:
            raise ValueError(f"Could not find {conf} table.")
        cols, rows = extract_table(tb)
        df = pd.DataFrame(rows)
        df.insert(0, "conference", conf)
        df.insert(0, "season", year)
        df = coerce_numeric(df)
        frames.append(df)

    out = pd.concat(frames, ignore_index=True)
    out.to_csv(OUTPUT_CSV, index=False)
    print(f"✅ Saved standings for {year} to {OUTPUT_CSV}")


if __name__ == "__main__":
    basketball_ref_teams_stats(2025)
