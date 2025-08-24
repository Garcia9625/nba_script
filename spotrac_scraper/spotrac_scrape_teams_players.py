import os
import time
import random
import sys
from typing import List, Set, Tuple
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY") or "b75bd47ce065ec63f921e2902a8602d2"
TARGET_URL = "https://www.spotrac.com/nba/teams"

# ---- Config ----
MAX_RETRIES = 5
BACKOFF_BASE = 1.0  # seconds
REQUEST_TIMEOUT = 45
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0"

TEAMS_OUT_FILE = "data/spotrac_teams.txt"
PLAYERS_OUT_FILE = "data/spotrac_players.txt"

def ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def scraperapi_get(url: str, **params) -> requests.Response:
    endpoint = "https://api.scraperapi.com/"
    q = {"api_key": SCRAPERAPI_KEY, "url": url, "keep_headers": "true"}
    q.update(params)
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.8"
    }
    return requests.get(endpoint, params=q, headers=headers, timeout=REQUEST_TIMEOUT)

def fetch_html(url: str) -> str:
    backoff = BACKOFF_BASE
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = scraperapi_get(url)
            if r.status_code in (429, 503):
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

def extract_team_urls(html: str, year: int) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    anchors = soup.select("a[href*='/nba/'][href$='/overview']")
    urls: Set[str] = set()
    for a in anchors:
        href = a.get("href", "").strip()
        if not href:
            continue
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            href = "https://www.spotrac.com" + href
        if href.endswith("/overview"):
            href = href.rstrip("/") + f"/_/year/{year}"
            urls.add(href)
    return sorted(urls)

def slug_to_team_name(team_url: str) -> str:
    try:
        parts = urlparse(team_url).path.strip("/").split("/")
        if len(parts) >= 2:
            slug = parts[1]
            return " ".join(w.capitalize() for w in slug.replace("-", " ").split())
    except Exception:
        pass
    return team_url

def extract_player_links_from_team(html: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table#table")
    if not table:
        return []
    anchors = table.select("a[href]")
    urls: Set[str] = set()
    for a in anchors:
        href = a.get("href", "").strip()
        if not href:
            continue
        href = urljoin(base_url, href)
        if not href.startswith("https://www.spotrac.com/nba/"):
            continue
        if "/player/" not in urlparse(href).path:
            continue
        urls.add(href)
    return sorted(urls)

def write_players_snapshot(path: str, urls: Set[str]) -> None:
    """Overwrite the file with the current snapshot (sorted, unique)."""
    with open(path, "w") as f:
        for u in sorted(urls):
            f.write(u + "\n")
        f.flush()

def sportrac_players_urls(year: int):
    ensure_dir(TEAMS_OUT_FILE)
    ensure_dir(PLAYERS_OUT_FILE)

    # Start fresh: empty players file at run start
    open(PLAYERS_OUT_FILE, "w").close()

    # 1) Team URLs (with year) -> file
    html = fetch_html(TARGET_URL)
    team_urls = extract_team_urls(html, year)
    with open(TEAMS_OUT_FILE, "w") as f:
        for url in team_urls:
            f.write(url + "\n")
    print(f"✅ Found {len(team_urls)} team URLs for year {year}. Saved to {TEAMS_OUT_FILE}")

    # 2) Process each team, overwrite snapshot after each team
    seen_players: Set[str] = set()
    per_team_counts: List[Tuple[str, int, int]] = []  # (team, found, total_so_far)

    for team_url in team_urls:
        team_name = slug_to_team_name(team_url)
        print(f"🔎 Processing team: {team_name} ({year})...")
        try:
            team_html = fetch_html(team_url)
        except Exception as e:
            print(f"  ⚠️  Failed to fetch {team_name}: {e}")
            continue

        player_links = extract_player_links_from_team(team_html, base_url=team_url)
        found = len(player_links)

        # merge into global set
        seen_players.update(player_links)

        # overwrite snapshot on disk after each team
        write_players_snapshot(PLAYERS_OUT_FILE, seen_players)

        per_team_counts.append((team_name, found, len(seen_players)))
        print(f"  ✅ Found {found} players | 🗂️ Total so far (unique): {len(seen_players)}")

        time.sleep(0.5 + random.uniform(0, 0.3))

    # 3) Summary
    print("\n———— Summary ————")
    for team_name, found, total_so_far in per_team_counts:
        print(f"{team_name}: found={found}, total_after_team={total_so_far}")
    print(f"\n🧮 Final total unique player URLs: {len(seen_players)}")
    print(f"💾 Final snapshot saved to {PLAYERS_OUT_FILE}")

if __name__ == "__main__":
    sportrac_players_urls(2024)
