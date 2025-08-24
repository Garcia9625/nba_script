import os, re, time, random, requests
from bs4 import BeautifulSoup
from urllib.parse import urlencode, urljoin

SCRAPERAPI_KEY = "b75bd47ce065ec63f921e2902a8602d2"
BASE = "https://www.basketball-reference.com"
YEAR = 2025  # 👈 change the year here

# Track requests
REQUEST_COUNT = 0

# Retry/backoff tunables
MAX_ATTEMPTS = 5           # total tries per URL
BASE_SLEEP = 0.8           # base backoff seconds
JITTER = 0.8               # random extra backoff
CONNECT_TIMEOUT = 8
READ_TIMEOUT = 25          # a bit lower than 45 so retry sooner on slow hops

# Optional ScraperAPI tuning (safe defaults)
USE_PREMIUM = False        # set True if your plan supports premium proxies
COUNTRY_CODE = "us"        # or None
KEEP_HEADERS = True        # forward headers (harmless here)

# Fix BBR quirks (historical codes vs NBA codes)
CODE_FIX = {
    "CHA": "CHO",  # Charlotte Hornets
    "NJN": "BRK",  # Brooklyn Nets
    "NOH": "NOP",  # Old Pelicans code
    # "PHX": "PHO",  # Phoenix Suns (uncomment if needed)
}

session = requests.Session()  # reuse TCP connections

def get_html(url: str) -> str:
    """GET via ScraperAPI with retries/backoff; returns text or '' on failure."""
    global REQUEST_COUNT

    params = {"api_key": SCRAPERAPI_KEY, "url": url}
    if KEEP_HEADERS:
        params["keep_headers"] = "true"
    if COUNTRY_CODE:
        params["country_code"] = COUNTRY_CODE
    if USE_PREMIUM:
        params["premium"] = "false"
    # NOTE: Basketball-Reference is static, so we don’t set render=true

    proxy_url = f"https://api.scraperapi.com/?{urlencode(params)}"

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            r = session.get(proxy_url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            REQUEST_COUNT += 1
            # Handle throttling / server hiccups
            if r.status_code == 429 or r.status_code >= 500:
                ra = r.headers.get("Retry-After")
                wait = int(ra) if ra and ra.isdigit() else BASE_SLEEP * (2 ** (attempt - 1)) + random.uniform(0, JITTER)
                print(f"⏳ Proxy/status {r.status_code} on {url} — retry {attempt}/{MAX_ATTEMPTS} in {wait:.1f}s")
                time.sleep(wait)
                continue

            r.raise_for_status()
            return r.text

        except (requests.ReadTimeout, requests.ConnectTimeout) as e:
            wait = BASE_SLEEP * (2 ** (attempt - 1)) + random.uniform(0, JITTER)
            print(f"⏱️ Timeout on {url} — retry {attempt}/{MAX_ATTEMPTS} in {wait:.1f}s ({e})")
            time.sleep(wait)

        except requests.RequestException as e:
            # Network oddities; try again with backoff
            wait = BASE_SLEEP * (2 ** (attempt - 1)) + random.uniform(0, JITTER)
            print(f"⚠️ Network error on {url} — retry {attempt}/{MAX_ATTEMPTS} in {wait:.1f}s ({e})")
            time.sleep(wait)

    print(f"❌ Request failed for {url}: exhausted {MAX_ATTEMPTS} attempts")
    return ""

def get_team_codes() -> list[str]:
    """Scrape team codes from /teams/ page (active franchises)."""
    html = get_html(f"{BASE}/teams/")
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", id="teams_active")
    if not table:
        print("❌ Could not find Active Franchises table on /teams/")
        return []
    codes = []
    for a in table.select("tbody tr th[data-stat='franch_name'] a[href^='/teams/']"):
        m = re.match(r"^/teams/([A-Z]{3})/$", a.get("href", ""))
        if m:
            code = CODE_FIX.get(m.group(1), m.group(1))
            codes.append(code)
    return sorted(set(codes))

def get_roster_player_hrefs(team_code: str, year: int) -> list[str]:
    """Return player profile links from roster table."""
    url = f"{BASE}/teams/{team_code}/{year}.html"
    print(f"➡️ Processing {team_code} {year}...")
    html = get_html(url)
    if not html:
        print(f"⚠️ Skipping {team_code} — no HTML fetched after retries")
        return []
    soup = BeautifulSoup(html, "lxml")
    roster = soup.find("table", id="roster")
    if not roster:
        print(f"⚠️ No roster table found for {team_code} {year}")
        return []
    hrefs = [urljoin(BASE, a["href"]) for a in roster.select('tbody td[data-stat="player"] a[href^="/players/"]')]
    return list(dict.fromkeys(hrefs))  # unique, preserve order

def basketball_ref_players_urls(
    year: int = YEAR,
    teams_path: str = "data/bf_teams.txt",
    players_path: str = "data/bf_players.txt",
):
    """Main orchestration function for scraping and saving results (sequential)."""
    # Ensure output folder exists
    os.makedirs(os.path.dirname(teams_path), exist_ok=True)
    if os.path.dirname(players_path):
        os.makedirs(os.path.dirname(players_path), exist_ok=True)

    # Gather all team URLs
    team_codes = get_team_codes()
    team_urls = [f"{BASE}/teams/{code}/{year}.html" for code in team_codes]

    # Save teams.txt
    with open(teams_path, "w", encoding="utf-8") as f:
        for url in team_urls:
            f.write(url + "\n")

    # Visit each team and print player URLs
    all_players = []
    for code in team_codes:
        players = get_roster_player_hrefs(code, year)
        all_players.extend(players)
        for link in players:
            print(link)

    # Save players.txt
    with open(players_path, "w", encoding="utf-8") as f:
        for url in all_players:
            f.write(url + "\n")

    print(f"\n✅ Done: {len(team_urls)} teams saved to {teams_path}, {len(all_players)} player URLs saved to {players_path}")
    print(f"📊 Total requests made: {REQUEST_COUNT}")

# Run directly
if __name__ == "__main__":
    basketball_ref_players_urls()
