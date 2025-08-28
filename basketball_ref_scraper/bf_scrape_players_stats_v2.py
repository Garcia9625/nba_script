import os
import re
import csv
import time
import random
from typing import Optional, Dict, Any, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from lxml import html


# ── ScraperAPI options (hardcoded toggles) ────────────────────────────────────
USE_PREMIUM = False          # set True to use ScraperAPI premium pool
USE_RENDER  = False          # BBRef is static → keep False
COUNTRY_CODE = None          # e.g. "us", "ca", "gb"; or None
SCRAPERAPI_KEY_HARDCODED = "aaa492ea5514911b40ac2e7679e21da7"  # your key

# threading
MAX_WORKERS = 10              # adjust to your network/CPU
REQUEST_TIMEOUT = 45

# CSV output
CSV_PATH = "basketball_ref_scraper/output_data/all_nba_players_stats.csv"


# ──────────────────────────────────────────────────────────────────────────────
# Networking + DOM helpers
# ──────────────────────────────────────────────────────────────────────────────

def fetch_html_via_scraperapi(
    url: str,
    api_key: Optional[str] = None,
    retries: int = 5,
    timeout: int = REQUEST_TIMEOUT,
    jitter_range: Tuple[float, float] = (0.2, 0.7),
    base: str = "https://api.scraperapi.com",
    user_agent: str = "Mozilla/5.0 (compatible; BBRBot/1.0)"
) -> str:
    """
    Fetch a URL via ScraperAPI with optional premium, render, and geo routing.
    Retries with exponential backoff + jitter.
    ALWAYS prints when retrying or encountering errors.
    """
    key = SCRAPERAPI_KEY_HARDCODED or (api_key or "")
    if not key:
        raise RuntimeError("SCRAPERAPI_KEY not set. Put it in SCRAPERAPI_KEY_HARDCODED or pass api_key.")

    params = {"api_key": key, "url": url}
    if USE_PREMIUM:
        params["premium"] = "true"
    if USE_RENDER:
        params["render"] = "true"
    if COUNTRY_CODE:
        params["country_code"] = COUNTRY_CODE

    headers = {"User-Agent": user_agent}

    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(base, params=params, headers=headers, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504, 403):
                # transient or block → retry
                last_err = RuntimeError(f"HTTP {r.status_code}")
                print(f"   ↻ RETRY {attempt}/{retries} [{r.status_code}] for {url}")
                if attempt == retries:
                    break
            else:
                r.raise_for_status()
                if r.text:
                    return r.text
                last_err = RuntimeError("Empty response body")
                print(f"   ↻ RETRY {attempt}/{retries} [empty-body] for {url}")
                if attempt == retries:
                    break
        except requests.RequestException as e:
            last_err = e
            print(f"   ↻ RETRY {attempt}/{retries} [exception: {type(e).__name__}] for {url}: {e}")
            if attempt == retries:
                break

        # backoff + jitter
        time.sleep(min(1.5 ** attempt, 12) + random.uniform(*jitter_range))

    raise RuntimeError(f"Failed to fetch {url} after {retries} attempts. Last error: {last_err}")


def _build_dom(html_text: str) -> html.HtmlElement:
    """
    Build an lxml DOM and also append any tables that may be hidden inside HTML comments.
    Basketball-Reference sometimes wraps tables in comments (e.g., per_game, per_minute, per_poss, advanced, contracts_*).
    """
    TABLE_ID_SUBSTRINGS = (
        "per_game_stats",
        "per_minute_stats",
        "per_poss",
        "advanced",
        "all_salaries",
        "contracts_",
    )

    dom = html.fromstring(html_text)

    for c in dom.xpath('//comment()'):
        c_text = c.text or ""
        if "<table" not in c_text:
            continue
        if any(sub in c_text for sub in TABLE_ID_SUBSTRINGS):
            try:
                sub = html.fromstring(c_text)
                for t in sub.xpath(".//table"):
                    tid = t.get("id") or ""
                    if any(sub in tid for sub in TABLE_ID_SUBSTRINGS):
                        dom.append(t)
            except Exception:
                pass

    return dom


def _ensure_dir_for_file(path: str) -> None:
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)


# ──────────────────────────────────────────────────────────────────────────────
# Parsers
# ──────────────────────────────────────────────────────────────────────────────

def players_personal_info(
    url: str,
    api_key: Optional[str] = None,
    dom: Optional[html.HtmlElement] = None,
    html_text: Optional[str] = None
) -> Dict[str, Optional[str]]:
    m = re.search(r"/([^/]+)\.html?$", url)
    player_id = m.group(1).lower() if m else None

    if dom is None:
        if html_text is None:
            html_text = fetch_html_via_scraperapi(url, api_key=api_key)
        dom = _build_dom(html_text)

    player_name = ''.join(dom.xpath("//div[@id='meta']//h1//span/text()")).strip()
    if not player_name:
        og = dom.xpath("//meta[@property='og:title']/@content")
        if og:
            player_name = og[0].split(" Stats", 1)[0].strip()

    team = ''.join(dom.xpath("//div[@id='meta']//p[strong[normalize-space()='Team']]/a/text()")).strip()
    if not team:
        p_text = dom.xpath("normalize-space(//div[@id='meta']//p[strong[normalize-space()='Team']])")
        if p_text and ":" in p_text:
            team = p_text.split(":", 1)[1].strip()

    birth_day = ''.join(dom.xpath("//strong[normalize-space()='Born:']/following-sibling::span[@id='necro-birth']/@data-birth")).strip()
    if not birth_day:
        birth_day = ''.join(dom.xpath("//strong[contains(.,'Born')]/following-sibling::span[@id='necro-birth']/@data-birth")).strip()

    exp_line = dom.xpath("string(//div[@id='meta']//p[strong[normalize-space()='Experience:']])")
    years_experience = None
    if exp_line:
        m_exp = re.search(r"Experience:\s*(\d+)", exp_line, flags=re.IGNORECASE)
        if m_exp:
            try:
                years_experience = int(m_exp.group(1))
            except ValueError:
                years_experience = None

    return {
        "player_id": player_id,
        "player_name": player_name or None,
        "team": team or None,
        "birth_day": birth_day or None,
        "years_experience": years_experience,
        "url": url
    }


def per_game_stats(url: str, api_key: Optional[str] = None,
                   prefer_tot: bool = True,
                   dom: Optional[html.HtmlElement] = None,
                   html_text: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if dom is None:
        if html_text is None:
            html_text = fetch_html_via_scraperapi(url, api_key=api_key)
        dom = _build_dom(html_text)

    table_nodes = dom.xpath("//table[@id='per_game_stats']")
    if not table_nodes:
        return None
    table = table_nodes[0]

    header_stats: List[str] = []
    for th in table.xpath(".//thead//tr[1]/th"):
        ds = th.get("data-stat")
        if ds:
            header_stats.append(ds)

    rows = table.xpath(".//tbody/tr[th[@data-stat='year_id']]")
    if not rows:
        return None

    seasons: List[tuple[int, Any]] = []
    for tr in rows:
        th = tr.xpath("./th[@data-stat='year_id']")[0]
        csk = th.get("csk")
        try:
            csk_int = int(csk)
        except (TypeError, ValueError):
            txt = th.xpath("normalize-space(string(.))")
            m = re.search(r"(\d{4})(?:-\d{2})?$", txt or "")
            csk_int = int(m.group(1)) if m else 0
        seasons.append((csk_int, tr))

    max_csk = max(c for c, _ in seasons)
    latest_rows = [tr for (c, tr) in seasons if c == max_csk]

    teams_abbrs: List[str] = []
    seen_team = set()
    for tr in latest_rows:
        abbr = ''.join(tr.xpath("./td[@data-stat='team_name_abbr']//text()")).strip()
        if not abbr or abbr.upper() == "TOT":
            continue
        if abbr not in seen_team:
            seen_team.add(abbr)
            teams_abbrs.append(abbr)

    was_traded = "Y" if len(teams_abbrs) > 1 else "N"
    teams_count = 1 if len(teams_abbrs) == 1 else len(teams_abbrs) - 1
    teams_played = ",".join(teams_abbrs)

    if len(latest_rows) == 1 or not prefer_tot:
        chosen = latest_rows[0]
    else:
        chosen = None
        for tr in latest_rows:
            team_txt = ''.join(tr.xpath("./td[@data-stat='team_name_abbr']//text()")).strip()
            if team_txt.upper() == "TOT":
                chosen = tr
                break
        if chosen is None:
            chosen = latest_rows[0]

    out: Dict[str, Any] = {}
    th = chosen.xpath("./th[@data-stat='year_id']")[0]
    out["season"] = th.xpath("normalize-space(string(.))")

    td_by_stat = {td.get("data-stat"): td for td in chosen.xpath("./td[@data-stat]")}

    no_per_g_and_rename = {
        "team_name_abbr": "team_id",
        "comp_name_abbr": "lg_id",
        "pos": "pos",
        "games": "g",
        "games_started": "gs",
        "awards": "awards",
    }

    for stat in header_stats:
        if stat == "year_id":
            continue
        td = td_by_stat.get(stat)
        if td is None:
            continue
        val = td.xpath("normalize-space(string(.))")

        if stat == "age":
            out["age"] = val
            continue

        if stat in no_per_g_and_rename:
            out[no_per_g_and_rename[stat]] = val
        else:
            key = stat if stat.endswith("_per_g") else f"{stat}_per_g"
            out[key] = val

    out["was_traded"] = was_traded
    out["teams_count"] = teams_count
    out["teams_played"] = teams_played

    tfoot = table.xpath("./tfoot")
    if tfoot:
        tf = tfoot[0]
        candidates = tf.xpath(".//tr[th[@data-stat='year_id'] and .//td[@data-stat]]")
        career_tr = None
        if candidates:
            best = None
            best_score = -1
            for tr in candidates:
                th = tr.xpath("./th[@data-stat='year_id']")[0]
                txt = th.xpath("normalize-space(string(.))")
                m_yrs = re.search(r"(\d+)\s+Yrs", txt)
                yrs_val = int(m_yrs.group(1)) if m_yrs else 0
                colspan = th.get("colspan")
                try:
                    colspan_val = int(colspan)
                except (TypeError, ValueError):
                    colspan_val = 0
                score = max(yrs_val, colspan_val)
                if score > best_score:
                    best_score = score
                    best = tr
            career_tr = best

        if career_tr is not None:
            c_td_by_stat = {td.get("data-stat"): td for td in career_tr.xpath("./td[@data-stat]")}
            career_exclude = {"year_id", "team_name_abbr", "comp_name_abbr", "pos", "awards"}
            for stat in header_stats:
                if stat in career_exclude:
                    continue
                td = c_td_by_stat.get(stat)
                if td is None:
                    continue
                val = td.xpath("normalize-space(string(.))")
                out[f"career_{stat}"] = val

    return out


def per_36_min_stats(url: str, api_key: Optional[str] = None, prefer_tot: bool = True,
                     dom: Optional[html.HtmlElement] = None,
                     html_text: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if dom is None:
        if html_text is None:
            html_text = fetch_html_via_scraperapi(url, api_key=api_key)
        dom = _build_dom(html_text)
    tables = dom.xpath("//table[@id='per_minute_stats']")
    if not tables:
        return None
    table = tables[0]

    header_stats: List[str] = []
    for th in table.xpath(".//thead//tr[1]/th"):
        ds = th.get("data-stat")
        if ds:
            header_stats.append(ds)

    rows = table.xpath(".//tbody/tr[th[@data-stat='year_id']]")
    if not rows:
        return None

    seasons: List[tuple[int, Any]] = []
    for tr in rows:
        th = tr.xpath("./th[@data-stat='year_id']")
        if not th:
            continue
        csk = th[0].get("csk")
        csk_int = 0
        if csk is not None:
            try:
                csk_int = int(csk)
            except ValueError:
                pass
        if csk_int == 0:
            txt = th[0].xpath("normalize-space(string(.))")
            m = re.search(r"(\d{4})(?:-\d{2})?$", txt or "")
            if m:
                try:
                    csk_int = int(m.group(1))
                except ValueError:
                    csk_int = 0
        seasons.append((csk_int, tr))

    if not seasons:
        return None

    max_csk = max(c for c, _ in seasons)
    latest_rows = [tr for (c, tr) in seasons if c == max_csk]

    aggregate_markers = {"TOT", "2TM", "3TM", "4TM"}
    exclude_stats = {"year_id", "age", "team_name_abbr", "comp_name_abbr", "pos", "awards"}

    chosen = None
    if prefer_tot and len(latest_rows) > 1:
        for tr in latest_rows:
            tds = tr.xpath("./td[@data-stat='team_name_abbr']")
            team_txt = tds[0].xpath("normalize-space(string(.))").upper() if tds else ""
            if team_txt in aggregate_markers:
                chosen = tr
                break

    need_manual_aggregate = (chosen is None and len(latest_rows) > 1)

    out: Dict[str, Any] = {}

    if not need_manual_aggregate:
        if chosen is None:
            chosen = latest_rows[0]
        tds = chosen.xpath("./td[@data-stat]")
        td_by_stat = {td.get("data-stat"): td for td in tds}
        for stat in header_stats:
            if stat in exclude_stats:
                continue
            td = td_by_stat.get(stat)
            if td is None:
                continue
            val = td.xpath("normalize-space(string(.))")
            key = stat if "per_minute_36" in stat else f"{stat}_per_minute_36"
            out[key] = val
    else:
        def _to_float_inline(s: str) -> Optional[float]:
            s = (s or "").strip()
            if not s:
                return None
            if s.startswith(".") and s[1:].replace(".", "", 1).isdigit():
                s = "0" + s
            try:
                return float(s)
            except ValueError:
                return None

        team_rows = []
        for tr in latest_rows:
            tds = tr.xpath("./td[@data-stat='team_name_abbr']")
            team_txt = tds[0].xpath("normalize-space(string(.))").upper() if tds else ""
            if team_txt not in aggregate_markers:
                team_rows.append(tr)

        parsed_rows: List[Dict[str, Optional[float]]] = []
        for tr in team_rows:
            row_dict: Dict[str, Optional[float]] = {}
            mp_td = tr.xpath("./td[@data-stat='mp']")
            mp_val = mp_td[0].xpath("normalize-space(string(.))") if mp_td else ""
            row_dict["mp"] = _to_float_inline(mp_val) or 0.0
            for stat in header_stats:
                if stat in exclude_stats:
                    continue
                td = tr.xpath(f"./td[@data-stat='{stat}']")
                sval = td[0].xpath("normalize-space(string(.))") if td else ""
                row_dict[stat] = _to_float_inline(sval)
            parsed_rows.append(row_dict)

        total_mp = sum((r.get("mp", 0.0) or 0.0) for r in parsed_rows)
        if total_mp <= 0:
            chosen = team_rows[0]
            tds = chosen.xpath("./td[@data-stat]")
            td_by_stat = {td.get("data-stat"): td for td in tds}
            for stat in header_stats:
                if stat in exclude_stats:
                    continue
                td = td_by_stat.get(stat)
                if td is None:
                    continue
                val = td.xpath("normalize-space(string(.))")
                key = stat if "per_minute_36" in stat else f"{stat}_per_minute_36"
                out[key] = val
        else:
            for stat in header_stats:
                if stat in exclude_stats:
                    continue
                num = 0.0
                den = 0.0
                for r in parsed_rows:
                    v = r.get(stat)
                    w = r.get("mp", 0.0) or 0.0
                    if v is None:
                        continue
                    num += v * w
                    den += w
                sval = ""
                if den > 0:
                    agg = num / den
                    sval = f"{agg:.3f}".rstrip("0").rstrip(".")
                key = stat if "per_minute_36" in stat else f"{stat}_per_minute_36"
                out[key] = sval

    tf_rows = table.xpath(".//tfoot/tr")
    career_tr = None
    if tf_rows:
        for tr in tf_rows:
            th_text = tr.xpath("normalize-space(./th[@data-stat='year_id'])")
            if re.fullmatch(r"\d+\s+Yr(?:s)?", th_text or ""):
                career_tr = tr
                break
        if career_tr is None:
            best = None
            best_years = -1
            for tr in tf_rows:
                th_text = tr.xpath("normalize-space(./th[@data-stat='year_id'])")
                m = re.search(r"(\d+)\s+Yr(?:s)?", th_text or "")
                if m:
                    yrs = int(m.group(1))
                    if yrs > best_years:
                        best_years = yrs
                        best = tr
            career_tr = best

    if career_tr is not None:
        td_by_stat_c = {td.get("data-stat"): td for td in career_tr.xpath("./td[@data-stat]")}
        for stat in header_stats:
            if stat in {"year_id", "age", "team_name_abbr", "comp_name_abbr", "pos", "awards"}:
                continue
            td = td_by_stat_c.get(stat)
            if td is None:
                continue
            val = td.xpath("normalize-space(string(.))")
            key = stat if "per_minute_36" in stat else f"{stat}_per_minute_36"
            out[f"career_{key}"] = val

    return out


def per_100_poss_stats(url: str, api_key: Optional[str] = None, prefer_tot: bool = True,
                       dom: Optional[html.HtmlElement] = None,
                       html_text: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if dom is None:
        if html_text is None:
            html_text = fetch_html_via_scraperapi(url, api_key=api_key)
        dom = _build_dom(html_text)

    tables = dom.xpath("//table[@id='per_poss']") or dom.xpath("//table[@id='per_poss_stats']")
    if not tables:
        return None
    table = tables[0]

    header_stats: List[str] = []
    ths = table.xpath(".//thead//tr[1]/th")
    for th in ths:
        ds = th.get("data-stat")
        if ds:
            header_stats.append(ds)

    rows = table.xpath(".//tbody/tr[th[@data-stat='year_id']]")
    if not rows:
        return None

    seasons: List[tuple[int, Any]] = []
    for tr in rows:
        th = tr.xpath("./th[@data-stat='year_id']")
        if not th:
            continue
        csk = th[0].get("csk")
        csk_int = 0
        if csk is not None:
            try:
                csk_int = int(csk)
            except ValueError:
                pass
        if csk_int == 0:
            txt = th[0].xpath("normalize-space(string(.))")
            m = re.search(r"(\d{4})(?:-\d{2})?$", txt or "")
            if m:
                try:
                    csk_int = int(m.group(1))
                except ValueError:
                    csk_int = 0
        seasons.append((csk_int, tr))

    if not seasons:
        return None

    max_csk = max(c for c, _ in seasons)
    latest_rows = [tr for (c, tr) in seasons if c == max_csk]

    aggregate_markers = {"TOT", "2TM", "3TM", "4TM"}
    exclude_stats = {"year_id", "age", "team_name_abbr", "comp_name_abbr", "pos", "awards"}

    chosen = None
    if prefer_tot and len(latest_rows) > 1:
        for tr in latest_rows:
            tds = tr.xpath("./td[@data-stat='team_name_abbr']")
            team_txt = tds[0].xpath("normalize-space(string(.))").upper() if tds else ""
            if team_txt in aggregate_markers:
                chosen = tr
                break

    need_manual_aggregate = (chosen is None and len(latest_rows) > 1)
    out: Dict[str, Any] = {}

    if not need_manual_aggregate:
        if chosen is None:
            chosen = latest_rows[0]
        td_by_stat = {td.get("data-stat"): td for td in chosen.xpath("./td[@data-stat]")}
        for stat in header_stats:
            if stat in exclude_stats:
                continue
            td = td_by_stat.get(stat)
            if td is None:
                continue
            val = td.xpath("normalize-space(string(.))")
            key = stat if stat.endswith("_per_poss") else f"{stat}_per_poss"
            out[key] = val
    else:
        def _to_float_inline(s: str) -> Optional[float]:
            s = (s or "").strip()
            if not s:
                return None
            if s.startswith(".") and s[1:].replace(".", "", 1).isdigit():
                s = "0" + s
            try:
                return float(s)
            except ValueError:
                return None

        team_rows = []
        for tr in latest_rows:
            tds = tr.xpath("./td[@data-stat='team_name_abbr']")
            team_txt = tds[0].xpath("normalize-space(string(.))").upper() if tds else ""
            if team_txt not in aggregate_markers:
                team_rows.append(tr)

        parsed_rows: List[Dict[str, Optional[float]]] = []
        for tr in team_rows:
            row_dict: Dict[str, Optional[float]] = {}
            mp_td = tr.xpath("./td[@data-stat='mp']")
            mp_val = mp_td[0].xpath("normalize-space(string(.))") if mp_td else ""
            row_dict["mp"] = _to_float_inline(mp_val) or 0.0
            for stat in header_stats:
                if stat in exclude_stats:
                    continue
                td = tr.xpath(f"./td[@data-stat='{stat}']")
                sval = td[0].xpath("normalize-space(string(.))") if td else ""
                row_dict[stat] = _to_float_inline(sval)
            parsed_rows.append(row_dict)

        total_mp = sum((r.get("mp", 0.0) or 0.0) for r in parsed_rows)
        if total_mp <= 0:
            chosen = team_rows[0]
            td_by_stat = {td.get("data-stat"): td for td in chosen.xpath("./td[@data-stat]")}
            for stat in header_stats:
                if stat in exclude_stats:
                    continue
                td = td_by_stat.get(stat)
                if td is None:
                    continue
                val = td.xpath("normalize-space(string(.))")
                key = stat if stat.endswith("_per_poss") else f"{stat}_per_poss"
                out[key] = val
        else:
            for stat in header_stats:
                if stat in exclude_stats:
                    continue
                num = 0.0
                den = 0.0
                for r in parsed_rows:
                    v = r.get(stat)
                    w = r.get("mp", 0.0) or 0.0
                    if v is None:
                        continue
                    num += v * w
                    den += w
                sval = ""
                if den > 0:
                    agg = num / den
                    sval = f"{agg:.3f}".rstrip("0").rstrip(".")
                key = stat if stat.endswith("_per_poss") else f"{stat}_per_poss"
                out[key] = sval

    tf_rows = table.xpath(".//tfoot/tr")
    career_tr = None
    if tf_rows:
        for tr in tf_rows:
            th_text = tr.xpath("normalize-space(./th[@data-stat='year_id'])")
            if re.fullmatch(r"\d+\s+Yr(?:s)?", th_text or ""):
                career_tr = tr
                break
        if career_tr is None:
            best = None
            best_years = -1
            for tr in tf_rows:
                th_text = tr.xpath("normalize-space(./th[@data-stat='year_id'])")
                m = re.search(r"(\d+)\s+Yr(?:s)?", th_text or "")
                if m:
                    yrs = int(m.group(1))
                    if yrs > best_years:
                        best_years = yrs
                        best = tr
            career_tr = best

    if career_tr is not None:
        td_by_stat_c = {td.get("data-stat"): td for td in career_tr.xpath("./td[@data-stat]")}
        for stat in header_stats:
            if stat in {"year_id", "age", "team_name_abbr", "comp_name_abbr", "pos", "awards"}:
                continue
            td = td_by_stat_c.get(stat)
            if td is None:
                continue
            val = td.xpath("normalize-space(string(.))")
            key = stat if stat.endswith("_per_poss") else f"{stat}_per_poss"
            out[f"career_{key}"] = val

    return out


def advanced_stats(url: str, api_key: Optional[str] = None, prefer_tot: bool = True,
                   dom: Optional[html.HtmlElement] = None,
                   html_text: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if dom is None:
        if html_text is None:
            html_text = fetch_html_via_scraperapi(url, api_key=api_key)
        dom = _build_dom(html_text)

    tables = dom.xpath("//table[@id='advanced']")
    if not tables:
        return None
    table = tables[0]

    header_stats: List[str] = []
    for th in table.xpath(".//thead//tr[1]/th"):
        ds = th.get("data-stat")
        if ds:
            header_stats.append(ds)

    rows = table.xpath(".//tbody/tr[th[@data-stat='year_id']]")
    if not rows:
        return None

    seasons: List[tuple[int, Any]] = []
    for tr in rows:
        th = tr.xpath("./th[@data-stat='year_id']")[0]
        csk = th.get("csk")
        try:
            csk_int = int(csk)
        except (TypeError, ValueError):
            txt = th.xpath("normalize-space(string(.))")
            m = re.search(r"(\d{4})(?:-\d{2})?$", txt or "")
            csk_int = int(m.group(1)) if m else 0
        seasons.append((csk_int, tr))
    max_csk = max(c for c, _ in seasons)
    latest_rows = [tr for (c, tr) in seasons if c == max_csk]

    def _cell_text(tr, stat: str) -> str:
        if stat == "year_id":
            th = tr.xpath("./th[@data-stat='year_id']")
            return th[0].xpath("normalize-space(string(.))") if th else ""
        td = tr.xpath(f"./td[@data-stat='{stat}']")
        return td[0].xpath("normalize-space(string(.))") if td else ""

    def _to_float(s: str) -> Optional[float]:
        s = (s or "").strip()
        if not s:
            return None
        if s.startswith(".") and s[1:].replace(".", "", 1).isdigit():
            s = "0" + s
        try:
            return float(s)
        except ValueError:
            return None

    def _out_key(stat: str) -> str:
        return f"{stat}_adv"

    aggregate_markers = {"TOT", "2TM", "3TM", "4TM"}
    exclude_stats = {"year_id", "age", "team_name_abbr", "comp_name_abbr", "pos", "awards"}
    total_like = {"ows", "dws", "ws", "vorp"}

    chosen = None
    if prefer_tot and len(latest_rows) > 1:
        for tr in latest_rows:
            team_txt = (_cell_text(tr, "team_name_abbr") or "").upper()
            if team_txt in aggregate_markers:
                chosen = tr
                break

    need_manual_aggregate = chosen is None and len(latest_rows) > 1
    out: Dict[str, Any] = {}

    if not need_manual_aggregate:
        if chosen is None:
            chosen = latest_rows[0]
        td_by_stat = {td.get("data-stat"): td for td in chosen.xpath("./td[@data-stat]")}
        for stat in header_stats:
            if stat in exclude_stats:
                continue
            td = td_by_stat.get(stat)
            if td is None:
                continue
            out[_out_key(stat)] = td.xpath("normalize-space(string(.))")
    else:
        team_rows = []
        for tr in latest_rows:
            team_txt = (_cell_text(tr, "team_name_abbr") or "").upper()
            if team_txt not in aggregate_markers:
                team_rows.append(tr)
        if not team_rows:
            team_rows = [latest_rows[0]]

        parsed_rows: List[Dict[str, Optional[float]]] = []
        for tr in team_rows:
            row: Dict[str, Optional[float]] = {}
            row["mp"] = _to_float(_cell_text(tr, "mp")) or 0.0
            for stat in header_stats:
                if stat in exclude_stats:
                    continue
                row[stat] = _to_float(_cell_text(tr, stat))
            parsed_rows.append(row)

        total_mp = sum((r.get("mp") or 0.0) for r in parsed_rows)
        for stat in header_stats:
            if stat in exclude_stats:
                continue
            if stat in total_like:
                s = 0.0
                have_any = False
                for r in parsed_rows:
                    v = r.get(stat)
                    if v is not None:
                        s += v
                        have_any = True
                out[_out_key(stat)] = (f"{s:.3f}".rstrip("0").rstrip(".") if have_any else "")
            else:
                if total_mp <= 0:
                    out[_out_key(stat)] = _cell_text(team_rows[0], stat)
                else:
                    num = 0.0
                    for r in parsed_rows:
                        v = r.get(stat)
                        w = r.get("mp") or 0.0
                        if v is None:
                            continue
                        num += v * w
                    agg = num / total_mp
                    out[_out_key(stat)] = f"{agg:.3f}".rstrip("0").rstrip(".")

    career_row = None
    tf_rows = table.xpath(".//tfoot/tr")
    if tf_rows:
        for tr in tf_rows:
            label = tr.xpath("normalize-space(./th[@data-stat='year_id'])")
            if re.fullmatch(r"\d+\s+Yr(?:s)?", label or ""):
                career_row = tr
                break
        if career_row is None:
            best = None
            best_years = -1
            for tr in tf_rows:
                label = tr.xpath("normalize-space(./th[@data-stat='year_id'])")
                m = re.search(r"(\d+)\s+Yr(?:s)?", label or "")
                if m:
                    yrs = int(m.group(1))
                    if yrs > best_years:
                        best_years = yrs
                        best = tr
            career_row = best

    if career_row is not None:
        for stat in header_stats:
            if stat in exclude_stats:
                continue
            td = career_row.xpath(f"./td[@data-stat='{stat}']")
            if not td:
                continue
            val = td[0].xpath("normalize-space(string(.))")
            out[f"career_{stat}_adv"] = val
    else:
        for stat in header_stats:
            if stat in exclude_stats:
                continue
            key = _out_key(stat)
            if key in out:
                out[f"career_{stat}_adv"] = out[key]

    return out


def player_salary(url: str, api_key: Optional[str] = None,
                  dom: Optional[html.HtmlElement] = None,
                  html_text: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if dom is None:
        if html_text is None:
            html_text = fetch_html_via_scraperapi(url, api_key=api_key)
        dom = _build_dom(html_text)

    table_nodes = dom.xpath("//table[@id='all_salaries']")
    if not table_nodes:
        return None
    table = table_nodes[0]

    def _season_end_year(season_text: str) -> int:
        s = (season_text or "").strip()
        m = re.match(r"^(\d{4})(?:-(\d{2}))?$", s)
        if not m:
            return 0
        start_year = int(m.group(1))
        return start_year + 1 if m.group(2) else start_year

    rows = table.xpath(".//tbody/tr[th[@data-stat='season']]")
    if not rows:
        return None

    best = None
    best_end_year = -1
    for tr in rows:
        th = tr.xpath("./th[@data-stat='season']")
        season_text = th[0].xpath("normalize-space(string(.))") if th else ""
        end_year = _season_end_year(season_text)
        if end_year > best_end_year:
            best_end_year = end_year
            best = tr

    if best is None:
        return None

    sal_td = best.xpath("./td[@data-stat='salary']")
    salary_text = sal_td[0].xpath("normalize-space(string(.))") if sal_td else ""
    return {"salary": salary_text}


def player_current_contract(url: str, api_key: Optional[str] = None,
                            dom: Optional[html.HtmlElement] = None,
                            html_text: Optional[str] = None) -> Dict[str, str]:
    if dom is None:
        if html_text is None:
            html_text = fetch_html_via_scraperapi(url, api_key=api_key)
        dom = _build_dom(html_text)

    tables = dom.xpath("//table[starts-with(@id,'contracts_')]")
    table = tables[0] if tables else None

    if table is None and html_text:
        m = re.search(r"(<table[^>]+id=[\"']?contracts_[^>]*>.*?</table>)",
                      html_text, re.DOTALL | re.IGNORECASE)
        if m:
            try:
                table = html.fromstring(m.group(1))
            except Exception:
                table = None

    if table is None:
        return {"current_contract": ""}

    rows = table.xpath(".//tbody/tr") or table.xpath(".//tr[td]")
    if not rows:
        return {"current_contract": ""}

    first_row = rows[0]
    tds = first_row.xpath("./td")
    if len(tds) < 2:
        return {"current_contract": ""}

    salary_text = tds[1].xpath("normalize-space(string(.))") or ""
    return {"current_contract": salary_text}


# ──────────────────────────────────────────────────────────────────────────────
# URL reader
# ──────────────────────────────────────────────────────────────────────────────

def _read_urls_from_file(txt_path: str = "data/bf_players.txt") -> List[str]:
    try:
        with open(txt_path, "r", encoding="utf-8") as f:
            raw = f.read()
    except FileNotFoundError:
        print(f"⚠️ players file not found: {txt_path}")
        return []

    def _normalize(token: str) -> Optional[str]:
        token = token.strip()
        if not token:
            return None
        token = token.split("#", 1)[0].strip()
        if not token:
            return None

        if re.fullmatch(r"[A-Za-z][A-Za-z0-9]{8,}", token):
            pid = token.lower()
            return f"https://www.basketball-reference.com/players/{pid[0]}/{pid}.html"

        if re.fullmatch(r"/?players/[A-Za-z]/[A-Za-z0-9]+(?:\.html)?/?", token):
            path = token if token.startswith("/") else "/" + token
            if not path.endswith(".html"):
                path = path.rstrip("/") + ".html"
            parts = path.split("/")
            parts[-1] = parts[-1].lower()
            parts[-2] = parts[-2].lower()
            return "https://www.basketball-reference.com" + "/".join(parts)

        if token.startswith("www.basketball-reference.com/"):
            token = "https://" + token

        if token.startswith(("http://", "https://")):
            m = re.match(r"^(https?://)(?:www\.)?basketball-reference\.com([^?#]*)", token, re.I)
            if not m:
                return None
            path = m.group(2)
            if not path.startswith("/"):
                path = "/" + path
            m2 = re.match(r"^/players/([A-Za-z])/([A-Za-z0-9]+)(?:\.html)?/?$", path)
            if not m2:
                return None
            return f"https://www.basketball-reference.com/players/{m2.group(1).lower()}/{m2.group(2).lower()}.html"

        return None

    tokens = re.split(r"[\s,]+", raw.strip())
    normalized, invalid_samples = [], []
    total_tokens = 0
    for t in tokens:
        if not t:
            continue
        total_tokens += 1
        u = _normalize(t)
        if u:
            normalized.append(u)
        else:
            if len(invalid_samples) < 10:
                invalid_samples.append(t)

    seen, unique_urls, duplicates = set(), [], []
    for u in normalized:
        if u in seen:
            duplicates.append(u)
        else:
            seen.add(u)
            unique_urls.append(u)

    num_valid = len(normalized)
    num_unique = len(unique_urls)
    num_dupes = len(duplicates)
    num_invalid = total_tokens - num_valid

    print(
        f"🔎 Parsed {total_tokens} entries → valid {num_valid}, "
        f"duplicates {num_dupes}, invalid {num_invalid}."
    )
    print(f"📄 Using {num_unique} unique URL(s) from {txt_path}")

    if duplicates:
        print("⚠️ Duplicate entries (player appeared multiple times):")
        for d in duplicates[:20]:
            print(f"   - {d}")
        if len(duplicates) > 20:
            print(f"   ... and {len(duplicates)-20} more")

    if invalid_samples:
        print("⚠️ Sample invalid entries (first 10):")
        for s in invalid_samples:
            print(f"   - {s}")

    return unique_urls


# ──────────────────────────────────────────────────────────────────────────────
# Threaded runner
# ──────────────────────────────────────────────────────────────────────────────

def _process_one(url: str, prefer_tot: bool = True) -> Tuple[str, Optional[Dict[str, Any]], Optional[str]]:
    """
    Returns (url, row_dict_or_None, error_message_or_None).
    """
    try:
        html_text = fetch_html_via_scraperapi(url)
        dom = _build_dom(html_text)

        personal = players_personal_info(url, dom=dom, html_text=html_text)
        salary   = player_salary(url, dom=dom, html_text=html_text)
        contract = player_current_contract(url, dom=dom, html_text=html_text)
        pergame  = per_game_stats(url, dom=dom, html_text=html_text)
        per36    = per_36_min_stats(url, dom=dom, html_text=html_text, prefer_tot=prefer_tot)
        per100   = per_100_poss_stats(url, dom=dom, html_text=html_text, prefer_tot=prefer_tot)
        adv      = advanced_stats(url, dom=dom, html_text=html_text, prefer_tot=prefer_tot)

        combined = dict(personal)
        if salary:   combined.update(salary)
        if contract: combined.update(contract)
        if pergame:  combined.update(pergame)
        if per36:    combined.update(per36)
        if per100:   combined.update(per100)
        if adv:      combined.update(adv)
        combined.setdefault("url", url)
        return url, combined, None
    except Exception as e:
        return url, None, f"{type(e).__name__}: {e}"


def basketball_ref_all_players_stats(
    urls: Optional[List[str]] = None,
    csv_path: str = CSV_PATH,
    prefer_tot: bool = True,
) -> None:
    """
    Threaded scraper:
      - Fetch/parse in parallel
      - Write CSV rows as futures complete (main thread)
      - Prints retry logs from fetcher and per-URL errors
    """
    if urls is None:
        urls = _read_urls_from_file()

    if not urls:
        print("🚫 No player URLs to process. Provide `urls=[...]` or put them in data/bf_players.txt")
        return

    total = len(urls)
    _ensure_dir_for_file(csv_path)

    fieldnames: Optional[List[str]] = None
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = None

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(_process_one, url, prefer_tot): url for url in urls}
            done_count = 0

            for fut in as_completed(futures):
                url = futures[fut]
                done_count += 1
                try:
                    _, row, err = fut.result()
                    if err:
                        print(f"[{done_count}/{total}] ❌ {url} — {err}")
                        continue

                    # initialize header on first valid row
                    if fieldnames is None:
                        personal_first = [
                            "player_id","player_name","team","birth_day","years_experience",
                            "salary","current_contract",
                            "season","age","team_id","lg_id","pos","g","gs",
                            "was_traded","teams_count","teams_played","url"
                        ]
                        dynamic_cols = [k for k in row.keys() if k not in personal_first]
                        fieldnames = personal_first + dynamic_cols
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()

                    writer.writerow({k: row.get(k, "") for k in fieldnames})
                    f.flush()
                    pid = row.get("player_id") or "unknown"
                    print(f"[{done_count}/{total}] ✅ {pid} — written")

                except Exception as e:
                    print(f"[{done_count}/{total}] 💥 Future error for {url}: {e}")

    print(f"📝 Finished. CSV written to {csv_path}")


if __name__ == "__main__":
    basketball_ref_all_players_stats()
