# main_run.py
import time
from datetime import timedelta

from basketball_ref_scraper.bf_scrape_teams_players import basketball_ref_players_urls
from basketball_ref_scraper.bf_scrape_players_stats import basketball_ref_all_players_stats
from basketball_ref_scraper.bf_scrape_teams_standings import basketball_ref_teams_stats
from spotrac_scraper.spotrac_scrape_teams_players import spotrac_players_urls
from spotrac_scraper.spotrac_scrape_players_stats import spotrac_all_players_stats


def _fmt(secs: float) -> str:
    return str(timedelta(seconds=round(secs)))


def _time_call(name: str, fn, *args, **kwargs) -> tuple[str, float, bool, str]:
    """
    Runs a function, measures elapsed seconds, and returns:
      (step_name, elapsed_seconds, ok, error_message_if_any)
    """
    t0 = time.perf_counter()
    try:
        fn(*args, **kwargs)
        ok = True
        err = ""
    except Exception as e:
        ok = False
        err = f"{type(e).__name__}: {e}"
    elapsed = time.perf_counter() - t0
    print(f"▶️  {name} ... {_fmt(elapsed)} {'✅' if ok else '❌'}")
    if not ok:
        print(f"    ↳ error: {err}")
    return name, elapsed, ok, err


def run_full_pipeline(
    br_year_players_urls: int = 2025,   # 2025 => 2024-25 season on BBRef
    br_year_team_standings: int = 2025, # 2025 => 2024-25 season on BBRef
    spotrac_year_players_urls: int = 2024  # 2024 => 2024-25 season on Spotrac
) -> dict:
    """
    Runs all scraping steps with timing.
    Returns a dict summary with per-step timings and total.
    """
    print("\n====== NBA Scrape: Full Run ======\n")

    timings = []

    # Basketball-Reference
    timings.append(_time_call(
        f"BBRef: collect team & player URLs (year={br_year_players_urls})",
        basketball_ref_players_urls,
        br_year_players_urls
    ))
    timings.append(_time_call(
        "BBRef: scrape all player stats",
        basketball_ref_all_players_stats
    ))
    timings.append(_time_call(
        f"BBRef: scrape team standings (year={br_year_team_standings})",
        basketball_ref_teams_stats,
        br_year_team_standings
    ))

    # Spotrac
    timings.append(_time_call(
        f"Spotrac: collect team & player URLs (year={spotrac_year_players_urls})",
        spotrac_players_urls,
        spotrac_year_players_urls
    ))
    timings.append(_time_call(
        "Spotrac: scrape all player details",
        spotrac_all_players_stats
    ))

    total_secs = sum(t for _, t, _, _ in timings)

    # Summary print
    print("\n------ Summary ------")
    for name, secs, ok, err in timings:
        status = "OK" if ok else "ERROR"
        print(f"{name:<60} {status:>6}  { _fmt(secs) }")
    print(f"{'-'*80}")
    print(f"{'TOTAL':<60}        { _fmt(total_secs) }")

    # Also return data if you want to log/store it elsewhere
    return {
        "steps": [
            {"name": n, "seconds": s, "ok": ok, "error": err}
            for (n, s, ok, err) in timings
        ],
        "total_seconds": total_secs,
    }


if __name__ == "__main__":
    run_full_pipeline()
