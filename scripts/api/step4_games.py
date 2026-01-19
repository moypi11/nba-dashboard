import time
import requests
import pandas as pd
from datetime import date, timedelta

BASE_URL = "https://api.balldontlie.io/v1"
API_KEY = "90f5c22c-7b1c-4aa7-8a1e-ff775bcf6308"
HEADERS = {"Authorization": API_KEY}


def fetch_games_by_date_range(
    start_date: str,
    end_date: str,
    per_page: int = 25,
    sleep_seconds: float = 1.5
):
    games = []
    cursor = None

    while True:
        params = {
            "per_page": per_page,
            "start_date": start_date,
            "end_date": end_date,
        }
        if cursor:
            params["cursor"] = cursor

        r = requests.get(f"{BASE_URL}/games",
                         headers=HEADERS, params=params, timeout=30)

        if r.status_code == 429:
            print("⚠️ Rate limited. Sleeping 10s...")
            time.sleep(10)
            continue

        r.raise_for_status()
        payload = r.json()

        data = payload.get("data", [])
        meta = payload.get("meta", {})

        if not data:
            break

        games.extend(data)
        cursor = meta.get("next_cursor")

        print(f"{start_date} → {end_date} | fetched {len(data)} | total {len(games)}")

        if not cursor:
            break

        time.sleep(sleep_seconds)

    return games


def flatten_games(games, season: int):
    rows = []
    for g in games:
        home = g.get("home_team") or {}
        visitor = g.get("visitor_team") or {}
        game_date = (g.get("date") or "")[:10]

        rows.append({
            "game_id": g.get("id"),
            "game_date": game_date,
            "season": season,
            "home_team_id": home.get("id"),
            "visitor_team_id": visitor.get("id"),
            "home_team_score": g.get("home_team_score"),
            "visitor_team_score": g.get("visitor_team_score"),
            "postseason": int(bool(g.get("postseason"))),
            "status": g.get("status"),
        })
    return rows


def month_ranges(season: int):
    # NBA season roughly Oct–Jun
    ranges = [
        ("10-01", "10-31"),
        ("11-01", "11-30"),
        ("12-01", "12-31"),
        ("01-01", "01-31"),
        ("02-01", "02-28"),
        ("03-01", "03-31"),
        ("04-01", "04-30"),
        ("05-01", "05-31"),
        ("06-01", "06-30"),
    ]

    out = []
    for start, end in ranges:
        y_start = season if start.startswith(
            ("10", "11", "12")) else season + 1
        y_end = season if end.startswith(("10", "11", "12")) else season + 1
        out.append((f"{y_start}-{start}", f"{y_end}-{end}"))

    return out


if __name__ == "__main__":
    SEASON = 2023
    all_rows = []

    for start_date, end_date in month_ranges(SEASON):
        games = fetch_games_by_date_range(start_date, end_date)
        all_rows.extend(flatten_games(games, SEASON))

        print(f"Completed {start_date} → {end_date}\n")

    df = pd.DataFrame(all_rows).drop_duplicates(subset=["game_id"])
    df.to_csv(f"games_{SEASON}.csv", index=False)

    print(f"\nSaved games_{SEASON}.csv")
    print("Total games:", len(df))
