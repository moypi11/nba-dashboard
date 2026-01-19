import time
import requests
import pandas as pd

BASE_URL = "https://api.balldontlie.io/v1"
API_KEY = "90f5c22c-7b1c-4aa7-8a1e-ff775bcf6308"  # replace with your key

HEADERS = {"Authorization": API_KEY}


def fetch_players(total_players=400, per_page=100, sleep_seconds=0.25):
    all_players = []
    seen_ids = set()
    cursor = None

    while len(all_players) < total_players:
        params = {"per_page": per_page}
        if cursor is not None:
            params["cursor"] = cursor

        response = requests.get(
            f"{BASE_URL}/players",
            headers=HEADERS,
            params=params,
            timeout=30
        )
        response.raise_for_status()

        payload = response.json()
        data = payload.get("data", [])
        meta = payload.get("meta", {})

        if not data:
            break

        for player in data:
            pid = player.get("id")
            if pid not in seen_ids:
                seen_ids.add(pid)
                all_players.append(player)

            if len(all_players) >= total_players:
                break

        cursor = meta.get("next_cursor")
        print(
            f"Fetched batch: {len(data)} | "
            f"Total kept: {len(all_players)} | "
            f"Next cursor: {cursor}"
        )

        if cursor is None:
            break

        time.sleep(sleep_seconds)

    return all_players


def flatten_players(players):
    rows = []
    for p in players:
        team = p.get("team") or {}
        rows.append({
            "id": p.get("id"),
            "first_name": p.get("first_name"),
            "last_name": p.get("last_name"),
            "position": p.get("position"),
            "height": p.get("height"),
            "weight": p.get("weight"),
            "jersey_number": p.get("jersey_number"),
            "college": p.get("college"),
            "country": p.get("country"),
            "draft_year": p.get("draft_year"),
            "draft_round": p.get("draft_round"),
            "draft_number": p.get("draft_number"),
            "team_id": team.get("id"),
            "team_abbreviation": team.get("abbreviation"),
            "team_city": team.get("city"),
            "team_name": team.get("name"),
            "team_conference": team.get("conference"),
            "team_division": team.get("division"),
        })
    return rows


if __name__ == "__main__":
    players = fetch_players(total_players=400, per_page=100)

    df = pd.DataFrame(flatten_players(players))

    # safety check
    print(f"\nRows fetched: {len(df)}")
    print(f"Unique player IDs: {df['id'].nunique()}")

    df.to_csv("players_400.csv", index=False, encoding="utf-8")
    print("Saved -> players_400.csv")
