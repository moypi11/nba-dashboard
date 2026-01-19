import requests
import pandas as pd

BASE_URL = "https://api.balldontlie.io/v1"
API_KEY = "90f5c22c-7b1c-4aa7-8a1e-ff775bcf6308"

HEADERS = {
    "Authorization": API_KEY
}


def fetch_teams():
    response = requests.get(
        f"{BASE_URL}/teams",
        headers=HEADERS,
        timeout=30
    )
    response.raise_for_status()

    payload = response.json()
    return payload.get("data", [])


def flatten_teams(teams):
    rows = []
    for t in teams:
        rows.append({
            "team_id": t.get("id"),
            "abbreviation": t.get("abbreviation"),
            "city": t.get("city"),
            "conference": t.get("conference"),
            "division": t.get("division"),
            "full_name": t.get("full_name"),
            "name": t.get("name"),
        })
    return rows


if __name__ == "__main__":
    teams = fetch_teams()
    df = pd.DataFrame(flatten_teams(teams))

    print(f"Teams fetched: {len(df)}")
    print(df.head())

    df.to_csv("teams.csv", index=False, encoding="utf-8")
    print("Saved -> teams.csv")
