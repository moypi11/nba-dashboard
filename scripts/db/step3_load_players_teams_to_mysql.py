import pandas as pd
import mysql.connector
from typing import Any

MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "intel5-9400F",
    "database": "nba_dashboard",
}


def connect():
    return mysql.connector.connect(**MYSQL_CONFIG)


def none_if_nan(x: Any):
    # Converts pandas NaN / NaT to None for MySQL NULL
    if x is None:
        return None
    try:
        # pd.isna works for NaN, NaT, None
        if pd.isna(x):
            return None
    except Exception:
        pass
    return x


def to_int_or_none(x: Any):
    x = none_if_nan(x)
    if x is None or x == "":
        return None
    try:
        return int(x)
    except Exception:
        return None


def to_str_or_none(x: Any):
    x = none_if_nan(x)
    if x is None:
        return None
    s = str(x).strip()
    return s if s != "" else None


def load_teams(csv_path: str):
    df = pd.read_csv(csv_path)

    # Convert NaN -> None everywhere
    df = df.where(pd.notna(df), None)

    sql = """
    INSERT INTO teams (team_id, abbreviation, city, conference, division, full_name, name)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      abbreviation=VALUES(abbreviation),
      city=VALUES(city),
      conference=VALUES(conference),
      division=VALUES(division),
      full_name=VALUES(full_name),
      name=VALUES(name);
    """

    values = []
    for r in df.itertuples(index=False):
        values.append((
            to_int_or_none(getattr(r, "team_id")),
            to_str_or_none(getattr(r, "abbreviation")),
            to_str_or_none(getattr(r, "city")),
            to_str_or_none(getattr(r, "conference")),
            to_str_or_none(getattr(r, "division")),
            to_str_or_none(getattr(r, "full_name")),
            to_str_or_none(getattr(r, "name")),
        ))

    cnx = connect()
    cur = cnx.cursor()
    cur.executemany(sql, values)
    cnx.commit()
    cur.close()
    cnx.close()

    print(f"Loaded teams: {len(values)}")


def load_players(csv_path: str):
    df = pd.read_csv(csv_path)

    # Convert NaN -> None everywhere
    df = df.where(pd.notna(df), None)

    sql = """
    INSERT INTO players
    (id, first_name, last_name, position, height, weight, jersey_number, college, country,
     draft_year, draft_round, draft_number, team_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      first_name=VALUES(first_name),
      last_name=VALUES(last_name),
      position=VALUES(position),
      height=VALUES(height),
      weight=VALUES(weight),
      jersey_number=VALUES(jersey_number),
      college=VALUES(college),
      country=VALUES(country),
      draft_year=VALUES(draft_year),
      draft_round=VALUES(draft_round),
      draft_number=VALUES(draft_number),
      team_id=VALUES(team_id);
    """

    values = []
    for r in df.itertuples(index=False):
        values.append((
            to_int_or_none(getattr(r, "id")),
            to_str_or_none(getattr(r, "first_name")),
            to_str_or_none(getattr(r, "last_name")),
            to_str_or_none(getattr(r, "position")),
            to_str_or_none(getattr(r, "height")),         # keep as string
            to_int_or_none(getattr(r, "weight")),
            to_str_or_none(getattr(r, "jersey_number")),
            to_str_or_none(getattr(r, "college")),
            to_str_or_none(getattr(r, "country")),
            to_int_or_none(getattr(r, "draft_year")),
            to_int_or_none(getattr(r, "draft_round")),
            to_int_or_none(getattr(r, "draft_number")),
            to_int_or_none(getattr(r, "team_id")),
        ))

    cnx = connect()
    cur = cnx.cursor()
    cur.executemany(sql, values)
    cnx.commit()
    cur.close()
    cnx.close()

    print(f"Loaded players: {len(values)}")


if __name__ == "__main__":
    load_teams("teams.csv")
    load_players("players_400.csv")
    print("Done.")
