import pandas as pd
import mysql.connector

MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "intel5-9400F",
    "database": "nba_dashboard",
}


def connect():
    return mysql.connector.connect(**MYSQL_CONFIG)


def none_if_nan(x):
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    return x


def to_int_or_none(x):
    x = none_if_nan(x)
    if x is None or x == "":
        return None
    try:
        return int(float(x))  # handles "1.0" type values too
    except Exception:
        return None


def to_str_or_none(x):
    x = none_if_nan(x)
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def load_games(csv_path: str, batch_size: int = 500):
    df = pd.read_csv(csv_path)
    df = df.where(pd.notna(df), None)

    sql = """
    INSERT INTO games
    (game_id, game_date, season, home_team_id, visitor_team_id,
     home_team_score, visitor_team_score, postseason, status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      game_date=VALUES(game_date),
      season=VALUES(season),
      home_team_id=VALUES(home_team_id),
      visitor_team_id=VALUES(visitor_team_id),
      home_team_score=VALUES(home_team_score),
      visitor_team_score=VALUES(visitor_team_score),
      postseason=VALUES(postseason),
      status=VALUES(status);
    """

    cnx = connect()
    cur = cnx.cursor()

    total = 0
    batch = []

    for r in df.itertuples(index=False):
        batch.append((
            to_int_or_none(getattr(r, "game_id")),
            to_str_or_none(getattr(r, "game_date")),   # YYYY-MM-DD
            to_int_or_none(getattr(r, "season")),
            to_int_or_none(getattr(r, "home_team_id")),
            to_int_or_none(getattr(r, "visitor_team_id")),
            to_int_or_none(getattr(r, "home_team_score")),
            to_int_or_none(getattr(r, "visitor_team_score")),
            to_int_or_none(getattr(r, "postseason")),
            to_str_or_none(getattr(r, "status")),
        ))

        if len(batch) >= batch_size:
            cur.executemany(sql, batch)
            cnx.commit()
            total += len(batch)
            print(f"Inserted/updated: {total}")
            batch = []

    # last batch
    if batch:
        cur.executemany(sql, batch)
        cnx.commit()
        total += len(batch)
        print(f"Inserted/updated: {total}")

    cur.close()
    cnx.close()
    print("Done loading games.")


if __name__ == "__main__":
    load_games("games_2023.csv")
