# app.py
# NBA Dashboard (Teams • Players • Games) + derived team_daily_results
# Adds "ALL" option in Team dropdown to show ONLY the All Teams averages table.

import re
import pandas as pd
import mysql.connector
from mysql.connector import Error

from dash import Dash, dcc, html, dash_table, Input, Output
import plotly.express as px

# ----------------------------
# MySQL Config (EDIT PASSWORD)
# ----------------------------
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = "intel5-9400F"  # <-- change this
MYSQL_DATABASE = "nba_dashboard"


def get_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )


def read_sql(query: str, params: tuple | None = None) -> pd.DataFrame:
    """Read SQL into a DataFrame; returns a DF with 'error' column on failure."""
    try:
        cnx = get_connection()
        df = pd.read_sql(query, cnx, params=params)
        cnx.close()
        return df
    except Error as e:
        return pd.DataFrame({"error": [str(e)], "query": [query]})


# ----------------------------
# Data access helpers
# ----------------------------
def get_team_abbr_col() -> str:
    """
    Detect the team abbreviation column name from the teams table.
    Returns the real column name (e.g., 'abbreviation', 'team_abbreviation', 'abbrev', 'abbr').
    """
    df = read_sql("SELECT * FROM teams LIMIT 1;")
    if "error" in df.columns or df.empty:
        return "abbreviation"  # fallback

    cols = set(df.columns.str.lower())
    candidates = ["abbreviation", "team_abbreviation", "abbrev", "abbr"]
    for c in candidates:
        if c in cols:
            return next(x for x in df.columns if x.lower() == c)

    return ""


def get_seasons() -> list[int]:
    df = read_sql("SELECT DISTINCT season FROM games ORDER BY season DESC;")
    if "error" in df.columns or df.empty:
        return []
    return df["season"].dropna().astype(int).tolist()


def get_teams() -> pd.DataFrame:
    return read_sql("""
        SELECT team_id, abbreviation, full_name
        FROM teams
        ORDER BY full_name;
    """)


def get_team_timeseries(team_id: int, season: int) -> pd.DataFrame:
    return read_sql("""
        SELECT game_date, points_for, points_against, wins, losses
        FROM team_daily_results
        WHERE team_id = %s AND season = %s
        ORDER BY game_date;
    """, (team_id, season))


def get_team_recent_games(team_id: int, season: int, limit: int = 105) -> pd.DataFrame:
    return read_sql(f"""
        SELECT
          g.game_date,
          g.home_team_id,
          ht.abbreviation AS home_abbr,
          ht.full_name AS home_team,
          g.home_team_score,
          g.visitor_team_id,
          vt.abbreviation AS visitor_abbr,
          vt.full_name AS visitor_team,
          g.visitor_team_score,
          g.postseason,
          g.status
        FROM games g
        JOIN teams ht ON ht.team_id = g.home_team_id
        JOIN teams vt ON vt.team_id = g.visitor_team_id
        WHERE g.season = %s AND (g.home_team_id = %s OR g.visitor_team_id = %s)
        ORDER BY g.game_date DESC
        LIMIT {int(limit)};
    """, (season, team_id, team_id))


def get_players_by_team(team_id: int, limit: int = 300) -> pd.DataFrame:
    return read_sql(f"""
        SELECT id, first_name, last_name, position, height, weight, jersey_number, college, country
        FROM players
        WHERE team_id = %s
        ORDER BY last_name, first_name
        LIMIT {int(limit)};
    """, (team_id,))


def get_season_date_bounds(season: int) -> tuple[str | None, str | None]:
    df = read_sql(
        "SELECT MIN(game_date) AS min_date, MAX(game_date) AS max_date FROM games WHERE season = %s;",
        (season,)
    )
    if "error" in df.columns or df.empty:
        return None, None
    return df.loc[0, "min_date"], df.loc[0, "max_date"]


def get_leaderboard(season: int) -> pd.DataFrame:
    query = """
        SELECT
          t.team_id AS team_id,
          t.abbreviation AS team_abbr,
          t.full_name AS team,
          SUM(r.wins) AS wins,
          SUM(r.losses) AS losses,
          ROUND(
            SUM(r.wins) / NULLIF(SUM(r.wins) + SUM(r.losses), 0),
            3
          ) AS win_pct,
          (SUM(r.points_for) - SUM(r.points_against)) AS total_point_diff,
          ROUND(SUM(r.points_for) / NULLIF(SUM(r.games_played), 0), 1) AS avg_points_for,
          ROUND(SUM(r.points_against) / NULLIF(SUM(r.games_played), 0), 1) AS avg_points_against,
          ROUND(
            (SUM(r.points_for) - SUM(r.points_against)) / NULLIF(SUM(r.games_played), 0),
            1
          ) AS avg_point_diff
        FROM team_daily_results r
        JOIN teams t ON t.team_id = r.team_id
        WHERE r.season = %s
        GROUP BY t.team_id, t.full_name
        ORDER BY wins DESC, avg_point_diff DESC;
    """
    return read_sql(query, (season,))
# ----------------------------
# All Teams averages helpers
# ----------------------------


def height_to_inches(h):
    """Convert '6-6' -> 78 inches. Returns None if invalid."""
    if h is None:
        return None
    s = str(h).strip()
    m = re.match(r"^\s*(\d+)\s*-\s*(\d+)\s*$", s)
    if not m:
        return None
    feet = int(m.group(1))
    inches = int(m.group(2))
    return feet * 12 + inches


def get_all_teams_avg_height_weight() -> pd.DataFrame:
    abbr_col = get_team_abbr_col()
    if not abbr_col:
        return pd.DataFrame({"error": ["No team abbreviation column found in teams table."]})

    df = read_sql(f"""
        SELECT
          t.team_id,
          t.full_name AS team,
          t.{abbr_col} AS team_abbr,
          p.height,
          p.weight
        FROM teams t
        LEFT JOIN players p
          ON p.team_id = t.team_id;
    """)

    if "error" in df.columns or df.empty:
        return df

    df["height_in"] = df["height"].apply(height_to_inches)
    df["weight_num"] = pd.to_numeric(df["weight"], errors="coerce")

    out = (
        df.groupby(["team_id", "team", "team_abbr"], as_index=False)
        .agg(
            avg_height_in=("height_in", "mean"),
            avg_weight=("weight_num", "mean"),
            players_count=("team_id", "count"),
        )
    )

    def inches_to_ft_in(x):
        if pd.isna(x):
            return None
        x = float(x)
        ft = int(x // 12)
        inch = int(round(x % 12))
        if inch == 12:
            ft += 1
            inch = 0
        return f"{ft}-{inch}"

    out["avg_height"] = out["avg_height_in"].apply(inches_to_ft_in)
    out["avg_weight"] = out["avg_weight"].round(1)

    out["team_with_logo"] = out.apply(
        lambda r: md_team_with_logo(r["team_abbr"], r["team"]), axis=1
    )

    return out[["team_with_logo", "avg_height", "avg_weight", "players_count"]]


# ----------------------------
# Build app (IMPORTANT: suppress_callback_exceptions)
# ----------------------------
app = Dash(__name__, suppress_callback_exceptions=True)
app.title = "NBA Dashboard (Teams • Players • Games)"


def team_logo_src(team_abbr: str) -> str:
    # Dash serves files from /assets automatically
    return app.get_asset_url(f"logos/{team_abbr}.png")


def md_team_with_logo(team_abbr: str, team_name: str) -> str:
    logo_url = app.get_asset_url(f"logos/{team_abbr}.png")
    return (
        f"<img src='{logo_url}' "
        f"style='height:20px; vertical-align:middle; margin-right:6px;'/>"
        f"{team_name}"
    )


teams_df = get_teams()
seasons = get_seasons()

default_season = int(seasons[0]) if seasons else 2023
default_team_id = int(teams_df["team_id"].iloc[0]) if not teams_df.empty else 1


def kpi_card(title: str, value: str) -> html.Div:
    return html.Div(
        style={
            "border": "1px solid #ddd",
            "borderRadius": "10px",
            "padding": "12px",
            "backgroundColor": "white",
        },
        children=[
            html.Div(title, style={"fontSize": "12px", "color": "#666"}),
            html.Div(value, style={"fontSize": "22px",
                     "fontWeight": "bold", "marginTop": "6px"}),
        ],
    )


# ----------------------------
# Layout
# ----------------------------
app.layout = html.Div(
    style={"maxWidth": "1200px", "margin": "20px auto", "fontFamily": "Arial"},
    children=[
        html.Div(
            style={"display": "flex", "alignItems": "center",
                   "gap": "10px", "marginBottom": "6px"},
            children=[
                html.Img(
                    src=app.get_asset_url("nba_logo.png"),
                    style={"height": "45px"}  # adjust size here
                ),
                html.H1("NBA Dashboard", style={"margin": "0"}),
            ],
        ),


        # Controls
        html.Div(
            style={
                "display": "grid",
                "gridTemplateColumns": "1fr 1fr",
                "gap": "12px",
                "marginBottom": "16px",
            },
            children=[
                html.Div([
                    html.Label("Season", style={"fontWeight": "bold"}),
                    dcc.Dropdown(
                        id="season-dd",
                        options=(
                            [{"label": str(s), "value": int(s)}
                             for s in seasons]
                            if seasons
                            else [{"label": str(default_season), "value": int(default_season)}]
                        ),
                        value=int(default_season),
                        clearable=False,
                    ),
                ]),
                html.Div([
                    html.Label("Team", style={"fontWeight": "bold"}),
                    dcc.Dropdown(
                        id="team-dd",
                        options=(
                            [{"label": "All Teams (Avg Height & Weight)", "value": "ALL"}] +
                            [{"label": r["full_name"], "value": int(r["team_id"])}
                             for r in teams_df.to_dict("records")]
                            if not teams_df.empty
                            else [{"label": "All Teams (Avg Height & Weight)", "value": "ALL"}]
                        ),
                        value="ALL" if teams_df.empty else int(
                            default_team_id),
                        clearable=False,
                        searchable=True,
                    ),
                ]),
            ],
        ),

        # TEAM-SPECIFIC SECTION
        html.Div(
            id="team-specific-section",
            style={
                "display": "grid",
                "gridTemplateColumns": "2fr 1fr",  # left big, right small
                "gap": "16px",
                "alignItems": "start",
            },
            children=[
                # KPIs + charts + tables
                html.Div(
                    children=[
                        html.Div(
                            id="kpi-row",
                            style={
                                "display": "grid",
                                "gridTemplateColumns": "repeat(5, 1fr)",
                                "gap": "12px",
                                "marginBottom": "18px",
                            },
                        ),

                        html.Div(
                            style={
                                "display": "grid",
                                "gridTemplateColumns": "1fr 1fr",
                                "gap": "16px",
                                "marginBottom": "18px",
                            },
                            children=[
                                html.Div([
                                    html.H3("Points For Over Time",
                                            style={"margin": "6px 0"}),
                                    dcc.Graph(
                                        id="points-line", config={"displayModeBar": False}, style={"height": "340px"}),
                                ]),
                                html.Div([
                                    html.H3("Cumulative Wins Over Time",
                                            style={"margin": "6px 0"}),
                                    dcc.Graph(
                                        id="wins-line", config={"displayModeBar": False}, style={"height": "340px"}),
                                ]),
                            ],
                        ),

                        html.Div(
                            style={"marginBottom": "18px"},
                            children=[
                                html.H3("Point Differential Over Time",
                                        style={"margin": "6px 0"}),
                                dcc.Graph(
                                    id="diff-line", config={"displayModeBar": False}, style={"height": "340px"}),
                            ],
                        ),

                        html.Div(
                            style={"display": "grid",
                                   "gridTemplateColumns": "1fr 1fr", "gap": "12px"},
                            children=[
                                html.Div([
                                    html.H3("Recent Games", style={
                                            "margin": "6px 0"}),
                                    dash_table.DataTable(
                                        id="recent-games-table",
                                        markdown_options={"html": True},
                                        page_size=15,
                                        style_table={"overflowX": "auto"},
                                        style_header={
                                            "backgroundColor": "rgb(240,240,240)", "fontWeight": "bold"},
                                        style_cell={
                                            "textAlign": "left", "padding": "6px", "whiteSpace": "normal"},
                                    ),
                                ]),
                                html.Div([
                                    html.H3("Players (on team)",
                                            style={"margin": "6px 0"}),
                                    dash_table.DataTable(
                                        id="players-table",
                                        page_size=12,
                                        style_table={"overflowX": "auto"},
                                        style_header={
                                            "backgroundColor": "rgb(240,240,240)", "fontWeight": "bold"},
                                        style_cell={
                                            "textAlign": "left", "padding": "6px", "whiteSpace": "normal"},
                                        filter_action="native",
                                        sort_action="native",
                                    ),
                                ]),
                            ],
                        ),
                    ]
                ),

                # Leaderboard
                html.Div(
                    # Stays visible while scrolling
                    style={"position": "sticky", "top": "12px"},
                    children=[
                        html.H3("Leaderboard",
                                style={"margin": "6px 0"}),
                        dash_table.DataTable(
                            id="leaderboard-table",
                            markdown_options={"html": True},
                            page_size=30,
                            style_table={"overflowX": "auto"},
                            style_header={
                                "backgroundColor": "rgb(240,240,240)", "fontWeight": "bold"},
                            style_cell={"textAlign": "left",
                                        "padding": "6px", "whiteSpace": "normal"},
                            sort_action="native",
                            style_cell_conditional=[
                                {"if": {"column_id": "rank"}, "width": "55px",
                                 "maxWidth": "55px", "textAlign": "center"},
                                {"if": {"column_id": "wins"},
                                 "width": "70px", "textAlign": "center"},
                                {"if": {"column_id": "losses"},
                                 "width": "70px", "textAlign": "center"},
                                {"if": {"column_id": "win_pct"},
                                 "width": "80px", "textAlign": "center"},
                            ],
                        ),
                    ],
                ),
            ],
        ),
        # ALL-TEAMS SECTION
        html.Div(
            id="all-teams-section",
            style={"marginTop": "18px"},
            children=[
                html.H3("All Teams — Average Height & Weight",
                        style={"margin": "6px 0"}),
                dash_table.DataTable(
                    id="team-avgs-table",
                    markdown_options={"html": True},
                    page_size=30,
                    style_table={"overflowX": "auto"},
                    style_header={
                        "backgroundColor": "rgb(240,240,240)", "fontWeight": "bold"},
                    style_cell={"textAlign": "left",
                                "padding": "6px", "whiteSpace": "normal"},
                    sort_action="native",
                    filter_action="native",
                ),
            ],
        ),

        html.Hr(style={"margin": "18px 0"}),

        html.Details(
            children=[
                html.Summary("Troubleshooting (click)", style={
                             "cursor": "pointer", "fontWeight": "bold"}),
                html.Div(
                    style={"marginTop": "10px", "color": "#333"},
                    children=[
                        html.Div("If you see an error in tables, check:"),
                        html.Ul([
                            html.Li("MySQL is running."),
                            html.Li("MYSQL_PASSWORD in app.py is correct."),
                            html.Li("Database name is nba_dashboard."),
                            html.Li(
                                "Tables exist: teams, players, games, team_daily_results."),
                        ]),
                    ],
                )
            ]
        ),
    ],
)

# ----------------------------
# Toggle which section shows
# ----------------------------


@app.callback(
    Output("team-specific-section", "style"),
    Output("all-teams-section", "style"),
    Input("team-dd", "value"),
)
def toggle_sections(team_value):
    if team_value == "ALL":
        return {"display": "none"}, {"display": "block", "marginTop": "18px"}
    return {"display": "block"}, {"display": "none"}


# ----------------------------
# Main callback
# ----------------------------
@app.callback(
    Output("kpi-row", "children"),
    Output("points-line", "figure"),
    Output("wins-line", "figure"),
    Output("diff-line", "figure"),
    Output("recent-games-table", "data"),
    Output("recent-games-table", "columns"),
    Output("players-table", "data"),
    Output("players-table", "columns"),
    Output("team-avgs-table", "data"),
    Output("team-avgs-table", "columns"),
    Output("leaderboard-table", "data"),
    Output("leaderboard-table", "columns"),
    Input("team-dd", "value"),
    Input("season-dd", "value"),

)
def update_dashboard(team_value, season_value):
    import traceback

    empty_fig = px.line(pd.DataFrame({"x": [], "y": []}), x="x", y="y")
    empty_fig.update_layout(height=320, margin=dict(l=20, r=20, t=10, b=20))

    try:
        season = int(
            season_value) if season_value is not None else default_season

        # All teams avg table
        team_avgs = get_all_teams_avg_height_weight()
        if "error" in team_avgs.columns:
            team_avgs_data = [{"error": team_avgs.iloc[0]["error"]}]
            team_avgs_cols = [{"name": "error", "id": "error"}]
        else:
            team_avgs_data = team_avgs.to_dict("records")
            team_avgs_cols = []
            for c in team_avgs.columns:
                if c == "team_with_logo":
                    team_avgs_cols.append(
                        {"name": "Team", "id": "team_with_logo", "presentation": "markdown"})
                else:
                    team_avgs_cols.append({"name": c, "id": c})

        # Leaderboard (Whole Season)
        leader = get_leaderboard(season)
        if "error" not in leader.columns and not leader.empty:
            # win_pct to percent string
            leader["win_pct"] = (
                (leader["win_pct"] * 100).round(1).astype(str) + "%")

            # ✅ Rank + Team logo markdown
            leader = leader.reset_index(drop=True)
            leader["rank"] = leader.index + 1
            leader["team_with_logo"] = leader.apply(
                lambda r: md_team_with_logo(r["team_abbr"], r["team"]), axis=1
            )

            # Put columns in a nice order (optional)
            leader = leader[[
                "rank", "team_with_logo",
                "wins", "losses", "win_pct",
                "avg_points_for", "avg_points_against",
                "avg_point_diff", "total_point_diff"
            ]]

        if "error" in leader.columns:
            leader_data = [{"error": leader.iloc[0]["error"]}]
            leader_cols = [{"name": "error", "id": "error"}]
        else:
            leader_data = leader.to_dict("records")

            # ✅ Make Team column markdown
            leader_cols = []
            for c in leader.columns:
                if c == "team_with_logo":
                    leader_cols.append(
                        {"name": "Team", "id": "team_with_logo", "presentation": "markdown"})
                else:
                    leader_cols.append({"name": c, "id": c})

        # ALL mode
        if team_value == "ALL":
            kpis = [
                kpi_card("Mode", "All Teams"),
                kpi_card("Season", str(season)),
                kpi_card("Win %", "—"),
                kpi_card("—", "—"),
                kpi_card("—", "—"),
            ]
            return (
                kpis,
                empty_fig,
                empty_fig,
                empty_fig,
                [], [],              # recent games
                [], [],              # players
                team_avgs_data,
                team_avgs_cols,
                leader_data,
                leader_cols,
            )

        # Team Mode
        team_id = int(team_value)

        ts = get_team_timeseries(team_id, season)
        if "error" in ts.columns:
            err = ts.iloc[0]["error"]
            kpis = [
                kpi_card("Status", "DB Error"),
                kpi_card("Details", str(err)[:28] + "..."),
                kpi_card("Team ID", str(team_id)),
                kpi_card("Season", str(season)),
            ]
            return (
                kpis,
                empty_fig,
                empty_fig,
                empty_fig,
                [{"error": err}],
                [{"name": "error", "id": "error"}],
                [{"error": err}],
                [{"name": "error", "id": "error"}],
                team_avgs_data,
                team_avgs_cols,
                leader_data,
                leader_cols,
            )

        # KPIs
        total_games = int(ts["wins"].sum() +
                          ts["losses"].sum()) if not ts.empty else 0
        total_wins = int(ts["wins"].sum()) if not ts.empty else 0
        total_losses = int(ts["losses"].sum()) if not ts.empty else 0
        win_pct = (total_wins / total_games * 100) if total_games > 0 else 0.0
        avg_pf = float(ts["points_for"].mean()) if not ts.empty else 0.0
        avg_pa = float(ts["points_against"].mean()) if not ts.empty else 0.0

        kpis = [
            kpi_card("Games", f"{total_games}"),
            kpi_card("Wins – Losses", f"{total_wins} – {total_losses}"),
            kpi_card("Win %", f"{win_pct:.1f}%"),
            kpi_card("Avg Points For", f"{avg_pf:.1f}"),
            kpi_card("Avg Points Against", f"{avg_pa:.1f}"),
        ]

        # Figures
        points_fig = px.line(ts, x="game_date", y="points_for")
        points_fig.update_layout(
            height=320, margin=dict(l=20, r=20, t=10, b=20))

        ts2 = ts.copy()
        ts2["cum_wins"] = ts2["wins"].cumsum()
        wins_fig = px.line(ts2, x="game_date", y="cum_wins")
        wins_fig.update_layout(height=320, margin=dict(l=20, r=20, t=10, b=20))

        ts3 = ts.copy()
        if not ts3.empty:
            ts3["point_diff"] = ts3["points_for"] - ts3["points_against"]
        diff_fig = px.line(ts3, x="game_date", y="point_diff")
        diff_fig.update_layout(height=320, margin=dict(l=20, r=20, t=10, b=20))

        # Tables
        recent = get_team_recent_games(team_id, season, limit=105)
        if "error" not in recent.columns and not recent.empty:
            recent["home_team_with_logo"] = recent.apply(
                lambda r: md_team_with_logo(r["home_abbr"], r["home_team"]), axis=1
            )
            recent["visitor_team_with_logo"] = recent.apply(
                lambda r: md_team_with_logo(r["visitor_abbr"], r["visitor_team"]), axis=1
            )

            recent = recent[[
                "game_date",
                "home_team_with_logo", "home_team_score",
                "visitor_team_with_logo", "visitor_team_score",
                "postseason", "status"
            ]]
        if "error" in recent.columns:
            recent_data = [{"error": recent.iloc[0]["error"]}]
            recent_cols = [{"name": "error", "id": "error"}]
        else:
            recent_data = recent.to_dict("records")
            recent_cols = []
            for c in recent.columns:
                if c in ("home_team_with_logo", "visitor_team_with_logo"):
                    nice = "Home Team" if c == "home_team_with_logo" else "Visitor Team"
                    recent_cols.append(
                        {"name": nice, "id": c, "presentation": "markdown"})
                else:
                    recent_cols.append({"name": c, "id": c})

        players = get_players_by_team(team_id, limit=300)
        if "error" in players.columns:
            players_data = [{"error": players.iloc[0]["error"]}]
            players_cols = [{"name": "error", "id": "error"}]
        else:
            players_data = players.to_dict("records")
            players_cols = [{"name": c, "id": c} for c in players.columns]

        return (
            kpis,
            points_fig,
            wins_fig,
            diff_fig,
            recent_data,
            recent_cols,
            players_data,
            players_cols,
            team_avgs_data,
            team_avgs_cols,
            leader_data,
            leader_cols,
        )

    except Exception as e:
        print("Callback crashed:\n", traceback.format_exc())
        err = f"{type(e).__name__}: {e}"

        # Safe fallback for all outputs
        kpis = [
            kpi_card("Status", "Callback Error"),
            kpi_card("Details", err[:28] + "..."),
            kpi_card("Team", str(team_value)),
            kpi_card("Season", str(season_value)),
        ]
        return (
            kpis,
            empty_fig,
            empty_fig,
            empty_fig,
            [{"error": err}],
            [{"name": "error", "id": "error"}],
            [{"error": err}],
            [{"name": "error", "id": "error"}],
            [{"error": err}],
            [{"name": "error", "id": "error"}],
            [{"error": err}],
            [{"name": "error", "id": "error"}],

        )


if __name__ == "__main__":
    # Install:
    #   pip install dash plotly pandas mysql-connector-python
    #
    # Run:
    #   python app.py
    #
    # Open:
    #   http://127.0.0.1:8050
    app.run(debug=True)
