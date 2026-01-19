import os

os.system("python scripts/api/step1_teams.py")
os.system("python scripts/api/step2_players.py")
os.system("python scripts/db/step3_load_players_teams_to_mysql.py")
os.system("python scripts/api/step4_games.py")
os.system("python scripts/db/step5_load_games_in_mysql.py")
