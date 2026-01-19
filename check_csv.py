import csv

path = "players_400.csv"
expected_cols = 18

with open(path, newline="", encoding="utf-8") as f:
    reader = csv.reader(f)
    header = next(reader)
    print("Header columns:", len(header))

    for i, row in enumerate(reader, start=2):
        if len(row) != expected_cols:
            print(f"Line {i}: {len(row)} columns")
            print(row)
            break
