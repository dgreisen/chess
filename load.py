from pathlib import Path
import csv
import json
import sqlite3
import sys

DIR = Path(__file__).parent
CSV_PATH = DIR / 'lichess_db_puzzle.csv'
JSON_PATH = DIR / 'themes.json'
SQLITE_PATH = DIR / 'chess.db'

if not CSV_PATH.exists():
    print("You must download and extract the csv file before running load. `wget https://database.lichess.org/lichess_db_puzzle.csv.zst`", file=sys.stderr)
    sys.exit(1)
SQLITE_PATH.unlink(missing_ok=True)
con = sqlite3.connect(SQLITE_PATH)
cur = con.cursor()
cur.execute('CREATE TABLE puzzle(PuzzleId PRIMARY KEY,FEN,Moves,Rating,RatingDeviation,Popularity,NbPlays,Themes,GameUrl,OpeningFamily,OpeningVariation)')
cur.execute('CREATE TABLE theme(id integer primary key autoincrement, theme, PuzzleId, FOREIGN KEY (PuzzleId) REFERENCES puzzle(PuzzleId))')

fieldnames = ('PuzzleId', 'FEN', 'Moves', 'Rating', 'RatingDeviation', 'Popularity', 'NbPlays', 'Themes', 'GameUrl', 'OpeningFamily', 'OpeningVariation')

themes = {}
puzzle_data = []

with CSV_PATH.open() as f:
    reader = csv.reader(f)
    for i, row in enumerate(reader):
        if i and not i % 100000:
            cur = con.executemany("INSERT INTO puzzle(PuzzleId,FEN,Moves,Rating,RatingDeviation,Popularity,NbPlays,Themes,GameUrl,OpeningFamily,OpeningVariation) VALUES (?,?,?,?,?,?,?,?,?,?,?)", puzzle_data)
            puzzle_data = []
            print(i)
        puzzle_data.append(row + ([''] * max(11-len(row), 0)))
        puzzle_id = row[0]
        puzzle_themes = row[7].split()
        for puzzle_theme in puzzle_themes:
            themes.setdefault(puzzle_theme, []).append(puzzle_id)

con.executemany("INSERT INTO puzzle(PuzzleId,FEN,Moves,Rating,RatingDeviation,Popularity,NbPlays,Themes,GameUrl,OpeningFamily,OpeningVariation) VALUES (?,?,?,?,?,?,?,?,?,?,?)", puzzle_data)

for theme, puzzle_ids in themes.items():
    theme_data = ((theme, puzzle_id) for puzzle_id in puzzle_ids)
    con.executemany("INSERT INTO theme(theme, PuzzleId) VALUES (?,?)", theme_data)

con.commit()

theme_counts = {}
acc = 0
for theme, ids in themes.items():
    count = len(ids)
    theme_counts[theme] = (acc, count)
    acc += count

with JSON_PATH.open('w') as f:
    json.dump(theme_counts, f, indent=2)

