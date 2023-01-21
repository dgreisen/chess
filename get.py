from pathlib import Path
import json
import sqlite3
from random import randint
from collections import namedtuple
import argparse
import sys
from datetime import datetime

Count = namedtuple('Count', ('offset', 'count', 'total'))
DIR = Path(__file__).parent
CSV_PATH = DIR / 'lichess_db_puzzle.csv'
JSON_PATH = DIR / 'themes.json'
SQLITE_PATH = DIR / 'chess.db'

with JSON_PATH.open() as f:
    all_theme_counts = json.load(f)

con = sqlite3.connect(SQLITE_PATH)
cur = con.cursor()

def get_puzzles(num_picks, *themes):
    start_time = datetime.now().timestamp()
    theme_counts = []
    total = 0
    for theme, (offset, count) in all_theme_counts.items():
        if theme not in themes:
            continue
        total += count
        theme_counts.append(Count(offset, count, total))

    while True:
        while True:
            raw_picks = sorted([randint(0, theme_counts[-1].total) + 1 for _ in range(num_picks)])
            if len(raw_picks) == len(set(raw_picks)):
                break

        picks = []
        for raw_pick in raw_picks:
            for theme_count in theme_counts:
                if raw_pick < theme_count.total:
                    picks.append(raw_pick + theme_count.offset - theme_count.total + theme_count.count)
                    break


        results = cur.execute(f'select PuzzleId,FEN,Moves,Rating,RatingDeviation,Popularity,NbPlays,Themes,GameUrl,OpeningFamily,OpeningVariation from puzzle natural join theme where theme.id in ({",".join("?"*len(picks))})', picks).fetchall()
        if len(results) == len(set(results)):
            break
    end_time = datetime.now().timestamp()
    results = {
        'results': results,
        'time': end_time - start_time,
    }
    return results

arg_parser = argparse.ArgumentParser(
    prog = 'Random Chess Puzzles',
    description = 'Return random chess puzzles that match one or more themes'
)
arg_parser.add_argument('--theme', action='append', dest='themes')
arg_parser.add_argument('--count', default=5, type=int)

if __name__ == '__main__':
    args = arg_parser.parse_args()
    if not args.themes:
        print('You must specify at least one theme.', file=sys.stderr)
        sys.exit(1)
    picks = get_puzzles(args.count, *args.themes)
    print(json.dumps(picks, indent=2))
