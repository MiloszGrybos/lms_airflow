import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

random.seed(42)

CONTENTS = [
    "Great app 😀",
    "Love it!!! 😍😍",
    "Works fine, nothing special.",
    "Crashes all the time 😡",
    "ok",
    "Good",
    "Bad",
    "Too many ads... uninstalling 👎",
    "Best app ever ❤️🔥",
    "Great app 😀",
    "Great app 😀",
    "Works fine, nothing special.",
    "Please fix the login bug!",
    "🙂",
    None,
]

rows = []
start = datetime(2024, 1, 1, 8, 0, 0)
for i in range(1, 61):
    created = start + timedelta(days=random.randint(0, 6), minutes=random.randint(0, 1000))
    rows.append(
        {
            "review_id": i,
            "user_name": random.choice(["anna", "bob", "carl", None]),
            "content": random.choice(CONTENTS),
            "score": random.choice([1, 2, 3, 4, 5, None]),
            "created_date": created.strftime("%Y-%m-%d %H:%M:%S"),
        }
    )

df = pd.DataFrame(rows)
out = Path(__file__).resolve().parent.parent / "data" / "input" / "reviews.csv"
df.to_csv(out, index=False)
print(f"Zapisano {len(df)} wierszy do {out}")
