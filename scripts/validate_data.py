
import json
from pathlib import Path

data_folder = Path("data")

files = [
    "users.json",
    "products.json",
    "categories.json",
    "transactions.json",
    "sessions_0.json"
]

print("\nDATASET VALIDATION")
print("-" * 40)

for filename in files:
    file_path = data_folder / filename

    if not file_path.exists():
        print(f"{filename}: NOT FOUND")
        continue

    with open(file_path, "r", encoding="utf-8") as file:
        records = json.load(file)

    print(f"{filename}: {len(records)} records")

    if records:
        print("Sample record fields:", list(records[0].keys()))
    print("-" * 40)
