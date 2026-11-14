
import json
from pathlib import Path
from pymongo import MongoClient

# Connect to local MongoDB
client = MongoClient("mongodb://localhost:27017")

# Select project database
db = client["ecommerce_analytics"]

# Project data folder
data_folder = Path("data")

# Files and MongoDB collections
datasets = {
    "users": "users.json",
    "products": "products.json",
    "categories": "categories.json",
    "transactions": "transactions.json"
}

print("\nMONGODB DATA IMPORT")
print("=" * 45)

for collection_name, filename in datasets.items():

    file_path = data_folder / filename

    with open(file_path, "r", encoding="utf-8") as file:
        records = json.load(file)

    collection = db[collection_name]

    # Avoid importing duplicate records
    if collection.count_documents({}) > 0:
        print(f"{collection_name}: Already contains data. Skipped.")
        continue

    if records:
        collection.insert_many(records)
        print(f"{collection_name}: Imported {len(records)} records.")
    else:
        print(f"{collection_name}: No records found.")

# Create useful indexes
db.users.create_index("user_id", unique=True)
db.products.create_index("product_id", unique=True)
db.categories.create_index("category_id", unique=True)
db.transactions.create_index("transaction_id", unique=True)
db.transactions.create_index("user_id")

print("\nIMPORT COMPLETED!")
print("Database: ecommerce_analytics")

client.close()
