
import json
import re
from pathlib import Path

# Read the original file
with open("dataset_generator.py", "r", encoding="utf-8") as f:
    notebook = json.load(f)

# Extract the actual Python code
code = "".join(notebook["cells"][0]["source"])

# Reduce the dataset for our first test
settings = {
    "NUM_USERS": 100,
    "NUM_PRODUCTS": 50,
    "NUM_CATEGORIES": 10,
    "NUM_TRANSACTIONS": 100,
    "NUM_SESSIONS": 200
}

for name, value in settings.items():
    code = re.sub(
        rf"^{name}\s*=.*$",
        f"{name} = {value}",
        code,
        flags=re.MULTILINE
    )

# Create the data folder
Path("data").mkdir(exist_ok=True)

# Save all generated files inside data/
code = code.replace('"users.json"', '"data/users.json"')
code = code.replace('"products.json"', '"data/products.json"')
code = code.replace('"categories.json"', '"data/categories.json"')
code = code.replace('"transactions.json"', '"data/transactions.json"')
code = code.replace('f"sessions_{i//CHUNK_SIZE}.json"', 'f"data/sessions_{i//CHUNK_SIZE}.json"')

# Save the corrected Python script
with open("dataset_generator_fixed.py", "w", encoding="utf-8") as f:
    f.write(code)

print("Success! dataset_generator_fixed.py has been created.")
