import os
import json
import random
import string

OUTPUT_DIR = "mock_logs"
NUM_FILES = 10000
MAX_FIELDS = 20

os.makedirs(OUTPUT_DIR, exist_ok=True)

def random_string(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def random_value():
    choice = random.randint(0, 3)
    if choice == 0:
        return random.randint(0, 1000)
    elif choice == 1:
        return round(random.uniform(0, 1000), 2)
    elif choice == 2:
        return random_string(random.randint(5, 15))
    else:
        return random.choice([True, False])

def generate_json():
    data = {}
    for _ in range(random.randint(3, MAX_FIELDS)):
        key = random_string(random.randint(4, 12))
        data[key] = random_value()
    return data

for i in range(NUM_FILES):
    filename = f"log_{i:04d}.json"
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, "w") as f:
        json.dump(generate_json(), f, indent=2)
