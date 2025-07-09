import os
import json
import random
import string
from tqdm import tqdm

OUTPUT_DIR = "mock_logs"
NUM_FILES = 10000

FIXED_KEYS = [
    "timestamp", "level", "message", "source_ip", "user_id", "request_id",
    "http_method", "http_path", "http_status", "user_agent", "response_time_ms",
    "app_version", "service_name", "region", "payload"
]

os.makedirs(OUTPUT_DIR, exist_ok=True)

def random_string(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def random_value(key):
    if key == "timestamp":
        return f"2025-07-09T{random.randint(0,23):02}:{random.randint(0,59):02}:{random.randint(0,59):02}.{random.randint(0,999):03}Z"
    elif key == "level":
        return random.choice(["INFO", "WARN", "ERROR", "DEBUG"])
    elif key == "message":
        return random_string(random.randint(50, 150))
    elif key == "source_ip":
        return f"{random.randint(1,254)}.{random.randint(1,254)}.{random.randint(1,254)}.{random.randint(1,254)}"
    elif key == "user_id":
        return f"user-{random.randint(1000, 9999)}"
    elif key == "request_id":
        return random_string(32)
    elif key == "http_method":
        return random.choice(["GET", "POST", "PUT", "DELETE"])
    elif key == "http_path":
        return "/" + "/".join([random_string(random.randint(5,10)) for _ in range(random.randint(1,3))])
    elif key == "http_status":
        return random.choice([200, 201, 400, 404, 500])
    elif key == "user_agent":
        return "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    elif key == "response_time_ms":
        return random.randint(10, 500)
    elif key == "app_version":
        return f"{random.randint(1,5)}.{random.randint(0,9)}.{random.randint(0,9)}"
    elif key == "service_name":
        return random.choice(["auth-service", "product-service", "order-service"])
    elif key == "region":
        return random.choice(["us-east-1", "us-west-2", "eu-central-1"])
    elif key == "payload":
        return random_string(2500)
    else:
        return random_string(10)


def generate_json():
    data = {}
    for key in FIXED_KEYS:
        data[key] = random_value(key)
    return data

for i in tqdm(range(NUM_FILES)):
    filename = f"log_{i:04d}.json"
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, "w") as f:
        json.dump(generate_json(), f, indent=2)
