# s3://s3-log-compressor-sourcebucket-rkeoqdxsxu2w/mock-logs/log_0000.json.gz
# for log_0000 to log_9999
# Base S3 bucket path
bucket_path = "s3://s3-log-compressor-sourcebucket-rkeoqdxsxu2w/mock-logs/"

# Generate manifest entries
manifest_entries = []

for i in range(10000):
    log_filename = f"log_{i:04d}.json.gz"
    s3_path = bucket_path + log_filename
    manifest_entries.append(s3_path)

# Output manifest as plaintext, one entry per line
with open("manifest.txt", "w") as f:
    for entry in manifest_entries:
        f.write(entry + "\n")

print(f"Generated manifest with {len(manifest_entries)} entries")