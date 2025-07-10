#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Step 1: Create the corrected Dockerfile ---
# The FROM instruction now points to the correct multi-arch image tag.
# Docker will select the arm64 version based on the --platform flag later.
echo "Creating Dockerfile..."
cat <<EOF > Dockerfile
# Use the official multi-architecture AL2023 image
FROM public.ecr.aws/amazonlinux/amazonlinux:2023

# Install the zip and zstd utilities
RUN dnf install -y zip zstd
EOF

# --- Step 2: Build the Docker image ---
# This command correctly targets the arm64 architecture.
echo "Building Docker image (al2023-utils-builder)..."
docker buildx build --platform linux/arm64 -t al2023-utils-builder .

# --- Step 3: Extract the binaries ---
echo "Creating layer directory structure..."
mkdir -p my-lambda-layer/bin

echo "Creating temporary container..."
CONTAINER_ID=$(docker create al2023-utils-builder)

echo "Copying 'zip' and 'zstd' binaries..."
docker cp "${CONTAINER_ID}:/usr/bin/zip" ./my-lambda-layer/bin/
docker cp "${CONTAINER_ID}:/usr/bin/zstd" ./my-lambda-layer/bin/

echo "Removing temporary container..."
docker rm "${CONTAINER_ID}"

# --- Step 4: Package the Lambda Layer ---
echo "Setting execute permissions..."
chmod +x ./my-lambda-layer/bin/zip
chmod +x ./my-lambda-layer/bin/zstd

echo "Creating lambda-layer.zip..."
cd my-lambda-layer
zip -r ../lambda-layer.zip .
cd ..

# --- Step 5: Clean up ---
echo "Cleaning up local files..."
rm -rf my-lambda-layer
rm Dockerfile

echo "Success! 'lambda-layer.zip' is ready to be uploaded to AWS Lambda."

