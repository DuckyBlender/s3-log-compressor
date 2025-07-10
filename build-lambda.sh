#!/bin/bash
set -e

echo "Building Rust Lambda function..."

# Change to the rust-compressor directory
cd rust-compressor

# Build the Lambda function for ARM64
echo "Running cargo lambda build --release --arm64..."
cargo lambda build --release --arm64

# Navigate to the build output directory
cd target/lambda/rust-compressor

# Create the lambda zip file
echo "Creating lambda.zip..."
zip lambda.zip bootstrap

# Move the zip file to the project root
echo "Moving lambda.zip to project root..."
mv lambda.zip ../../../../

echo "Build complete! lambda.zip is now in the project root."