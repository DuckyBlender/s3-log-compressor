import os
import re

# Path to the mock script and the directory containing generated logs
MOCK_SCRIPT_PATH = "mock.py"
LOG_DIR = "/Users/alan/Downloads/s3-log-compressor-sourcebucket-rkeoqdxsxu2w 2/mock-logs"

def get_num_files_from_script(script_path):
    """Extracts the NUM_FILES variable from the mock script."""
    try:
        with open(script_path, "r") as f:
            content = f.read()
            match = re.search(r"^NUM_FILES\s*=\s*(\d+)", content, re.MULTILINE)
            if match:
                return int(match.group(1))
    except FileNotFoundError:
        print(f"Error: Mock script not found at '{script_path}'")
    except Exception as e:
        print(f"An error occurred while reading the script: {e}")
    return None

def get_expected_filenames(num_files):
    """Generates a set of expected filenames."""
    return {f"log_{i:04d}.json" for i in range(num_files)}

def get_actual_filenames(directory):
    """Gets a set of actual filenames from a directory."""
    try:
        if not os.path.isdir(directory):
            print(f"Error: Directory not found at '{directory}'")
            return set()
        return {item for item in os.listdir(directory) if os.path.isfile(os.path.join(directory, item))}
    except Exception as e:
        print(f"An error occurred while reading directory '{directory}': {e}")
        return set()

def count_files_in_directory(directory):
    """Counts the number of files in a given directory."""
    try:
        if not os.path.isdir(directory):
            print(f"Error: Directory not found at '{directory}'")
            return 0
        
        # Count only files, not subdirectories
        file_count = sum(1 for item in os.listdir(directory) if os.path.isfile(os.path.join(directory, item)))
        return file_count
    except Exception as e:
        print(f"An error occurred while counting files: {e}")
        return 0

def main():
    """Main function to verify the number of mock log files."""
    print("Starting verification process...")

    # Correct path for mock script
    mock_script_full_path = os.path.abspath(MOCK_SCRIPT_PATH)

    # 1. Get the expected number of files from the mock script
    expected_files_count = get_num_files_from_script(mock_script_full_path)
    if expected_files_count is None:
        print("Could not determine the expected number of files. Exiting.")
        return

    print(f"Expected number of files based on '{mock_script_full_path}': {expected_files_count}")

    # 2. Get expected and actual filenames
    expected_filenames = get_expected_filenames(expected_files_count)
    actual_filenames = get_actual_filenames(LOG_DIR)
    actual_files_count = len(actual_filenames)
    
    print(f"Found {actual_files_count} files in '{LOG_DIR}'.")

    # 3. Compare and report the result
    if actual_files_count == expected_files_count:
        print("\nVerification successful!")
        print(f"The number of files in '{LOG_DIR}' matches the expected count of {expected_files_count}.")
    else:
        print("\nVerification failed!")
        print(f"Expected {expected_files_count} files, but found {actual_files_count}.")
        
        missing_files = expected_filenames - actual_filenames
        if missing_files:
            print(f"\nFound {len(missing_files)} missing files:")
            # Sort for consistent output and limit to 100 for readability
            sorted_missing = sorted(list(missing_files))
            for filename in sorted_missing[:100]:
                print(f"  - {filename}")
            if len(sorted_missing) > 100:
                print(f"  ... and {len(sorted_missing) - 100} more.")

        extra_files = actual_filenames - expected_filenames
        if extra_files:
            print(f"\nFound {len(extra_files)} extra files:")
            sorted_extra = sorted(list(extra_files))
            for filename in sorted_extra[:100]:
                 print(f"  - {filename}")
            if len(sorted_extra) > 100:
                print(f"  ... and {len(sorted_extra) - 100} more.")

if __name__ == "__main__":
    main()
