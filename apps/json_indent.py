import argparse
import json
from json import JSONDecodeError

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pretty-prints any JSON file")

    parser.add_argument("file", type=str, help="file name containing JSON content")

    args = parser.parse_args()

    filename = args.file

    try:
        with open(filename, "r") as f:
            file_contents = f.read()
            parsed = json.loads(file_contents)
            pretty = json.dumps(parsed, indent=2)
            print(pretty)

    except FileNotFoundError as e:
        print(f"Error reading file ({filename}): {e}")
        exit(1)
    except OSError as e:
        print(f"Error reading file ({filename}): {e}")
        exit(1)
    except JSONDecodeError as e:
        print(f"Error reading file ({filename}): {e}")
        exit(1)
