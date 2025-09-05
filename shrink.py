import argparse
import os
import re


def main():
    parser = argparse.ArgumentParser(description="delete files by log entries")
    parser.add_argument("--log", help="the file list to be deleted")
    args = parser.parse_args()

    with open(args.log, "r") as f:
        counter = 1
        for line in f:
            result = re.match(r"\s+- \d+\s+(.*)", line)
            if result:
                file = result.groups()[0]
                if os.path.exists(file) and os.path.isfile(file):
                    os.remove(file)
                    print(f"{counter}: {file}")
                    counter += 1


if "__main__" == __name__:
    main()
