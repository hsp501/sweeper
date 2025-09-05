import argparse
import sys
import xml.etree.ElementTree as ET

from src import cleanup


class color:
    red = "\033[91m"
    green = "\033[92m"
    blue = "\033[94m"
    end = "\033[0m"


def check_max_deletion(value):
    max_deletion = int(value)
    if max_deletion < 0:
        raise argparse.ArgumentTypeError(
            "%s is an invalid positive int value" % max_deletion
        )

    return max_deletion


def print_sample_xml():
    print(f"{' * ' * 5}sample xml config format{' * ' * 5}\n")

    print("""<?xml version="1.0" encoding="UTF-8"?>
<directories>
    <protected role="files in these directories will stay the same">
        <dir>/home/hsp501/data/pictures</dir>
        <dir>/home/hsp501/data2/fold2</dir>
    </protected>
    
    <redudant role="duplicate files in these directories will be deleted">
        <dir>/home/hsp501/data09/fold</dir>
        <dir>/home/hsp501/data10/fold</dir>
    </redudant>
</directories>""")


def parse_xml(xml_file: str):
    protected_dirs = []
    redudant_dirs = []

    tree = ET.parse(xml_file)
    for category in tree.getroot():
        dirs = redudant_dirs if category.tag == "redudant" else protected_dirs

        for dir in category:
            dirs.append(dir.text)

    return protected_dirs, redudant_dirs


def main():
    parser = argparse.ArgumentParser(
        description="find & clean duplicate files to release disk space"
    )
    parser.add_argument(
        "--format", action="store_true", help="print the format of xml config file"
    )
    parser.add_argument(
        "--xconf",
        help="the xml config of directory list from where to compare file & release space",
    )
    parser.add_argument(
        "--max",
        type=check_max_deletion,
        default=0,
        help="max number of files to delete",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="no file will be deleted"
    )
    args = parser.parse_args()

    if args.format:
        print_sample_xml()
        sys.exit(0)

    if args.xconf:
        protected_dirs, redudant_dirs = parse_xml(args.xconf)
        if len(protected_dirs) == 0 or len(redudant_dirs) == 0:
            print("no protected or redudant directories configed")
            sys.exit(1)

        cleaner = cleanup(args.max, args.dry_run)

        print("protected directories:")
        for dir in protected_dirs:
            print(f"{' ' * 2}{dir}")
            cleaner.watch(dir=dir, redundant=False)

        print(
            "\nredudant directories (*** duplicate files in these directories will be deleted ***):"
        )
        for dir in redudant_dirs:
            print(f"{color.red}{' ' * 2}{dir}{color.end}")
            cleaner.watch(dir=dir, redundant=True)
        print("")

        select = input(
            "begin to free disk space by deleting duplicate files (yes/no)? "
        )
        if "yes" == select.lower():
            cleaner.shrink()


if "__main__" == __name__:
    main()
