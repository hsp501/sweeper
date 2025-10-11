import argparse
import os
import platform

import send2trash

from src import Util


def _scan(top: str, delete: bool = False):
    dirs = []
    dirs_with_files = []

    # 获取所有目录及包含文件的目录
    for root, _, files in os.walk(top):
        dirs.append(root)
        if files:
            dirs_with_files.append(root)

    # 目录排序
    dirs.sort()
    dirs_with_files.sort()
    dirs_blank = []

    for dir in dirs:
        flag_break = False
        for detected in dirs_blank:
            if (dir + os.sep).startswith(detected + os.sep):
                flag_break = True
                break
        if flag_break:
            continue

        flag_break = False
        for dwf in dirs_with_files:
            if (dwf + os.sep).startswith(dir + os.sep):
                flag_break = True
                break
        if not flag_break:
            dirs_blank.append(dir)

    for idx, dir in enumerate(dirs_blank):
        info = dir
        if delete and "Windows" == platform.system():
            try:
                send2trash.send2trash(dir)
                info += " [deleted]"
            except Exception:
                info += " [failed]"
        Util.debug(f"{idx + 1}: {info}", fmt_indent=2)


if "__main__" == __name__:
    parser = argparse.ArgumentParser(description="scanning blank directories")
    parser.add_argument(
        "--delete",
        action="store_true",
        default=False,
        help="move blank directories to recycle bin",
    )
    args = parser.parse_known_args()

    for top in args[1]:
        if os.path.exists(top) and os.path.isdir(top):
            print(f"scaning {top}")
            _scan(top, args[0].delete)
            print("")
