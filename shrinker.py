import argparse
import os
import random
import re
import socket
import traceback
from typing import List, Optional, Tuple

from src import Command, Key, Messanger, Role, Storage, Util


class Shrink(Storage):
    def __init__(
        self,
        yaml_file,
        *,
        debug_mode=False,
        limit_delete=0,
        erase_mode=False,
        step_mode=True,
        erase_blank=False,
    ):
        super().__init__(
            Role.SHRINKER, yaml_file, limit=(limit_delete, 0), debug_mode=debug_mode
        )

        self._erase_mode = erase_mode
        self._step_mode = step_mode
        self._erase_blank = erase_blank

    def _parse_yaml(self, config):
        super()._parse_yaml(config)

        self._local_mode = config.get("local_mode", False)
        self._sweep_dirs = [
            dir
            for dir in self._sweep_dirs
            if os.path.isabs(dir) and os.path.exists(dir)
        ]

    def start(self):
        if not self._sweep_dirs:
            Util.debug("sweeper directory not specified, shrink aborted", fmt_time=True)
            return

        if self._confirm_shrink():
            # 处理 0 字节文件
            if self._erase_blank:
                self._remove_blanks()

            # 处理 重复文件
            if self._erase_mode:
                _socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                _socket.connect((self._host, self._port))
                self._messanger = Messanger(self._device_id, _socket, self._debug_mode)

            for chunk_hash, scan_result in self._config["duplicate"].items():
                if self._stat.reach_limit():
                    Util.debug("shrink limit reached", fmt_time=True)
                    break

                self._remove_duplicates(chunk_hash, scan_result)

            result = "shrink completed"
            if self._stat.deleted > 0:
                result += f", {Util.readable_size(self._stat.shrink_bytes)} freed form {self._stat.deleted} files"
            Util.debug(result, fmt_time=True)
        else:
            Util.debug("shrink aborted by user", fmt_time=True)

    def _remove_blanks(self):
        for file in self._config["blank"]:
            if not Util.check_file_size(file, 0):
                continue

            if not self._step_mode or self._get_user_decision(
                f"[ZERO]delete:  {file} ? (yes/no) [no]: "
            ):
                self._delete_file(file, True)
                if self._step_mode:
                    print("")

    def _get_user_decision(self, prompt, default=False):
        while True:
            answer = input(prompt).strip().lower()
            if not answer:
                return default
            if answer in ("yes", "y"):
                return True
            if answer in ("no", "n"):
                return False

            print("input yes or no")

    def _confirm_shrink(self):
        Util.debug("shrink started", fmt_time=True)
        mode = f"{'erase' if self._erase_mode else 'dry run'} mode"
        if self._erase_mode:
            mode += f": ** {'with' if self._step_mode else 'without'} ** confirmation before deletion"
            mode += f"\n {' ' * 20}** {'delete' if self._erase_blank else 'keep'} ** blank files"
        Util.debug(mode, fmt_indent=9)
        if self._sweep_dirs:
            Util.debug("shrink dirs:", fmt_indent=9)
            for i, dir in enumerate(self._sweep_dirs):
                Util.debug(f"{(i + 1):02d}: {dir}", fmt_indent=12)
        print("")
        Util.debug(f"yaml entry: {os.path.abspath(self._yaml_file)}", fmt_indent=9)
        print("")

        return self._get_user_decision("Proceed? (yes/no) [no]: ", default=False)

    def _delete_file(self, file: str, blank: bool) -> bool:
        head = "ZERO" if blank else "DUPL"

        try:
            if self._erase_mode:
                os.remove(file)

            Util.debug(
                f"[{head}]removed{'' if self._erase_mode else '-dry'}: {file}",
                fmt_time=True,
            )
            return True
        except Exception:
            Util.debug(f"[{head}]failed:  {file}", fmt_time=True)
            traceback.print_exc()
            return False

    def _parse_original(self, original: str) -> Tuple[str, str]:
        match = re.search(r"^original@(.+?):(.*)", original)

        return (match.group(1), match.group(2)) if match else (None, None)

    def _remove_duplicates(self, chunk_hash: str, scan_result: List) -> int:
        size = int(scan_result[0].split("-")[1])
        server_id, file_original = self._parse_original(scan_result[1])
        if not server_id or not file_original:
            return 0

        # 第一遍筛选 文件大小
        files_copy = [
            path for path in scan_result[2:] if Util.check_file_size(path, size)
        ]
        if not files_copy:
            return 0

        if self._local_mode and Util.check_file_size(file_original, size):
            files_copy.append(file_original)

        # 第二遍筛选 sweep 目录的优先级
        files_deletable = self._sort_deletable_files(files_copy)
        if not files_deletable:
            return 0

        # dry run 模式下不需要向服务器请求文件 hash
        if self._erase_mode:
            # 本地模式下随机选择一个文件让 server 计算 hash
            if self._local_mode:
                if len(files_copy) > len(files_deletable):
                    files_safe = [
                        file for file in files_copy if file not in files_deletable
                    ]
                    file_original = random.choices(files_safe)
                else:
                    file_original = random.choices(files_copy)
                file_original = file_original[0]

            server_hash = self._original_file_hash(
                request_id=chunk_hash,
                server_id=server_id,
                path=file_original,
                size=size,
            )
            if not server_hash:
                return 0

            # 第三遍筛选 文件 hash
            files_copy = [
                path
                for path in files_copy
                if (self._local_mode and path == file_original)
                or self._ch.file_hash(path) == server_hash
            ]
            if not files_copy:
                return 0

            # 第四遍筛选 sweep 目录的优先级
            files_deletable = self._sort_deletable_files(files_copy)
            if not files_deletable:
                return 0

        copies, deleted = len(files_copy), 0

        # 本地模式时确保至少保留一个副本
        if self._local_mode:
            copies -= 1

        while deleted < copies and files_deletable:
            file = files_deletable.pop(0)
            if not self._step_mode or self._get_user_decision(
                f"[DUPL]delete:  {file} ? (yes/no) [no]: "
            ):
                deleted += 1
                if self._delete_file(file, False):
                    self._stat.on_erase(size)
                if self._step_mode:
                    print("")

        return deleted

    def _original_file_hash(
        self, *, request_id: str, server_id: str, path: str, size: int
    ) -> Optional[str]:
        msg = self._msg_builder.req_calc_file_hash(
            device_id=self._device_id,
            request_id=request_id,
            server_id=server_id,
            path=path,
            size=size,
        )
        if not self._messanger.send_json(msg):
            return None

        echo_message = self._messanger.recv_json()
        if (
            echo_message
            and Command.ECHO_CALC_FILE_HASH == echo_message.get(Key.COMMAND, None)
            and request_id == echo_message.get(Key.REQUEST_ID, None)
        ):
            return echo_message.get(Key.RESULT, None)
        else:
            return None

    def _sort_deletable_files(self, files_copy: List) -> List:
        sorted_copy = []

        for dir in self._sweep_dirs:
            for file in files_copy:
                if file in sorted_copy:
                    continue

                if Util.is_parent_dir(dir, file):
                    sorted_copy.append(file)

        return sorted_copy

    def parse_duplicate_directory(self):
        dir_stat = {}
        for _, scan_result in self._config["duplicate"].items():
            if self._local_mode:
                _, file_original = self._parse_original(scan_result[1])
                dir = os.path.dirname(file_original)
                if dir not in dir_stat:
                    dir_stat[dir] = 1
                else:
                    dir_stat[dir] += 1

            for file in scan_result[2:]:
                dir = os.path.dirname(file)
                if dir not in dir_stat:
                    dir_stat[dir] = 1
                else:
                    dir_stat[dir] += 1

        Util.debug("duplicate directory list:", fmt_time=True)
        for i, dir in enumerate(sorted(dir_stat.keys())):
            Util.debug(f"{(i + 1):03d}: [{dir_stat[dir]:04d}] {dir}", fmt_indent=3)


def check_max(value):
    _max = int(value)
    if _max < 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" % _max)

    return _max


def parse_args():
    parser = argparse.ArgumentParser(description="delete files by log entries")

    parser.add_argument(
        "--yaml", required=True, help="the yaml stat file produced by sweeper client"
    )

    parser.add_argument(
        "--parse",
        action="store_true",
        default=False,
        help="parse yaml stat file and output directory which duplicate files locate in",
    )

    parser.add_argument(
        "--erase",
        action="store_true",
        default=False,
        help="actually delete the files, default is dry run",
    )

    parser.add_argument(
        "--blank",
        action="store_true",
        default=False,
        help="delete files which is 0 bytes",
    )

    parser.add_argument(
        "--auto",
        action="store_true",
        default=False,
        help="delete files without prompt",
    )

    parser.add_argument(
        "--delete",
        type=check_max,
        default=0,
        help="max number of files to delete",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="debug mode, show more detail logs",
    )

    return parser.parse_args()


if "__main__" == __name__:
    try:
        args = parse_args()
        shrinker = Shrink(
            args.yaml,
            debug_mode=args.debug,
            limit_delete=args.delete,
            erase_mode=args.erase,
            step_mode=not args.auto,
            erase_blank=args.blank,
        )
        if args.parse:
            shrinker.parse_duplicate_directory()
        else:
            shrinker.start()
    except KeyboardInterrupt:
        pass
