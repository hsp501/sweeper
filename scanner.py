import argparse
import hashlib
import os
import socket
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import yaml

from src import Command, Key, Messanger, Role, Sweeper, Util


class Scanner(Sweeper):
    def __init__(
        self, yaml_file: str, *, local_mode: bool, debug_mode: bool, limit: Tuple
    ):
        super().__init__(
            Role.SCANNER,
            yaml_file,
            limit=limit,
            debug_mode=debug_mode,
        )

        self._local_mode = local_mode
        self._session_id = (
            f"{Util.random_string(3)}[{datetime.now().strftime('%H%M%S')}]"
        )

    def start(self):
        self._stat.group_by_size(self._sweep_dirs)
        self._show_sweep_dirs()

        _socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        _socket.connect((self._host, self._port))
        self._messanger = Messanger(self._device_id, _socket, self._debug_mode)

        # 优先处理大文件
        for size in sorted(self._stat.size_group.keys(), reverse=True):
            group_files = self._stat.size_group[size]
            files = len(group_files)

            if not self._local_mode:
                request_id = (
                    f"{self._device_id}:{self._session_id}-size inquiry-[{size}]"
                )
                if not self._compare_size(False, request_id, request_id, size):
                    self._stat.on_scan(files)
                    continue

            reach_limit, flg_hashed = self._shrink(group_files)

            if flg_hashed:
                Util.debug(
                    f"size group shrinked{[size]} with {files} files, total: {self._stat.files_to_scan}, scaned: {self._stat.scaned}, left: {self._stat.files_to_scan - self._stat.scaned}\n",
                    fmt_time=True,
                )

            if reach_limit:
                break

    def stop(self) -> Any:
        super().stop()

        return self._flush_stat()

    def _shrink(self, group_files: List[str]) -> Tuple[bool, bool]:
        flag_hashed = False

        for path in sorted(group_files):
            if self._stat.reach_limit():
                return True, flag_hashed

            self._stat.on_scan()

            if self._local_mode and self._stat.skip_scan(path):
                Util.debug(
                    f"{os.path.basename(path)}{'~' * 5}SKIP",
                    fmt_time=True,
                )
                continue

            fstat = Util.stat(path)
            if not fstat:
                continue

            request_id = f"{self._device_id}-{path}"
            request_id = (
                hashlib.md5(request_id.encode("utf-8")).hexdigest()
                + f"-{self._session_id}"
            )

            if not self._compare_size(
                self._local_mode, request_id, path, fstat.st_size
            ):
                continue

            self._compare_hash(request_id, path, fstat)
            flag_hashed = True

        return False, flag_hashed

    def _compare_size(
        self,
        local_mode,
        request_id,
        path: str,
        size: int,
    ) -> bool:
        msg = self._msg_builder.req_size(
            device_id=self._device_id,
            request_id=request_id,
            local_mode=local_mode,
            path=path,
            size=size,
        )
        if not self._messanger.send_json(msg):
            return False

        echo_message = self._messanger.recv_json()
        if not (
            echo_message
            and Key.RESULT in echo_message
            and Command.ECHO_CHECK_SIZE == echo_message.get(Key.COMMAND, None)
            and request_id == echo_message.get(Key.REQUEST_ID, None)
            and size == echo_message.get(Key.SIZE, -1)
        ):
            Util.debug("unexpected echo message", fmt_indent=3, fmt_time=True)
            return False

        return echo_message[Key.RESULT] > 0

    def _compare_hash(self, request_id, path: str, fstat: os.stat_result):
        fid, chunk_hashes = self._file_details(
            path=path, size=fstat.st_size, mtime=fstat.st_mtime, request_id=request_id
        )
        if not chunk_hashes:
            self._record_file_with_error(path)
            return

        flag_time = True
        blocks = self._ch.blocks(fstat.st_size)
        while True:
            # check chunk hashes
            error, echo_message = self._check_chunk_hashes(
                self._msg_builder.req_hash(
                    device_id=self._device_id,
                    request_id=request_id,
                    local_mode=self._local_mode,
                    path=path,
                    size=fstat.st_size,
                    chunk_hashes=chunk_hashes,
                ),
            )
            # unique file found or error
            if error or echo_message[Key.RESULT] is None:
                break

            # no more chunk
            if len(chunk_hashes) == blocks:
                if self._stat.on_duplicate(
                    server_id=echo_message[Key.DEVICE_ID],
                    server_path=echo_message[Key.RESULT],
                    chunk_hashes=chunk_hashes,
                    client_path=path,
                    free_space=fstat.st_size,
                    local_mode=self._local_mode,
                ):
                    Util.debug(
                        f"{os.path.basename(path)}{'-' * 5}COPY",
                        fmt_indent=(0 if flag_time else 9),
                        fmt_time=flag_time,
                    )
                break

            # update next chunk
            flag_time = False
            self._update_next_chunk(fid, path, chunk_hashes)

    def _check_chunk_hashes(self, msg: Dict) -> Tuple[bool, Optional[Dict]]:
        # return value (error_flag, echo_message)
        if not self._messanger.send_json(msg):
            return True, None

        echo_message = self._messanger.recv_json()
        if not (
            echo_message
            and Key.RESULT in echo_message
            and Command.ECHO_CHECK_HASH == echo_message.get(Key.COMMAND, None)
            and msg[Key.REQUEST_ID] == echo_message.get(Key.REQUEST_ID, None)
        ):
            info = "unexpected echo message"
            if echo_message:
                info += f" [{str(echo_message)}]"
            Util.debug(info, fmt_time=True)
            return True, None

        return False, echo_message

    def _update_next_chunk(self, fid: int, path: str, chunk_hashes: List):
        serial = len(chunk_hashes) + 1
        hash, block_size = self._ch.block_hash(path=path, serial=serial)
        self._stat.on_hash(block_size)
        if -1 != fid:
            if self._db.add_chunk_hashes(fid=fid, hashes=[(serial, block_size, hash)]):
                Util.debug(f"{os.path.basename(path)}-[{serial:02d}]", fmt_indent=9)
            else:
                self._record_file_with_error(path)

        chunk_hashes.append({"serial": serial, "block_size": block_size, "hash": hash})

    def _record_file_with_error(self, path: str):
        self._stat.update_error(path)

        Util.debug(f"!!! {os.path.basename(path)}", fmt_time=True)

    def _flush_stat(self) -> str:
        f_stat = f"sweeper.{datetime.now().strftime('%Y%m%d_%H%M%S')}.yaml"
        f_stat = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log", f_stat)
        os.makedirs(os.path.dirname(f_stat), exist_ok=True)

        with open(f_stat, "w", encoding="utf-8") as f:
            stat = {}
            stat["total"] = f"{self._stat.files_to_scan} files"
            stat["freed"] = (
                f"{Util.readable_size(self._stat.shrink_bytes)} from {self._stat.deleted} files"
            )
            stat["hashed"] = f"{Util.readable_size(self._stat.hash_bytes)}"
            yaml.dump(
                {
                    "id": self._device_id,
                    "local_mode": self._local_mode,
                    "server": f"{self._host}:{self._port}",
                    "sweep_dirs": [
                        "*** absolute path in which duplicate files will be deleted ***"
                    ],
                    "-": "-" * 90,
                    "stat": stat,
                    "scanned_dirs": self._sweep_dirs,
                    "file_extensions": sorted(self._stat.extensions),
                    "error": self._stat.files_error,
                    "blank": self._stat.files_0bytes,
                    "duplicate": self._stat.files_duplicate,
                },
                f,
                allow_unicode=True,
                sort_keys=False,
                default_flow_style=False,
            )

        return f_stat


def check_max(value):
    _max = int(value)
    if _max < 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" % _max)

    return _max


def parse_args():
    parser = argparse.ArgumentParser(
        description="find & clean duplicate files to release disk space"
    )

    parser.add_argument(
        "--yaml",
        default="scanner.yaml",
        help="the yaml config of directory list from where to compare file & release space",
    )

    parser.add_argument(
        "--delete",
        type=check_max,
        default=0,
        help="max number of files to delete",
    )

    parser.add_argument(
        "--scan",
        type=check_max,
        default=0,
        help="max number of files to scan",
    )

    parser.add_argument(
        "--local",
        action="store_true",
        default=False,
        help="client & server running on local mode, don't compare the same path file",
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
        scanner = Scanner(
            args.yaml,
            local_mode=args.local,
            debug_mode=args.debug,
            limit=(args.delete, args.scan),
        )
        scanner.start()
    except KeyboardInterrupt:
        pass
    finally:
        log = scanner.stop()
        print("")
        Util.debug(f"log: {log}", fmt_time=True)
