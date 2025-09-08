import argparse
import hashlib
import os
import socket
from datetime import datetime
from typing import Any, Dict, List, Tuple

import yaml

from src import Command, Key, Sweeper, Util


class Client(Sweeper):
    def __init__(
        self,
        yaml_file: str,
        max_delete: int,
        max_scan: int,
        local_mode: bool,
        debug: bool,
    ):
        super().__init__(
            True, yaml_file, max_delete=max_delete, max_scan=max_scan, debug=debug
        )

        self._local_mode = local_mode

    def start(self):
        self._show_sweep_dirs()

        self._socket: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((self._host, self._port))

        for top in self._dirs:
            self._shrink(self._socket, top)

    def stop(self) -> Any:
        super().stop()

        return self._flush_log()

    def _shrink(self, _socket: socket.socket, top: str):
        for root, _, files in os.walk(top):
            Util.debug(root, fmt_time=True)

            for file in files:
                path = os.path.join(root, file)
                fstat = os.stat(path, follow_symlinks=False)
                if not Util.important_file(fstat, root, file):
                    if 0 == fstat.st_size:
                        self._stat.update_empty(path)
                    continue
                self._stat.on_scan()

                request_id = f"{self._id}-{path}"
                request_id = hashlib.md5(request_id.encode("utf-8")).hexdigest()

                if self._client_mode and self._stat.skip_scan(path):
                    Util.debug(f"{file}{'~' * 5}SKIP", fmt_indent=3, fmt_time=True)
                    continue

                if not self._compare_size(_socket, request_id, path, fstat):
                    continue

                self._compare_hash(_socket, request_id, path, fstat)

                if self._stat.reach_target():
                    return

    def _compare_size(
        self, _socket: socket.socket, request_id, path: str, fstat: os.stat_result
    ) -> bool:
        if not self._send_json(
            _socket,
            self._req_size(
                request_id=request_id,
                local_mode=self._local_mode,
                path=path,
                size=fstat.st_size,
            ),
        ):
            return False

        echo_message = self._recv_json(_socket)
        if not (
            echo_message
            and Key.RESULT in echo_message
            and Command.ECHO_CHECK_SIZE == echo_message.get(Key.COMMAND, None)
            and request_id == echo_message.get(Key.REQUEST_ID, None)
            and fstat.st_size == echo_message.get(Key.SIZE, -1)
        ):
            Util.debug("unexpected echo message", fmt_indent=3, fmt_time=True)
            return False

        return echo_message[Key.RESULT] > 0

    def _compare_hash(
        self, _socket: socket.socket, request_id, path: str, fstat: os.stat_result
    ):
        fid, chunk_hashes = self._file_details(
            path=path, size=fstat.st_size, mtime=fstat.st_mtime, max_serial=1
        )
        if not chunk_hashes:
            self._record_file_with_error(path)
            return

        blocks = self._ch.blocks(fstat.st_size)
        while True:
            # check chunk hashes
            error, echo_message = self._check_chunk_hashes(
                _socket,
                self._req_hash(
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
                        fmt_indent=12,
                    )
                break

            # update next chunk
            self._update_next_chunk(fid, path, chunk_hashes)

    def _check_chunk_hashes(
        self, _socket: socket.socket, message: Dict
    ) -> Tuple[bool, Any, Dict]:
        # return value (error_flag, echo_message)
        if not self._send_json(_socket, message):
            return True, None

        echo_message = self._recv_json(_socket)
        if not (
            echo_message
            and Key.RESULT in echo_message
            and Command.ECHO_CHECK_HASH == echo_message.get(Key.COMMAND, None)
            and message[Key.REQUEST_ID] == echo_message.get(Key.REQUEST_ID, None)
        ):
            info = "unexpected echo message"
            if echo_message:
                info += f" [{str(echo_message)}]"
            Util.debug(info, fmt_indent=3, fmt_time=True)
            return True, None

        return False, echo_message

    def _update_next_chunk(self, fid: int, path: str, chunk_hashes: List):
        serial = len(chunk_hashes) + 1
        hash, block_size = self._ch.block_hash(path=path, serial=serial)
        self._stat.on_hash(block_size)
        if -1 != fid:
            if self._db.add_chunk_hashes(fid=fid, hashes=[(serial, block_size, hash)]):
                Util.debug(f"{os.path.basename(path)}-[{serial:02d}]", fmt_indent=12)
            else:
                self._record_file_with_error(path)

        chunk_hashes.append({"serial": serial, "block_size": block_size, "hash": hash})

    def _record_file_with_error(self, path: str):
        self._stat.update_error(path)

        Util.debug(f"!!! {os.path.basename(path)}", fmt_indent=3, fmt_time=True)

    def _flush_log(self) -> str:
        log = f"sweeper.{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        log = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log", log)
        os.makedirs(os.path.dirname(log), exist_ok=True)

        with open(log, "w", encoding="utf-8") as f:
            yaml.dump(
                {
                    "stat": f"{Util.bytes_readable(self._stat.shrink_bytes)} from {self._stat.deleted} files, total hash {Util.bytes_readable(self._stat.hash_bytes)}",
                    "error": self._stat.files_error,
                    "empty": self._stat.files_empty,
                    "duplicate": self._stat.files_duplicate,
                },
                f,
                allow_unicode=True,
                sort_keys=False,
                default_flow_style=False,
            )

        return log


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
        default="client.yaml",
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
        client = Client(args.yaml, args.delete, args.scan, args.local, args.debug)
        client.start()
    except KeyboardInterrupt:
        pass
    finally:
        log = client.stop()
        print("")
        Util.debug(f"log: {log}", fmt_time=True)
