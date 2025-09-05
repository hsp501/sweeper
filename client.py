import argparse
import hashlib
import os
import socket
import stat
from datetime import datetime
from typing import List, Optional, Tuple

import yaml

from src import CMD, ChunkHash, HashDB, Util


class Client:
    def __init__(self, config_file: str, max_delete: int, max_scan: int, local: bool):
        self._socket = None

        self._copy = {}
        self._zero, self._failed = [], []
        self._deleted, self._scaned, self._shrink_size = 0, 0, 0

        self._local = local
        self._max_delete, self._max_scan = max_delete, max_scan

        working_dir = os.path.dirname(os.path.abspath(__file__))

        with open(os.path.join(working_dir, config_file), "r") as f:
            config = yaml.safe_load(f)

            self._dirs = []
            for dir in config["dirs"]:
                self._dirs.append(os.path.abspath(dir))
            server = config["server"].split(":")
            self._host = server[0]
            self._port = int(server[1]) if len(server) == 2 else 5555

        self._ch = ChunkHash()
        self._db = HashDB(os.path.join(working_dir, config["hash_db"]))

    def start(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((self._host, self._port))

        print("directory list:")
        for top in self._dirs:
            print(f"{' ' * 3} {top}")
        print("")

        for top in self._dirs:
            self._scan_dir(self._socket, top)

    def stop(self) -> str:
        if self._socket:
            try:
                self._socket.close()
            except Exception:
                pass

        log = f"shrink.{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        log = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log", log)
        os.makedirs(os.path.dirname(log), exist_ok=True)

        with open(log, "w", encoding="utf-8") as f:
            yaml.dump(
                {
                    "failed": self._failed,
                    "zero": self._zero,
                    "stat": {
                        "files": self._deleted,
                        "space": Util.bytes_readable(self._shrink_size),
                    },
                    "copy": self._copy,
                },
                f,
                allow_unicode=True,
                sort_keys=False,
                default_flow_style=False,
            )

        return log

    def _scan_dir(self, tcp_socket, top: str):
        for root, _, files in os.walk(top):
            print(f"{datetime.now().strftime('%H:%M:%S')} {root}")

            for file in files:
                path = os.path.join(root, file)
                fstat = os.stat(path, follow_symlinks=False)
                if stat.S_ISREG(fstat.st_mode) and "@eaDir" not in root:
                    if 0 == fstat.st_size:
                        self._zero.append(path)
                        continue

                    self._scaned += 1

                    details = self._details_from_db(path, fstat.st_size, fstat.st_mtime)
                    if not details:
                        self._record_failed(path)
                        continue

                    fid, chunk_hashes = details
                    blocks = self._ch.blocks(path)
                    filter_id = hashlib.md5(path.encode("utf-8")).hexdigest()
                    while True:
                        Util.send_json(
                            tcp_socket,
                            {
                                "command": CMD.FILTER,
                                "filter_id": filter_id,
                                "path": path,
                                "local": self._local,
                                "size": fstat.st_size,
                                "chunk_hashes": chunk_hashes,
                            },
                        )
                        echo = Util.recv_json(tcp_socket)
                        assert (
                            echo
                            and filter_id == echo["filter_id"]
                            and CMD.ECHO_FILTER == echo["command"]
                        )

                        if echo["matched_file"] is None:
                            # print(f"{' ' * 3}+++ {file}")
                            break

                        serial = chunk_hashes[-1]["serial"]
                        assert serial <= blocks

                        if serial == blocks:
                            self._deleted += 1
                            self._shrink_size += fstat.st_size

                            server = f"{echo['server_id']}-{echo['matched_file']}"
                            if server not in self._copy:
                                self._copy[server] = [
                                    {
                                        "size": f"{fstat.st_size} - {Util.bytes_readable(fstat.st_size)}"
                                    }
                                ]
                            self._copy[server].append(path)

                            print(f"{' ' * 3}--- {file}")
                            break

                        serial += 1
                        hash, block_size = self._ch.block_hash(path=path, serial=serial)
                        if not self._db.add_chunk_hashes(
                            fid=fid, hashes=[(serial, block_size, hash)]
                        ):
                            self._record_failed(path)
                            break

                        chunk_hashes = [
                            {"serial": serial, "block_size": block_size, "hash": hash}
                        ]

                    if (self._max_scan > 0 and self._scaned >= self._max_scan) or (
                        self._max_delete > 0 and self._deleted >= self._max_delete
                    ):
                        return

            print("")

    def _details_from_db(
        self, path: str, size: int, mtime: float
    ) -> Optional[Tuple[int, List]]:
        fid, chunk_hashes = self._db.get_file_details(path=path, size=size, mtime=mtime)

        if -1 == fid:
            fid = self._db.add_file(
                path=os.path.dirname(path),
                name=os.path.basename(path),
                size=size,
                mtime=mtime,
            )
            if -1 == fid:
                return None

        if not chunk_hashes:
            hash, block_size = self._ch.block_hash(path=path, serial=1)
            if not self._db.add_chunk_hashes(fid=fid, hashes=[(1, block_size, hash)]):
                return None

            chunk_hashes = [{"serial": 1, "block_size": block_size, "hash": hash}]

        return fid, chunk_hashes

    def _record_failed(self, path: str):
        self._failed.append(path)

        print(f"{' ' * 3}!!! {os.path.basename(path)}")


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

    return parser.parse_args()


if "__main__" == __name__:
    try:
        args = parse_args()
        client = Client(args.yaml, args.delete, args.scan, args.local)
        client.start()
    except KeyboardInterrupt:
        pass
    finally:
        print(client.stop())
