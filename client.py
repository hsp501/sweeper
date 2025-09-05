import argparse
import hashlib
import json
import os
import stat
from datetime import datetime
from typing import List, Optional, Tuple

import yaml
import zmq

from src import CMD, ChunkHash, HashDB, Util


class Client:
    def __init__(self, config_file: str, max_delete: int, max_scan: int):
        self._copy = {}
        self._zero, self._failed = [], []
        self._deleted, self._scaned, self._shrink_size = 0, 0, 0
        self._max_delete, self._max_scan = max_delete, max_scan

        working_dir = os.path.dirname(os.path.abspath(__file__))

        with open(os.path.join(working_dir, config_file), "r") as f:
            config = yaml.safe_load(f)

            self._dirs = []
            for dir in config["dirs"]:
                self._dirs.append(os.path.abspath(dir))
            self._server = config["server"]

        self._ch = ChunkHash()
        self._db = HashDB(os.path.join(working_dir, config["hash_db"]))

    def start(self):
        context = zmq.Context()
        socket = context.socket(zmq.DEALER)
        socket.connect(self._server)

        for top in self._dirs:
            self._scan_dir(socket, top)

    def stop(self):
        log = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "log",
            f"shrink.{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
        )

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

    def _scan_dir(self, socket, top: str):
        for root, _, files in os.walk(top):
            print(f"{datetime.now().strftime('%H:%M:%S')} shrink {root}")

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
                        socket.send_json(
                            {
                                "command": CMD.FILTER,
                                "filter_id": filter_id,
                                "size": fstat.st_size,
                                "chunk_hashes": chunk_hashes,
                            }
                        )
                        echo = json.loads(socket.recv().decode())
                        assert filter_id == echo["filter_id"]

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
                                        "size": f"fstat.st_size - {Util.bytes_readable(fstat.st_size)}"
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

    return parser.parse_args()


if "__main__" == __name__:
    try:
        args = parse_args()
        client = Client(args.yaml, args.delete, args.scan)
        client.start()
    except KeyboardInterrupt:
        pass
    finally:
        client.stop()
