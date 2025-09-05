import os
import socket
import stat
from typing import List

import yaml

from src import CMD, ChunkHash, HashDB, Util


class Server:
    def __init__(self, config_file: str):
        self._session = {}
        self._size_group = {}

        working_dir = os.path.dirname(os.path.abspath(__file__))

        with open(os.path.join(working_dir, config_file), "r") as f:
            config = yaml.safe_load(f)

            self._dirs = []
            for dir in config["dirs"]:
                self._dirs.append(os.path.abspath(dir))
            self._server_id = config["id"]
            bind = config["bind"].split(":")
            self._host = bind[0]
            self._port = int(bind[1]) if (len(bind) == 2) else 5555

        self._ch = ChunkHash()
        self._db = HashDB(os.path.join(working_dir, config["hash_db"]))
        self._context = self._socket = None

    def start(self):
        for top in self._dirs:
            self._scan(top)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self._host, self._port))
            s.listen(1)

            while True:
                csocket, caddress = s.accept()
                print(f"client connected: {caddress}")
                self._handle_client(csocket)

    def stop(self):
        if self._socket:
            self._socket.close()

        if self._context:
            self._context.term()

    def _scan(self, top: str):
        for root, _, files in os.walk(top):
            for file in files:
                path = os.path.join(root, file)
                fstat = os.stat(path, follow_symlinks=False)
                if (
                    stat.S_ISREG(fstat.st_mode)
                    and fstat.st_size > 0
                    and "@eaDir" not in root
                ):
                    if fstat.st_size not in self._size_group:
                        self._size_group[fstat.st_size] = []
                    self._size_group[fstat.st_size].append(path)

    def _handle_client(self, csocket: socket.socket):
        while True:
            request = Util.recv_json(csocket)
            if not request:
                break

            if request["command"] != CMD.FILTER:
                csocket.close()
                break

            path = self._filter(
                request["filter_id"],
                request["size"],
                request["chunk_hashes"],
            )

            Util.send_json(
                csocket,
                {
                    "command": CMD.ECHO_FILTER,
                    "server_id": self._server_id,
                    "filter_id": request["filter_id"],
                    "matched_file": path,
                },
            )

    def _filter(self, filter_id: str, size: int, client_hash: List) -> str:
        if filter_id not in self._session:
            self._session[filter_id] = None
            if size in self._size_group:
                self._session[filter_id] = sorted(self._size_group[size])

        while self._session[filter_id]:
            path = self._session[filter_id][0]
            if not self._check_hash(path, client_hash):
                self._session[filter_id].pop(0)
            else:
                return path

        return None

    def _check_hash(self, path: str, client_hash: List) -> bool:
        fs = os.stat(path, follow_symlinks=False)
        fid, server_hash = self._db.get_file_details(
            path=path, size=fs.st_size, mtime=fs.st_mtime
        )

        if -1 == fid:
            fid = self._db.add_file(
                path=os.path.dirname(path),
                name=os.path.basename(path),
                size=fs.st_size,
                mtime=fs.st_mtime,
            )
            assert -1 != fid
            print(f"{' ' * 3}{os.path.basename(path)}")

        if not server_hash:
            server_hash = []

        for chash in client_hash:
            serial = chash["serial"]

            if serial <= len(server_hash) and server_hash[serial - 1] != chash:
                return False
            elif serial > len(server_hash):
                hash, blk_size = self._ch.block_hash(path=path, serial=serial)
                assert self._db.add_chunk_hashes(
                    fid=fid, hashes=[(serial, blk_size, hash)]
                )
                print(f"{' ' * 3}{os.path.basename(path)} - [{serial:03d}]")

                server_hash.append(
                    {"serial": serial, "block_size": blk_size, "hash": hash}
                )

                if server_hash[-1] != chash:
                    return False

        return True


if "__main__" == __name__:
    try:
        server = Server("server.yaml")
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()
