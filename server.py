import argparse
import os
import socket
from typing import Dict, List

from src import Command, Key, Sweeper, Util


class Server(Sweeper):
    def __init__(self, yaml_file: str, debug: bool):
        super().__init__(False, yaml_file, debug=debug)

    def start(self):
        self._stat.group_by_size(self._dirs)
        self._show_sweep_dirs()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self._host, self._port))
            s.listen(1)

            while True:
                self._session = {}
                csocket, caddress = s.accept()
                Util.debug(f"client connected: {caddress}", fmt_time=True)
                self._handle_request(csocket)

    def _handle_request(self, _socket: socket.socket):
        while True:
            request = self._recv_json(_socket)
            if not request:
                break

            if request[Key.COMMAND] == Command.CHECK_SIZE:
                self._handle_req_size(_socket, request)
            elif request[Key.COMMAND] == Command.CHECK_HASH:
                self._handle_req_hash(_socket, request)
            else:
                _socket.close()
                break

    def _handle_req_size(self, _socket: socket.socket, request: Dict):
        result = 0

        size = request[Key.SIZE]
        files = self._stat.size_group.get(size, None)
        if files:
            result = len(files)
            if request[Key.LOCAL_MODE] and request[Key.PATH] in files:
                result -= 1

        self._send_json(
            _socket,
            self._echo_size(
                request_id=request[Key.REQUEST_ID],
                size=size,
                files=result,
            ),
        )

    def _handle_req_hash(self, _socket: socket.socket, request: Dict):
        path = self._filter_by_hash(
            request_id=request[Key.REQUEST_ID],
            local_mode=request[Key.LOCAL_MODE],
            client_path=request[Key.PATH],
            size=request[Key.SIZE],
            client_hash=request[Key.HASH],
        )

        self._send_json(
            _socket,
            self._echo_hash(
                request_id=request[Key.REQUEST_ID],
                path=path,
            ),
        )

    def _filter_by_hash(
        self,
        *,
        request_id: str,
        local_mode: bool,
        client_path: str,
        size: int,
        client_hash: List,
    ) -> str:
        if request_id not in self._session:
            self._session[request_id] = None
            if size in self._stat.size_group:
                self._session[request_id] = sorted(self._stat.size_group[size])

        while self._session[request_id]:
            path = self._session[request_id][0]
            if local_mode and path == client_path:
                self._session[request_id].pop(0)
                continue

            if not self._check_hash(path, client_path, client_hash):
                self._session[request_id].pop(0)
            else:
                return path

        return None

    def _check_hash(self, path: str, client_path: str, client_hash: List) -> bool:
        if not Util.is_serial_hashes(client_hash):
            Util.debug(
                f"bad client chunk hashes: {os.path.basename(path)} <-> {os.path.basename(client_path)}",
                fmt_indent=3,
                fmt_time=True,
            )
            Util.debug("--- client hashes", fmt_indent=12)
            self._show_chunk_hash(client_hash, fmt_indent=16)
            return False

        fstat = os.stat(path, follow_symlinks=False)
        fid, server_hash = self._file_details(
            path=path,
            size=fstat.st_size,
            mtime=fstat.st_mtime,
            max_serial=len(client_hash),
        )
        if not server_hash or len(server_hash) < len(client_hash):
            return False

        for i in range(len(client_hash)):
            if server_hash[i] != client_hash[i]:
                return False

        return True


def parse_args():
    parser = argparse.ArgumentParser(
        description="find & clean duplicate files to release disk space"
    )

    parser.add_argument(
        "--yaml",
        default="server.yaml",
        help="the yaml config of directory list from where to compare file & release space",
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
        server = Server(args.yaml, args.debug)
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()
