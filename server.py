import argparse
import os
import socket
from typing import Dict, List

from src import Command, Key, Messanger, Role, Sweeper, Util


class Server(Sweeper):
    def __init__(self, yaml_file: str, *, debug_mode: bool):
        super().__init__(Role.SERVER, yaml_file, debug_mode=debug_mode)

    def start(self):
        self._stat.group_by_size(self._sweep_dirs)
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
        if self._messanger:
            self._messanger.close()
        self._messanger = Messanger(self._device_id, _socket, self._debug_mode)

        while True:
            request = self._messanger.recv_json()
            if not request:
                break

            if request[Key.COMMAND] == Command.CHECK_SIZE:
                self._handle_req_size(request)
            elif request[Key.COMMAND] == Command.CHECK_HASH:
                self._handle_req_chunk_hash(request)
            elif request[Key.COMMAND] == Command.CALC_FILE_HASH:
                self._handle_req_file_hash(request)
            else:
                self._messanger.close()
                self._messanger = None
                break

    def _handle_req_size(self, request: Dict):
        result = 0

        size = request[Key.SIZE]
        files = self._stat.size_group.get(size, None)
        if files:
            result = len(files)
            if request[Key.LOCAL_MODE] and request[Key.PATH] in files:
                result -= 1

        msg = self._msg_builder.echo_size(
            device_id=self._device_id,
            request_id=request[Key.REQUEST_ID],
            size=size,
            files=result,
        )
        self._messanger.send_json(msg)

    def _handle_req_chunk_hash(self, request: Dict):
        Util.debug(
            f"req-check hash: {request[Key.REQUEST_ID]}-{os.path.basename(request[Key.PATH])}[{len(request[Key.HASH]):02d}]",
            fmt_time=True,
        )
        path = self._filter_by_hash(
            request_id=request[Key.REQUEST_ID],
            local_mode=request[Key.LOCAL_MODE],
            client_path=request[Key.PATH],
            size=request[Key.SIZE],
            client_hash=request[Key.HASH],
        )

        msg = self._msg_builder.echo_hash(
            device_id=self._device_id,
            request_id=request[Key.REQUEST_ID],
            path=path,
        )
        self._messanger.send_json(msg)

    def _handle_req_file_hash(self, request: Dict):
        path = request[Key.PATH]
        size = request[Key.SIZE]
        request_id = request[Key.REQUEST_ID]

        Util.debug(
            f"req-calc hash: {request_id}-{os.path.basename(path)}[{size}]",
            fmt_time=True,
        )

        file_hash = None
        if self._device_id == request[Key.SERVER_ID] and Util.check_file_size(
            path, size
        ):
            file_hash = self._ch.file_hash(path)
            Util.debug(f"{file_hash}-{os.path.basename(path)}", fmt_indent=24)

        msg = self._msg_builder.echo_calc_file_hash(
            device_id=self._device_id, request_id=request_id, hash=file_hash
        )

        self._messanger.send_json(msg)

    def _filter_by_hash(
        self,
        *,
        request_id: str,
        local_mode: bool,
        client_path: str,
        size: int,
        client_hash: List,
    ) -> str:
        if self._debug_mode:
            self._show_session_files(request_id, True)

        if request_id not in self._session:
            self._session[request_id] = None
            if size in self._stat.size_group:
                self._session[request_id] = sorted(self._stat.size_group[size])

        found = None
        while self._session[request_id]:
            path = self._session[request_id][0]
            if local_mode and path == client_path:
                self._session[request_id].pop(0)
                if self._debug_mode:
                    Util.debug(f"pop session file[local]: {path}", fmt_indent=13)
                continue

            if not self._check_hash(request_id, path, client_path, client_hash):
                file_poped = self._session[request_id].pop(0)
                if self._debug_mode:
                    Util.debug(f"pop session file[hash]: {file_poped}", fmt_indent=13)
            else:
                found = path
                break

        if self._debug_mode:
            self._show_session_files(request_id, False)

        return found

    def _check_hash(
        self, request_id, path: str, client_path: str, client_hash: List
    ) -> bool:
        if not Util.is_serial_hashes(client_hash):
            Util.debug(
                f"bad client chunk hashes: {os.path.basename(path)} <-> {os.path.basename(client_path)}",
                fmt_time=True,
            )
            Util.debug("--- client hashes", fmt_indent=12)
            self._show_chunk_hash(client_hash, fmt_indent=16)
            return False

        fstat = Util.stat(path)
        if not fstat:
            return False

        fid, server_hash = self._file_details(
            path=path,
            size=fstat.st_size,
            mtime=fstat.st_mtime,
            request_id=request_id,
            ref_hashes=client_hash,
        )

        if server_hash and len(server_hash) > len(client_hash):
            server_hash = server_hash[: len(client_hash)]

        return server_hash and self._equal_chunk_hashes(server_hash, client_hash)

    def _show_session_files(self, request_id: str, flag_initial: bool):
        head = f"{'>>>' if flag_initial else '<<<'} session files [{request_id}]:"
        session_files = self._session.get(request_id, None)
        if not session_files:
            Util.debug(f"{head} None", fmt_time=True)
            return

        Util.debug(head, fmt_time=True)
        for i, path in enumerate(session_files):
            Util.debug(f"{i:02d}: {path}", fmt_indent=13)


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
        server = Server(args.yaml, debug_mode=args.debug)
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()
