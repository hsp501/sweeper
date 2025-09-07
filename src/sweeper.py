import json
import os
import socket
import struct
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import yaml

from src import ChunkHash, Command, HashDB, Key, ShrinkStat, Util


class Sweeper:
    def __init__(
        self,
        client_mode: bool,
        yaml_file: str,
        *,
        max_delete: int = 0,
        max_scan: int = 0,
    ) -> None:
        self._client_mode = client_mode
        self._socket: socket.socket = None
        self._stat = ShrinkStat(max_delete=max_delete, max_scan=max_scan)

        pwd = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(pwd, yaml_file), "r") as f:
            config = yaml.safe_load(f)

            self._dirs = []
            for dir in config["dirs"]:
                self._dirs.append(os.path.abspath(dir))

            self._id = (
                config["id"]
                if "id" in config
                else f"{'client' if self._client_mode else 'server'}-{Util.random_string()}"
            )

            key = "server" if self._client_mode else "bind"
            address = config[key].split(":")
            self._host = address[0]
            self._port = int(address[1]) if len(address) == 2 else 5555

            self._ch = ChunkHash()
            self._db = HashDB(os.path.join(pwd, config["hash_db"]))

    def stop(self) -> Any:
        try:
            if self._socket:
                self._socket.close()
        except Exception:
            pass

    def _send_json(
        self, _socket: socket.socket, data: Dict, *, debug: bool = False
    ) -> bool:
        try:
            raw = json.dumps(data).encode()
            _socket.sendall(struct.pack("!I", len(raw)) + raw)

            if debug:
                Util.debug(f"{self._id} --->>>:", fmt_indent=3, fmt_time=True)
                for k, v in data.items():
                    Util.debug(f"{k}: {v}", fmt_indent=12)

            return True
        except Exception as exp:
            Util.debug(f"_send_json() -> {str(exp)}", fmt_time=True)
            return False

    def _recv_json(self, _socket: socket.socket, *, debug: bool = False) -> Dict:
        raw_len = _socket.recv(4)
        if not raw_len:
            return None

        data = b""
        to_read = struct.unpack("!I", raw_len)[0]
        while len(data) < to_read:
            chunk = _socket.recv(to_read - len(data))
            if not chunk:
                return None
            data += chunk

        message = json.loads(data.decode())
        if debug:
            Util.debug(f"{self._id} <<<---:", fmt_indent=3, fmt_time=True)
            for k, v in message.items():
                Util.debug(f"{k}: {v}", fmt_indent=12)

        return message

    def _show_sweep_dirs(self):
        Util.debug("sweep directory list:", fmt_time=True)

        for i, top in enumerate(self._dirs):
            Util.debug(f"{(i + 1):02d}: {top}", fmt_indent=3)
        print("")

    def _debug_socket_data(self, data: Dict, send: bool):
        if Key.REQUEST_ID in data and Key.HASH in data:
            info = (
                "socket-"
                + ("send" if send else "recv")
                + " "
                + datetime.now().strftime("%H:%M:%S")
            )
            info += f": {data[Key.REQUEST_ID]}"
            for hash in data[Key.HASH]:
                info += f"\n{' ' * 3}{hash['serial']}-{hash['hash']}"
            print(info)

    def _show_chunk_hash(
        self, chunk_hashes: List, *, fmt_indent: int = 0, fmt_time: bool = False
    ):
        for hash in chunk_hashes:
            Util.debug(
                f"{hash['serial']:02d}: {hash['hash']} {hash['block_size']}",
                fmt_indent=fmt_indent,
                fmt_time=fmt_time,
            )

    def _file_details(
        self, *, path: str, size: int, mtime: float, max_serial: int
    ) -> Optional[Tuple[int, List]]:
        fid, chunk_hashes = self._db.get_file_details(path=path, size=size, mtime=mtime)

        if -1 == fid:
            fid = self._db.add_file(
                path=os.path.dirname(path),
                name=os.path.basename(path),
                size=size,
                mtime=mtime,
            )
            if -1 != fid:
                Util.debug(f"{os.path.basename(path)}", fmt_indent=3, fmt_time=True)

        if not Util.is_serial_hashes(chunk_hashes):
            chunk_hashes = None
            if -1 != fid:
                self._db.delete_chunk_hashes(fid)

        blocks = self._ch.blocks(size)
        if max_serial < 0 or max_serial > blocks:
            max_serial = blocks

        if not chunk_hashes or len(chunk_hashes) < max_serial:
            min_serial = (len(chunk_hashes) + 1) if chunk_hashes else 1
            for i in range(max_serial - min_serial + 1):
                serial = min_serial + i
                hash, block_size = self._ch.block_hash(path=path, serial=serial)
                self._stat.on_hash(block_size)
                if -1 != fid and self._db.add_chunk_hashes(
                    fid=fid, hashes=[(serial, block_size, hash)]
                ):
                    Util.debug(
                        f"{os.path.basename(path)}-[{serial:02d}]", fmt_indent=12
                    )

                chunk = {"serial": serial, "block_size": block_size, "hash": hash}
                if chunk_hashes is None:
                    chunk_hashes = []
                chunk_hashes.append(chunk)

        return fid, chunk_hashes

    def _req_size(self, *, request_id, local_mode: bool, path: str, size: int) -> Dict:
        return {
            Key.COMMAND: Command.CHECK_SIZE,
            Key.DEVICE_ID: self._id,
            Key.REQUEST_ID: request_id,
            Key.LOCAL_MODE: local_mode,
            Key.PATH: path,
            Key.SIZE: size,
        }

    def _echo_size(self, *, request_id, size: int, files: int) -> Dict:
        return {
            Key.COMMAND: Command.ECHO_CHECK_SIZE,
            Key.DEVICE_ID: self._id,
            Key.REQUEST_ID: request_id,
            Key.SIZE: size,
            Key.RESULT: files,
        }

    def _req_hash(
        self, *, request_id, local_mode: bool, path: str, size: int, chunk_hashes: List
    ) -> Dict:
        return {
            Key.COMMAND: Command.CHECK_HASH,
            Key.DEVICE_ID: self._id,
            Key.REQUEST_ID: request_id,
            Key.LOCAL_MODE: local_mode,
            Key.PATH: path,
            Key.SIZE: size,
            Key.HASH: chunk_hashes,
        }

    def _echo_hash(self, *, request_id, path: str) -> Dict:
        return {
            Key.COMMAND: Command.ECHO_CHECK_HASH,
            Key.DEVICE_ID: self._id,
            Key.REQUEST_ID: request_id,
            Key.RESULT: path,
        }
