import json
import os
import socket
import struct
from enum import IntEnum
from typing import Any, Dict, List, Optional, Tuple

import yaml

from src import ChunkHash, Command, HashDB, Key, ShrinkStat, Util


class Role(IntEnum):
    SERVER = 0
    SCANNER = 1
    SHRINKER = 2


class MessageBuilder:
    def req_size(
        self, *, device_id: str, request_id, local_mode: bool, path: str, size: int
    ) -> Dict:
        return {
            Key.COMMAND: Command.CHECK_SIZE,
            Key.DEVICE_ID: device_id,
            Key.REQUEST_ID: request_id,
            Key.LOCAL_MODE: local_mode,
            Key.PATH: path,
            Key.SIZE: size,
        }

    def echo_size(self, *, device_id: str, request_id, size: int, files: int) -> Dict:
        return {
            Key.COMMAND: Command.ECHO_CHECK_SIZE,
            Key.DEVICE_ID: device_id,
            Key.REQUEST_ID: request_id,
            Key.SIZE: size,
            Key.RESULT: files,
        }

    def req_hash(
        self,
        *,
        device_id: str,
        request_id,
        local_mode: bool,
        path: str,
        size: int,
        chunk_hashes: List,
    ) -> Dict:
        return {
            Key.COMMAND: Command.CHECK_HASH,
            Key.DEVICE_ID: device_id,
            Key.REQUEST_ID: request_id,
            Key.LOCAL_MODE: local_mode,
            Key.PATH: path,
            Key.SIZE: size,
            Key.HASH: chunk_hashes,
        }

    def echo_hash(self, *, device_id: str, request_id, path: str) -> Dict:
        return {
            Key.COMMAND: Command.ECHO_CHECK_HASH,
            Key.DEVICE_ID: device_id,
            Key.REQUEST_ID: request_id,
            Key.RESULT: path,
        }

    def req_calc_file_hash(
        self, *, device_id: str, request_id, server_id: str, path: str, size: int
    ) -> Dict:
        return {
            Key.COMMAND: Command.CALC_FILE_HASH,
            Key.DEVICE_ID: device_id,
            Key.REQUEST_ID: request_id,
            Key.SERVER_ID: server_id,
            Key.PATH: path,
            Key.SIZE: size,
        }

    def echo_calc_file_hash(self, *, device_id: str, request_id, hash: str) -> Dict:
        return {
            Key.COMMAND: Command.ECHO_CALC_FILE_HASH,
            Key.DEVICE_ID: device_id,
            Key.REQUEST_ID: request_id,
            Key.RESULT: hash,
        }


class Messanger:
    def __init__(self, device_id: str, socket: socket.socket, debug_mode: bool):
        self._device_id = device_id
        self._socket = socket
        self._debug_mode = debug_mode

    def send_json(self, message: Dict) -> bool:
        try:
            raw = json.dumps(message).encode()
            self._socket.sendall(struct.pack("!I", len(raw)) + raw)

            if self._debug_mode:
                self._debug_socket_data(message, True, fmt_indent=3, fmt_time=True)

            return True
        except Exception as exp:
            Util.debug(f"send_json() -> {str(exp)}", fmt_time=True)
            return False

    def recv_json(self) -> Dict:
        raw_len = self._socket.recv(4)
        if not raw_len:
            return None

        data = b""
        to_read = struct.unpack("!I", raw_len)[0]
        while len(data) < to_read:
            chunk = self._socket.recv(to_read - len(data))
            if not chunk:
                return None
            data += chunk

        message = json.loads(data.decode())
        if self._debug_mode:
            self._debug_socket_data(message, False, fmt_indent=3, fmt_time=True)

        return message

    def _debug_socket_data(
        self, data: Dict, send: bool, *, fmt_indent: int, fmt_time: bool
    ):
        time_space = 9 if fmt_time else 0

        Util.debug(
            f"{self._device_id} {'--->>>' if send else '<<<---'}:",
            fmt_indent=fmt_indent,
            fmt_time=fmt_time,
        )
        for k, v in data.items():
            if k == Key.HASH:
                Util.debug(f"{k}:", fmt_indent=fmt_indent + time_space)
                for hash in v:
                    hash = str(hash)[1:-1].replace("'", "")
                    Util.debug(hash, fmt_indent=fmt_indent + time_space + 3)
            else:
                Util.debug(f"{k}: {v}", fmt_indent=fmt_indent + time_space)

    def close(self) -> Any:
        try:
            if self._socket:
                self._socket.close()
        except Exception:
            pass


class Storage:
    def __init__(
        self,
        role: Role,
        yaml_file: str,
        *,
        limit: Tuple = None,
        debug_mode: bool = False,
    ):
        self._role: Role = role
        self._debug_mode = debug_mode

        self._messanger: Messanger = None
        self._msg_builder = MessageBuilder()

        limit_delete, limit_scan = limit if limit else (0, 0)
        self._stat = ShrinkStat(limit_delete=limit_delete, limit_scan=limit_scan)

        self._yaml_file = yaml_file
        with open(yaml_file, "r") as f:
            config = yaml.safe_load(f)
            self._parse_yaml(config)

    def start(self):
        pass

    def _parse_yaml(self, config: Dict):
        self._config = config

        self._sweep_dirs = []
        for dir in config["sweep_dirs"]:
            self._sweep_dirs.append(dir)

        self._device_id = (
            config["id"]
            if "id" in config
            else f"{self._role.name}-{Util.random_string()}"
        )

        key = "bind" if self._role == Role.SERVER else "server"
        address = config[key].split(":")
        self._host = address[0]
        self._port = int(address[1]) if len(address) == 2 else 5555

        self._ch = ChunkHash()

    def _show_sweep_dirs(self):
        Util.debug("sweep directory list:", fmt_time=True)

        for i, top in enumerate(self._sweep_dirs):
            Util.debug(f"{(i + 1):02d}: {top}", fmt_indent=3)
        print("")


class Sweeper(Storage):
    def __init__(
        self,
        role: Role,
        yaml_file: str,
        *,
        limit: Tuple = None,
        debug_mode: bool = False,
    ) -> None:
        super().__init__(role, yaml_file, limit=limit, debug_mode=debug_mode)

    def _parse_yaml(self, config: Dict):
        super()._parse_yaml(config)

        pwd = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self._db = HashDB(os.path.join(pwd, config["hash_db"]))

    def stop(self) -> Any:
        if self._messanger:
            self._messanger.close()

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
        self, *, path: str, size: int, mtime: float, request_id, ref_hashes: List = None
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
                Util.debug(
                    f"{os.path.basename(path)}-{request_id}",
                    fmt_indent=3,
                    fmt_time=True,
                )

        if not Util.is_serial_hashes(chunk_hashes):
            chunk_hashes = None
            if -1 != fid:
                self._db.delete_chunk_hashes(fid)

        blocks = self._ch.blocks(size)
        max_serial = 1 if not ref_hashes else min(len(ref_hashes), blocks)

        if chunk_hashes and ref_hashes:
            serial = min(len(chunk_hashes), len(ref_hashes))
            if not self._equal_chunk_hashes(chunk_hashes[:serial], ref_hashes[:serial]):
                return fid, chunk_hashes

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

                if ref_hashes and chunk != ref_hashes[serial - 1]:
                    # differences found, stop further MD5 hash
                    break

        return fid, chunk_hashes

    def _equal_chunk_hashes(self, hash1: List, hash2: List) -> bool:
        if hash1 and hash2:
            if len(hash1) != len(hash2):
                return False

            for i in range(len(hash1)):
                if hash1[i] != hash2[i]:
                    return False

            return True

        return hash1 == hash2
