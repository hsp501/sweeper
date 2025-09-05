import json
import struct


class Util:
    @staticmethod
    def bytes_readable(num_bytes: int) -> str:
        size = float(num_bytes)
        units = ["B", "KB", "MB", "GB", "TB", "PB"]
        for unit in units:
            if size < 1024:
                return f"{size:.2f} {unit}"
            size /= 1024

        return f"{size:.2f} PB"

    @staticmethod
    def send_json(conn, data):
        raw = json.dumps(data).encode()
        conn.sendall(struct.pack("!I", len(raw)) + raw)

    @staticmethod
    def recv_json(conn):
        raw_len = conn.recv(4)
        if not raw_len:
            return None
        msg_len = struct.unpack("!I", raw_len)[0]
        data = b""
        while len(data) < msg_len:
            chunk = conn.recv(msg_len - len(data))
            if not chunk:
                return None
            data += chunk

        return json.loads(data.decode())
