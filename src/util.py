import os
import random
import stat
import string
from datetime import datetime
from typing import List


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
    def random_string(len: int = 5):
        if len < 0:
            len = 5

        chars = string.ascii_letters + string.digits

        return "".join(random.choice(chars) for _ in range(len))

    @staticmethod
    def debug(*args, **kwargs):
        head = ""
        if "fmt_indent" in kwargs and kwargs["fmt_indent"] > 0:
            head += f"{' ' * kwargs['fmt_indent']}"
        if "fmt_time" in kwargs and kwargs["fmt_time"]:
            head += datetime.now().strftime("%H:%M:%S") + " "

        content = kwargs.get("sep", "").join([str(item) for item in args])

        kwargs = {
            k: v for k, v in kwargs.items() if k not in {"fmt_indent", "fmt_time"}
        }

        print(f"{head}{content}", **kwargs)

    @staticmethod
    def is_serial_hashes(chunk_hashes: List, *, null_as_serial: bool = True) -> bool:
        if chunk_hashes:
            for i, hash in enumerate(chunk_hashes):
                if (
                    not isinstance(hash, dict)
                    or (i + 1) != hash.get("serial", -1)
                    or "block_size" not in hash
                    or "hash" not in hash
                ):
                    return False
            return True

        return null_as_serial

    @staticmethod
    def important_file(fstat: os.stat_result, path: str, name: str) -> bool:
        return (
            fstat.st_size > 0 and stat.S_ISREG(fstat.st_mode) and "@eaDir" not in path
        )
