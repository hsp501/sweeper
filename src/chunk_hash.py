import hashlib
import os
from typing import Tuple

HEAD_SIZE = 128 * 1024
BLOCK_SIZE = 64 * 1024 * 1024
READ_SIZE = 256 * 1024


class ChunkHash:
    def blocks(self, path: str) -> int:
        fs = os.stat(path)

        _blocks = 1
        if fs.st_size > HEAD_SIZE:
            tail_size = (fs.st_size - HEAD_SIZE) % BLOCK_SIZE
            if tail_size > 0:
                _blocks += 1

            _blocks += (fs.st_size - HEAD_SIZE - tail_size) // BLOCK_SIZE

        return _blocks

    def block_size(self, *, path: str, serial: int) -> int:
        size = os.stat(path).st_size

        if 1 == serial:
            blk_size = HEAD_SIZE if size >= HEAD_SIZE else size
        else:
            blk_size = size - HEAD_SIZE - (serial - 2) * BLOCK_SIZE
            if blk_size < 0:
                blk_size = 0
            elif blk_size >= BLOCK_SIZE:
                blk_size = BLOCK_SIZE

        return blk_size

    def block_hash(self, *, path: str, serial: int) -> Tuple:
        if 1 == serial:
            start = 0
            blk_size = HEAD_SIZE
        else:
            start = HEAD_SIZE + (serial - 2) * BLOCK_SIZE
            blk_size = BLOCK_SIZE

        readed = 0
        md5 = hashlib.md5()
        with open(path, "rb") as f:
            f.seek(start)

            while blk_size > readed:
                bytes_read = f.read(min(READ_SIZE, blk_size - readed))
                if not bytes_read:
                    break

                md5.update(bytes_read)
                readed += len(bytes_read)

        return md5.hexdigest(), readed
