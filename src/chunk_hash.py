import hashlib
import traceback
from typing import Optional, Tuple

HEAD_SIZE = 128 * 1024
BLOCK_SIZE = 64 * 1024 * 1024
READ_SIZE = 256 * 1024


class ChunkHash:
    def blocks(self, size: int) -> int:
        assert isinstance(size, int) and size > 0

        _blocks = 1
        if size > HEAD_SIZE:
            tail_size = (size - HEAD_SIZE) % BLOCK_SIZE
            if tail_size > 0:
                _blocks += 1

            _blocks += (size - HEAD_SIZE - tail_size) // BLOCK_SIZE

        return _blocks

    def block_size(self, size: int, serial: int) -> int:
        assert (
            isinstance(size, int)
            and isinstance(serial, int)
            and size > 0
            and serial >= 1
        )

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

        try:
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
        except Exception:
            traceback.print_exc()
            return None, 0

    def file_hash(self, path, chunk_size=READ_SIZE) -> Optional[str]:
        try:
            md5 = hashlib.md5()

            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(chunk_size), b""):
                    md5.update(chunk)

            return md5.hexdigest()
        except Exception:
            traceback.print_exc()
            return None
