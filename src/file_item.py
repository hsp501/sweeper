import hashlib
import os


class file_item:
    def __init__(self, *, path: str, size: int) -> None:
        self._path = path
        self._size = size
        self._flag_redundant = False
        self._flag_delete = False
        self._hashes = {}
        self._file_handler = None
        self._file_position = 0
        self._duplicates = []
        self._soul = None
        self._hashed_bytes = 0
        self._saved_space = 0
        self._deletion_serial = 0

    @property
    def path(self) -> str:
        return self._path

    @property
    def size(self) -> int:
        return self._size

    @property
    def flag_redundant(self) -> bool:
        return self._flag_redundant

    def mark_redundant(self):
        if self._flag_redundant:
            raise AttributeError(
                f"mark_redundant called more than once: {self._path}")

        self._flag_redundant = True

    @property
    def flag_delete(self) -> bool:
        return self._flag_delete

    def mark_delete(self):
        if not self._flag_redundant:
            raise AttributeError(
                f"protected item can't be marked deletable: {self._path}")

        self._flag_delete = True

    @property
    def hashed_bytes(self) -> int:
        return self._hashed_bytes

    @property
    def saved_space(self) -> int:
        return self._saved_space

    def update_duplicate(self, duplicate):
        assert (not self._flag_redundant)
        assert (isinstance(duplicate, file_item) and duplicate._flag_redundant)

        duplicate._soul = self
        self._duplicates.append(duplicate)

    @property
    def duplicates(self):
        return self._duplicates

    @property
    def soul(self):
        return self._soul

    def mark_deletion_serial(self, serial: int):
        self._deletion_serial = serial

    @property
    def deletion_serial(self):
        return self._deletion_serial

    @property
    def hash_times(self):
        return len(self._hashes)

    def get_hash(self, *, index: int, flag_update: bool, block_size: int, algorithm: str):
        cached = self._hashes.get(index)
        if cached:
            hash_hex, hash_size, hash_alg = cached
            if hash_size == block_size and hash_alg == algorithm:
                return hash_hex

        if not flag_update:
            return None
        else:
            if index >= self.blocks(block_size):
                raise IndexError(
                    f"size: {self._size}, index: {index}, block size: {block_size}, {self._path}")

            if not self._file_handler:
                self._file_handler = open(self._path, 'rb')

            if self._file_position != index * block_size:
                self._file_handler.seek(index * block_size)

            bytes = self._file_handler.read(block_size)
            readed = len(bytes)
            self._file_position += readed

            hash = hashlib.new(algorithm)
            hash.update(bytes)
            hash_hex = hash.hexdigest()
            self._hashed_bytes += readed

            self._hashes[index] = (hash_hex, block_size, algorithm)

            return hash_hex

    def close(self):
        try:
            if self._file_handler:
                self._file_handler.close()
                self._file_handler = None
        except Exception:
            pass

    def delete(self):
        if not self._flag_redundant or not self._flag_delete:
            raise Exception(f"protected item can't be deleted: {self._path}")

        self.close()

        if os.path.isfile(self._path):
            os.remove(self._path)
            self._saved_space = self._size

    def blocks(self, block_size: int) -> int:
        blocks = self._size // block_size
        if blocks * block_size < self._size:
            blocks += 1

        return blocks
