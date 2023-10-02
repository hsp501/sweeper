import os
import hashlib
from sweeper import hash_config


class file_item:
    def __init__(self, *, path: str, size: int) -> None:
        self._path = path
        self._size = size
        self._flag_redundant = False
        self._flag_delete = False
        self._hashes = {}
        self._file_handler = None
        self._file_position = 0
        self._same_item = None

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

    def same_as(self, item_protected):
        self._same_item = item_protected

    def get_hash(self, *, index: int, flag_update: bool):
        hex_hash = self._hashes.get(index)

        if hex_hash:
            return hex_hash
        elif not flag_update:
            return None
        else:
            blocks = self._size // hash_config.block_size()
            if self._size % hash_config.block_size() > 0:
                blocks += 1
            if index >= blocks:
                raise IndexError(
                    f"size: {self._size}, index: {index}, {self._path}")

            if not self._file_handler:
                self._file_handler = open(self._path, 'rb')

            if self._file_position != index * hash_config.block_size():
                self._file_handler.seek(index * hash_config.block_size())

            bytes = self._file_handler.read(hash_config.block_size())
            self._file_position = len(bytes)

            hash = hashlib.new(hash_config.algorithm())
            hash.update(bytes)
            hex_hash = hash.hexdigest()
            self._hashes[index] = hex_hash

            return hex_hash

    def close(self):
        try:
            if self._file_handler:
                self._file_handler.close()
                self._file_handler = None
        except Exception:
            pass

    def delete(self):
        if not self._flag_redundant:
            raise Exception(f"protected item can't be deleted: {self._path}")
        if not self._flag_delete:
            raise Exception(f"protected item can't be deleted: {self._path}")

        self.close()

        if os.path.isfile(self._path):
            os.remove(self._path)
            print(f"{'*' * 5}  remove {self._path}")
