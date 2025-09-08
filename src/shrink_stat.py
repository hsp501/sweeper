import hashlib
import os
import re
from typing import Dict, List

from src import Util


class ShrinkStat:
    def __init__(self, *, max_delete: int = 0, max_scan: int = 0) -> None:
        self._files_0bytes = []
        self._files_errors = []
        self._files_duplicate = {}

        self._max_delete, self._max_scan = max_delete, max_scan

        (
            self._deleted,
            self._scaned,
            self._shrink_bytes,
            self._hash_bytes,
            self._important_files,
        ) = [0] * 5

    def group_by_size(self, dirs: List):
        # key:      file size (int)
        # value:    file path list
        size_group: Dict[int, List[str]] = {}

        self._important_files = 0
        for top in dirs:
            for root, _, files in os.walk(top):
                for file in files:
                    path = os.path.join(root, file)
                    fstat = os.stat(path, follow_symlinks=False)
                    if Util.important_file(fstat, root, file):
                        if fstat.st_size not in size_group:
                            size_group[fstat.st_size] = []
                        size_group[fstat.st_size].append(path)
                        self._important_files += 1
                    elif 0 == fstat.st_size:
                        self.update_empty(path)

        self._size_group = size_group

    def update_empty(self, path: str):
        self._files_0bytes.append(path)

    def update_error(self, path: str):
        self._files_errors.append(path)

    @property
    def files_empty(self) -> List:
        return self._files_0bytes

    @property
    def files_error(self) -> List:
        return list(set(self._files_errors))

    @property
    def files_duplicate(self) -> Dict:
        return self._files_duplicate

    @property
    def size_group(self) -> Dict:
        return self._size_group

    def on_duplicate(
        self,
        *,
        server_id: str,
        server_path: str,
        chunk_hashes: List,
        client_path: str,
        free_space: int,
        local_mode: bool,
    ) -> bool:
        flag = False

        key = "-".join([hash["hash"] for hash in chunk_hashes])
        key = hashlib.md5(key.encode("utf-8")).hexdigest()
        if key not in self._files_duplicate:
            duplicates = []
            duplicates.append(f"{Util.bytes_readable(free_space)}-{free_space}")
            duplicates.append(f"original@{server_id}:{server_path}")
            self._files_duplicate[key] = duplicates
        duplicates = self._files_duplicate[key]

        if 2 == len(duplicates) or not local_mode:
            flag = True
            duplicates.append(client_path)
        elif local_mode:
            files = duplicates[1:]
            head = len(f"original@{server_id}:")
            files[0] = files[0][head:]
            if client_path not in files:
                flag = True
                duplicates.append(client_path)

        if flag:
            self._deleted += 1
            self._shrink_bytes += free_space

        return flag

    @property
    def deleted(self):
        return self._deleted

    @property
    def shrink_bytes(self):
        return self._shrink_bytes

    def on_scan(self, scaned: int = 1):
        self._scaned += scaned

    def reach_limit(self) -> bool:
        return (self._max_scan > 0 and self._scaned >= self._max_scan) or (
            self._max_delete > 0 and self._deleted >= self._max_delete
        )

    def on_hash(self, size: int):
        self._hash_bytes += size

    @property
    def hash_bytes(self):
        return self._hash_bytes

    def skip_scan(self, path: str) -> bool:
        for duplicate in self.files_duplicate.values():
            file = duplicate[1]
            file = re.sub(r"^original@\w+:", "", duplicate[1])

            if path == file:
                return True

        return False

    @property
    def files_to_scan(self) -> int:
        return self._important_files

    @property
    def scaned(self) -> int:
        return self._scaned
