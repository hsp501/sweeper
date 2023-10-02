import os
from functools import cmp_to_key

from sweeper import file_item, hash_config


class sweeper:
    def __init__(self) -> None:
        self._flag_clean_started = False
        self._directories = {}
        self._file_size_dict = {}

    def watch(self, *, dir: str, redundant: bool):
        if self._flag_clean_started:
            raise Exception(
                "can't watch directory after clean operation started")

        if self._directories.get(dir):
            raise Exception(f"{dir} has been watched")
        for watched in self._directories:
            if dir.startswith(watched):
                raise Exception(
                    f"The parent of {dir} has been watched: {watched}")
        self._directories[dir] = redundant

    def clean(self):
        self._flag_clean_started = True

        self._group_by_size()
        for size in sorted(self._file_size_dict.keys(), reverse=True):
            self._clean_files(size, self._file_size_dict[size])

    def _group_by_size(self):
        self._file_size_dict.clear()

        for dir, redundant in self._directories.items():
            for root, _, files in os.walk(dir):
                for file in files:
                    path = os.path.join(root, file)
                    size = os.path.getsize(path)
                    item = file_item(path=path, size=size)
                    if redundant:
                        item.mark_redundant()
                    self._file_size_dict.setdefault(size, []).append(item)

    def _clean_files(self, size: int, file_items: list):
        protected_items = list(
            filter(lambda item: not item.flag_redundant, file_items))

        blocks = size // hash_config.block_size()
        if size % hash_config.block_size() > 0:
            blocks += 1

        for redudant in filter(lambda item:  item.flag_redundant, file_items):
            excluded = set()
            flag_over = False

            for index in range(0, blocks):
                if flag_over:
                    break

                for protected in protected_items:
                    if protected not in excluded:
                        if redudant.get_hash(index=index, flag_update=True) == protected.get_hash(index=index, flag_update=True):
                            if index == (blocks - 1):
                                redudant.same_as(protected)
                                redudant.mark_delete()
                                redudant.delete()
                                flag_over = True
                                break
                        else:
                            excluded.add(protected)

        for item in file_items:
            item.close()
