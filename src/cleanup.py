import os
import sys
from datetime import datetime
from functools import cmp_to_key

from src import file_item, hash_config


class util:
    @staticmethod
    def convert_bytes(byte_size):
        suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']

        index = 0
        while byte_size >= 1024 and index < len(suffixes) - 1:
            byte_size /= 1024.0
            index += 1

        formatted_size = "{:.2f} {}".format(byte_size, suffixes[index])

        return formatted_size


class operation:
    def __init__(self) -> None:
        self.reset(False)

    def reset(self, operation_started: bool):
        self._soul_items = []
        self._deleted_items = []
        self._flag_operation_started = operation_started

    def log_soul(self, soul_item: file_item) -> int:
        self._soul_items.append(soul_item)

        return len(self._soul_items)

    def log_deleted(self, deletion_item: file_item) -> int:
        self._deleted_items.append(deletion_item)

        return len(self._deleted_items)

    @property
    def soul_serial(self) -> int:
        return len(self._soul_items)

    @property
    def deletion_serial(self) -> int:
        return len(self._deleted_items)

    @property
    def flag_started(self) -> bool:
        return self._flag_operation_started


class cleanup:
    def __init__(self, max_deletion: int, dry_run: bool) -> None:
        self._max_deletion = max_deletion
        self._dry_run = dry_run

        self._directories = {}
        self._file_size_dict = {}

        self._operation = operation()

    def watch(self, *, dir: str, redundant: bool):
        if self._operation.flag_started:
            raise Exception(
                "can't watch directory after clean operation started")

        if self._directories.get(dir):
            raise Exception(f"{dir} has been watched")
        for watched in self._directories:
            if dir.startswith(watched):
                raise Exception(
                    f"The parent of {dir} has been watched: {watched}")
            if watched.startswith(dir):
                raise Exception(
                    f"The child of {dir} has been watched: {watched}")

        self._directories[dir] = redundant

    def shrink(self):
        start = datetime.now()
        print(
            f"{start.strftime('%Y-%m-%d %H:%M:%S')} begin to free disk space\n")

        self._operation.reset(True)

        self._group_by_size()
        for size in sorted(self._file_size_dict.keys(), reverse=True):
            if self._task_over():
                break

            self._execute(size, self._file_size_dict[size])

        end = datetime.now()
        print(
            f"{start.strftime('%Y-%m-%d %H:%M:%S')} free disk space ended: {str(end - start)}\n")

        hashed_bytes = space_freed = 0
        for _, file_items in self._file_size_dict.items():
            for item in file_items:
                hashed_bytes += item.hashed_bytes
                if item.flag_redundant:
                    space_freed += item.saved_space

        print(
            f"space freed: {util.convert_bytes(space_freed)} ({space_freed}), hash bytes: {util.convert_bytes(hashed_bytes)} ({hashed_bytes})")

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

    def _execute(self, size: int, file_items: list):
        algorithm = hash_config.algorithm()
        block_size = hash_config.block_size()

        protected_items = list(
            filter(lambda item: not item.flag_redundant, file_items))

        redudant_items = list(
            filter(lambda item:  item.flag_redundant, file_items))

        for redudant in redudant_items:
            assert (size == redudant.size)

            for protected in protected_items:
                assert (size == protected.size)

                if self._compare(redudant, protected, block_size, algorithm):
                    serial = self._operation.log_deleted(redudant)
                    redudant.mark_deletion_serial(serial)
                    protected.update_duplicate(redudant)

                    if not self._dry_run:
                        redudant.mark_delete()
                        redudant.delete()

            if self._task_over():
                break

        for item in file_items:
            item.close()

        self._log(protected_items, redudant_items)
        sys.stdout.flush()

    def _log(self, protected_items: list, redudant_items: list):
        now = datetime.now().strftime('%H:%M')

        for protected in protected_items:
            assert (not protected.flag_redundant)

            if len(protected.duplicates) > 0:
                serial = self._operation.log_soul(protected)
                head = f"{serial}: {now}".ljust(12)
                logs = [f"{head}{protected.path}"]
                for duplicate in protected.duplicates:
                    logs.append(
                        f"{' ' * 2}- {str(duplicate.deletion_serial).ljust(8)}{duplicate.path}")

                print('\n'.join(logs) + '\n')

        logs = None
        head = f"{' ' * 2}+".ljust(12)
        for redudant in redudant_items:
            assert (redudant.flag_redundant)

            if not redudant.soul:
                if not logs:
                    logs = [f"++: {now}"]
                logs.append(f"{head}{redudant.path}")

        if logs:
            print('\n'.join(logs) + '\n')

    def _task_over(self) -> bool:
        return self._max_deletion > 0 and self._operation.deletion_serial >= self._max_deletion

    def _compare(self, one: file_item, other: file_item, block_size: int, algorithm: str) -> bool:
        if one.size == other.size:
            for index in range(0, one.blocks(block_size)):
                hash_one = one.get_hash(
                    index=index, flag_update=True, block_size=block_size, algorithm=algorithm)
                hash_other = other.get_hash(
                    index=index, flag_update=True, block_size=block_size, algorithm=algorithm)

                if hash_one != hash_other:
                    return False

            return True

        return False
