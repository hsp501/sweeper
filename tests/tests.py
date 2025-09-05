import os
import random
import shutil
import unittest

from src import cleanup, hashConfig


class test_sweeper(unittest.TestCase):
    def make_block_bytes(self, size: int):
        hex_chars = []
        for _ in range(0, 1024 // 16):
            for i in range(0, 10):
                hex_chars.extend(chr(ord("0") + i))
            for i in range(0, 6):
                hex_chars.extend(chr(ord("a") + i))
        random.shuffle(hex_chars)

        block_bytes = bytearray()
        for _ in range(0, (2 * size) // len(hex_chars)):
            start = random.randint(0, len(hex_chars) // 2)
            block_bytes.extend(bytearray.fromhex("".join(hex_chars[(2 * start) :])))
            block_bytes.extend(bytearray.fromhex("".join(hex_chars[0 : (2 * start)])))

        tail = size - len(block_bytes)
        if tail > 0:
            random.shuffle(hex_chars)
            block_bytes.extend(bytearray.fromhex("".join(hex_chars[0 : (2 * tail)])))

        self.assertEqual(len(block_bytes), size)

        return block_bytes

    def make_sub_dirs(self, max_levels: int, *, prefix: str = "sub_"):
        sub_dirs = range(0, random.randint(0, max_levels))
        sub_dirs = map(lambda level: f"{prefix}{level + 1}", sub_dirs)
        return os.path.sep.join(sub_dirs)

    def resolve_path(self):
        dir_base = os.path.expanduser("~")
        dir_base = os.path.join(dir_base, "x.data", "stf_095h3")
        dir_repo = os.path.join(dir_base, "repo")
        dir_redu = os.path.join(dir_base, "redu")

        return dir_base, dir_repo, dir_redu

    def build_file_name(
        self, serial: int, prefix: str, blocks: int, tail: int, size: int
    ) -> str:
        h = "H" if blocks > 0 else "0"
        t = "T" if tail > 0 else "0"
        parts = [prefix, h + t, str(blocks), str(tail), str(size)]

        return f"{serial}.{'_'.join(parts)}"

    def generate_file_data(self, fewer: bool):
        if fewer:
            max_files = 4
            max_blocks = 4
        else:
            max_files = 20
            max_blocks = 16 * 1024 * 1024 // hashConfig.block_size()

        dupl_files = []
        dist_files = []

        # generate data of duplicate & distinct files
        for k in range(0, 2):
            prefix = "dupl" if 0 == k else "dist"
            dict_files = dupl_files if 0 == k else dist_files

            for i in range(0, max_files):
                tail = random.randint(1, hashConfig.block_size() - 1)
                name = self.build_file_name(i + 1, prefix, 0, tail, tail)
                dict_files.append((name, self.make_block_bytes(tail)))

            for i in range(0, max_files):
                head = random.randint(1, max_blocks)
                tail = random.randint(1, hashConfig.block_size() - 1)
                size = head * hashConfig.block_size() + tail
                name = self.build_file_name(i + 1, prefix, head, tail, size)
                dict_files.append((name, self.make_block_bytes(size)))

            for i in range(0, max_files):
                head = random.randint(1, max_blocks)
                size = head * hashConfig.block_size()
                name = self.build_file_name(i + 1, prefix, head, 0, size)
                dict_files.append((name, self.make_block_bytes(size)))

        return dupl_files, dist_files

    def make_files_v1(self, fewer: bool):
        dir_base, dir_repo, dir_redu = self.resolve_path()
        if os.path.exists(dir_base):
            shutil.rmtree(dir_base)

        dupl_files, dist_files = self.generate_file_data(fewer)

        files_created = []

        # write duplicate files
        for name, block_bytes in dupl_files:
            for i in range(0, 2):
                flag_repo = 0 == i

                dir_dest = dir_repo if flag_repo else dir_redu
                dir_dest = os.path.join(dir_dest, self.make_sub_dirs(3))
                if not os.path.exists(dir_dest):
                    os.makedirs(dir_dest)

                file_path = os.path.join(dir_dest, name)
                with open(file_path, "wb") as file:
                    file.write(block_bytes)
                    files_created.append((file_path, flag_repo))

                if not flag_repo:
                    for j in range(0, random.randint(0, 4)):
                        dir_clone = dir_redu if 0 == random.randint(0, 1) else dir_dest
                        file_path = os.path.join(dir_clone, f"{name}.clone.{j + 1}")

                        with open(file_path, "wb") as file:
                            file.write(block_bytes)
                            files_created.append((file_path, False))

                        if 1 == random.randint(1, 3):
                            file_path = os.path.join(dir_clone, f"{name}.fake")
                            fake_block_bytes = self.make_block_bytes(len(block_bytes))
                            with open(file_path, "wb") as file:
                                file.write(fake_block_bytes)
                                files_created.append((file_path, True))

        # write distinct files
        for name, block_bytes in dist_files:
            dir_dest = dir_repo if 0 == random.randint(0, 1) else dir_redu
            dir_dest = os.path.join(dir_dest, self.make_sub_dirs(3))
            if not os.path.exists(dir_dest):
                os.makedirs(dir_dest)

            file_path = os.path.join(dir_dest, name)
            with open(file_path, "wb") as file:
                file.write(block_bytes)
                files_created.append((file_path, True))

        return files_created

    def make_files_v2(self):
        dir_base, dir_repo, dir_redu = self.resolve_path()
        if os.path.exists(dir_base):
            shutil.rmtree(dir_base)

        files_created = []

        dupl_files, dist_files = self.generate_file_data(True)

        name, block_bytes = dupl_files[0]
        for i in range(0, 2):
            flag_repo = 0 == i

            dir_dest = dir_repo if flag_repo else dir_redu
            os.makedirs(os.path.join(dir_dest, "sub-1"))

            file_paths = []
            file_paths.append(os.path.join(dir_dest, f"{name}.1"))
            file_paths.append(os.path.join(dir_dest, f"{name}.2"))
            file_paths.append(os.path.join(dir_dest, "sub-1", f"{name}.3"))
            for file_path in file_paths:
                with open(file_path, "wb") as file:
                    file.write(block_bytes)
                    files_created.append((file_path, flag_repo))

        for name, block_bytes in dist_files:
            dir_dest = dir_repo if 0 == random.randint(0, 1) else dir_redu
            dir_dest = os.path.join(dir_dest, self.make_sub_dirs(3))
            if not os.path.exists(dir_dest):
                os.makedirs(dir_dest)

            file_path = os.path.join(dir_dest, name)
            with open(file_path, "wb") as file:
                file.write(block_bytes)
                files_created.append((file_path, True))

        return files_created

    def test_clean(self):
        # files = self.make_files_v1(True)
        files = self.make_files_v2()

        if True:
            dir_base, dir_repo, dir_redu = self.resolve_path()

            cleaner = cleanup(0, False)
            cleaner.watch(dir=dir_repo, redundant=False)
            cleaner.watch(dir=dir_redu, redundant=True)
            cleaner.shrink()

            for file, flag_exist in files:
                self.assertEqual(os.path.exists(file), flag_exist)


if "__main__" == __name__:
    unittest.main()
