import hashlib
import os
import random
import re
import shutil
import string
import unittest
from typing import List

from src import BLOCK_SIZE, HEAD_SIZE


class TestFakeFiles(unittest.TestCase):
    def setUp(self):
        self._dir_fake = "/home/hsp501/x.data/code/python/sweeper/fake_files/auto"
        if os.path.exists(self._dir_fake):
            shutil.rmtree(self._dir_fake)
        os.makedirs(self._dir_fake)

        self._blocks = 9
        self._build_blocks(self._blocks)

    def tearDown(self):
        pass

    def _block_hash(self, block_bytes: bytes) -> str:
        md5 = hashlib.md5()
        md5.update(block_bytes)

        return md5.hexdigest()

    def _build_block_group(self, group_len: int, size):
        hashes = set()
        block_group = []
        for _ in range(group_len):
            if isinstance(size, int):
                block_bytes = os.urandom(size)
            else:
                block_bytes = os.urandom(random.randint(size[0], size[1]))
            block_hash = self._block_hash(block_bytes)
            if block_hash in hashes:
                continue

            hashes.add(block_hash)
            block_group.append(block_bytes)

        return block_group

    def _build_blocks(self, blocks: int):
        self._blocks_head = self._build_block_group(blocks, HEAD_SIZE)
        self._blocks_head_orphan = self._build_block_group(blocks, (1, HEAD_SIZE - 1))
        self._blocks_body = self._build_block_group(blocks, BLOCK_SIZE)
        self._blocks_tail = self._build_block_group(blocks, (1, BLOCK_SIZE - 1))

    def _name2blocks(self, name: str) -> List:
        # orphan1
        match = re.match(r"^orphan(\d)$", name)
        if match:
            serial = int(match.group(1))
            return [self._blocks_head_orphan[serial - 1]]

        # h1
        match = re.match(r"^h(\d)$", name)
        if match:
            serial = int(match.group(1))
            return [self._blocks_head[serial - 1]]

        blocks = []

        # h1-t4
        match = re.match(r"^h(\d)_t(\d)$", name)
        if match:
            serial_head = int(match.group(1))
            blocks.append(self._blocks_head[serial_head - 1])

            serial_tail = int(match.group(2))
            blocks.append(self._blocks_tail[serial_tail - 1])

            return blocks

        # h1-b23
        match = re.match(r"^h(\d)_b(\d+)$", name)
        if match:
            serial_head = int(match.group(1))
            blocks.append(self._blocks_head[serial_head - 1])

            for serial_body in [int(i) for i in list(match.group(2))]:
                blocks.append(self._blocks_body[serial_body - 1])

            return blocks

        # h1-b23-t2
        match = re.match(r"^h(\d)_b(\d+)_t(\d)$", name)
        if match:
            serial_head = int(match.group(1))
            blocks.append(self._blocks_head[serial_head - 1])

            for serial_body in [int(i) for i in list(match.group(2))]:
                blocks.append(self._blocks_body[serial_body - 1])

            serial_tail = int(match.group(3))
            blocks.append(self._blocks_tail[serial_tail - 1])

            return blocks

        print(f"invalid name format: {name}")
        self.assertTrue(False)

    def _create_file(self, path: str, blocks: List):
        with open(path, "wb") as f:
            for _bytes in blocks:
                f.write(_bytes)

    def _create_same_files(self):
        # same files
        names = []

        # sub1/orpahn1 sub2/orphan1
        names.append(f"orphan{random.randint(1, self._blocks)}")
        # sub1/h1 sub2/h1
        names.append(f"h{random.randint(1, self._blocks)}")

        # sub1/h1-t1 sub2/h1-t1
        names.append(
            f"h{random.randint(1, self._blocks)}_t{random.randint(1, self._blocks)}"
        )

        # sub1/h1-b21 sub2/h1-b21
        for _ in range(3):
            body_serials = "".join(
                random.choices(string.digits, k=random.randint(1, 3))
            )
            head_serial = random.randint(1, self._blocks)
            name = f"h{head_serial}_b{body_serials}"
            if name not in names:
                names.append(name)

        # sub1/h1-b23-t2 sub2/h1-b23-t2
        for _ in range(3):
            body_serials = "".join(
                random.choices(string.digits, k=random.randint(1, 3))
            )
            head_serial = random.randint(1, self._blocks)
            tail_serial = random.randint(1, self._blocks)
            name = f"h{head_serial}_b{body_serials}_t{tail_serial}"
            if name not in names:
                names.append(name)

        for name in names:
            blocks = self._name2blocks(name)
            self._create_file(
                os.path.join(self._dir_fake, "sub1", f"sam_{name}-sub1"), blocks
            )
            self._create_file(
                os.path.join(self._dir_fake, "sub2", f"sam_{name}-sub2"), blocks
            )
            if 1 == random.randint(0, 2):
                self._create_file(
                    os.path.join(self._dir_fake, "sub3", f"sam_{name}-sub3"), blocks
                )

    # h1-b23-t5 -> ['1', '2', '3', '5'] or '1235' -> serial
    def _serials2name(self, serials, *, with_tail: bool) -> str:
        if isinstance(serials, str):
            serials = [char for char in serials]

        if 1 == len(serials):
            name = f"orphan{serials[0]}"
        elif 2 == len(serials):
            name = f"h{serials[0]}_{'t' if with_tail else 'b'}{serials[1]}"
        else:
            name = f"h{serials[0]}"
            name += f"_b{''.join(serials[1 : (-1 if with_tail else len(serials))])}"
            if with_tail:
                name += f"_t{serials[-1]}"

        return name

    def _create_similar_files(self):
        names = []
        serials = random.choices(string.digits, k=4)
        names.append(self._serials2name(serials, with_tail=True))
        for i in range(len(serials) - 1):
            serial = int(serials[i + 1])
            while True:
                variant = (serial + random.randint(1, 100)) % self._blocks + 1
                if serial != variant:
                    break
            variant = (
                "".join(serials[0 : (i + 1)]) + str(variant) + "".join(serials[i + 2 :])
            )
            names.append(self._serials2name(variant, with_tail=True))

        for name in names:
            blocks = self._name2blocks(name)
            self._create_file(
                os.path.join(
                    self._dir_fake, f"sub{random.randint(1, 3)}", f"sim_{name}"
                ),
                blocks,
            )

    def _create_different_files(self):
        for _ in range(10):
            serials = random.choices(string.digits, k=random.randint(1, 7))
            name = self._serials2name(serials, with_tail=random.randint(0, 2) == 1)
            blocks = self._name2blocks(name)
            self._create_file(
                os.path.join(
                    self._dir_fake, f"sub{random.randint(1, 3)}", f"dif_{name}"
                ),
                blocks,
            )

    def test_fake_files(self):
        for i in range(3):
            sub = os.path.join(self._dir_fake, f"sub{i + 1}")
            os.makedirs(sub)

        self._create_same_files()
        self._create_similar_files()
        self._create_different_files()


if __name__ == "__main__":
    unittest.main()
