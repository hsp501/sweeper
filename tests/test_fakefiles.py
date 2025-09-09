import os
import random
import shutil
import unittest
from typing import List

from src import BLOCK_SIZE, HEAD_SIZE, Util


class TestFakeFiles(unittest.TestCase):
    def setUp(self):
        self._dir_fake = os.path.dirname(os.path.abspath(__file__))
        self._dir_fake = os.path.join(
            os.path.dirname(self._dir_fake), "fake_files", "auto"
        )
        if os.path.exists(self._dir_fake):
            shutil.rmtree(self._dir_fake)
        os.makedirs(self._dir_fake, exist_ok=True)

        self._blocks = 10
        self._heads = []
        self._bodys = []
        self._tails = []

        for i in range(self._blocks):
            self._heads.append(os.urandom(HEAD_SIZE))
            self._bodys.append(os.urandom(BLOCK_SIZE))

            if 0 == (i % 2):
                size = random.randint(1, BLOCK_SIZE - 1)
            self._tails.append(os.urandom(size))

    def tearDown(self):
        pass

    def _create_file(self, path: str, bytes_blocks: List):
        with open(path, "wb") as f:
            for block in bytes_blocks:
                f.write(block)

    def _concat(self, *, heads: List, bodys: List, tails: List):
        name = ""
        bytes_blocks = []

        for i, idx in enumerate(heads):
            if 0 == i:
                name += f"h{idx}"
            else:
                name += f"{idx}"
            bytes_blocks.append(self._heads[idx])

        for i, idx in enumerate(bodys):
            if 0 == i:
                name += f"-b{idx}"
            else:
                name += f"{idx}"
            bytes_blocks.append(self._bodys[idx])

        for i, idx in enumerate(tails):
            if 0 == i:
                name += f"-t{idx}"
            else:
                name += f"{idx}"
            bytes_blocks.append(self._tails[idx])

        return name, bytes_blocks

    def test_fake_files(self):
        sub1 = os.path.join(self._dir_fake, f"sub1-{Util.random_string()}")
        sub2 = os.path.join(self._dir_fake, f"sub2-{Util.random_string()}")
        os.makedirs(sub1)
        os.makedirs(sub2)

        # sub1 h1-b23-t2 h1-b24-t2
        # sub2 h1-b24-t2
        idx_head = random.randint(0, self._blocks - 1)
        idx_b1 = random.randint(0, self._blocks - 1)
        idx_s1f1b2 = random.randint(0, self._blocks - 1)
        while True:
            idx_s1f2b2 = random.randint(0, self._blocks - 1)
            if idx_s1f2b2 != idx_s1f1b2:
                break
        idx_tail = random.randint(0, self._blocks - 1)

        f_s1f1, bytes_blocks = self._concat(
            heads=[idx_head], bodys=[idx_b1, idx_s1f1b2], tails=[idx_tail]
        )
        self._create_file(os.path.join(sub1, f"sub1-{f_s1f1}"), bytes_blocks)

        f_s1f2, bytes_blocks = self._concat(
            heads=[idx_head], bodys=[idx_b1, idx_s1f2b2], tails=[idx_tail]
        )
        self._create_file(os.path.join(sub1, f"sub1-{f_s1f2}"), bytes_blocks)
        self._create_file(os.path.join(sub2, f"sub2-{f_s1f2}"), bytes_blocks)

        # sub1 h1-b234-t0 h1-b134-t1 h1-b234-t1
        f_s1, bytes_blocks = self._concat(heads=[1], bodys=[2, 3, 4], tails=[0])
        self._create_file(os.path.join(sub1, f"xsub1-{f_s1}"), bytes_blocks)
        f_s1, bytes_blocks = self._concat(heads=[1], bodys=[1, 3, 4], tails=[1])
        self._create_file(os.path.join(sub1, f"ysub1-{f_s1}"), bytes_blocks)
        f_s1, bytes_blocks = self._concat(heads=[1], bodys=[2, 3, 4], tails=[1])
        self._create_file(os.path.join(sub1, f"zsub1-{f_s1}"), bytes_blocks)


if __name__ == "__main__":
    unittest.main()
