import os
import random
import subprocess
import unittest

from src.chunk_hash import BLOCK_SIZE, HEAD_SIZE, READ_SIZE, ChunkHash


class TestChunkHash(unittest.TestCase):
    def setUp(self):
        self._file_id = 1
        self._ch = ChunkHash()
        self._dir_temp = os.path.dirname(os.path.abspath(__file__))

    def tearDown(self):
        for id in range(1, self._file_id):
            os.remove(os.path.join(self._dir_temp, f"chunk_hash_{id:03d}.bin"))

    def _create_file(self, *, size: int, id: int) -> str:
        to_write = size
        file_path = os.path.join(self._dir_temp, f"chunk_hash_{id:03d}.bin")
        with open(file_path, "wb") as f:
            while to_write > 0:
                blk_size = min(to_write, READ_SIZE)
                f.write(os.urandom(blk_size))
                to_write -= blk_size

        self.assertEqual(size, os.stat(file_path).st_size)

        return file_path

    def _hash_file(self, size: int, id: int):
        file_path = self._create_file(size=size, id=id)
        self.assertEqual(size, os.stat(file_path).st_size)

        if size <= HEAD_SIZE:
            blocks = 1
        else:
            blocks = (size - HEAD_SIZE) // BLOCK_SIZE + 1
            if (size - HEAD_SIZE) % BLOCK_SIZE > 0:
                blocks += 1
        self.assertEqual(blocks, self._ch.blocks(size))
        print(f"{os.path.basename(file_path)}: {size:09d} {blocks:02d}")

        for i in range(blocks):
            count = 1 if 0 == i else 512
            skip = 0 if 0 == i else (1 + (i - 1) * 512)

            hash, blk_size = self._ch.block_hash(path=file_path, serial=i + 1)
            cmd = f"dd if={file_path} bs=128K skip={skip} count={count} status=none | md5sum"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

            self.assertIn(hash, result.stdout)
            self.assertEqual(blk_size, self._ch.block_size(size, i + 1))
            print(f"{' ' * 4}{(i + 1):02d}: {hash} - {blk_size}")

    def test_chunk_hash(self):
        sizes = []

        for i in range(10):
            sizes.append(random.randint(1, HEAD_SIZE - 1))

        sizes.append(HEAD_SIZE)

        for i in range(5):
            sizes.append(HEAD_SIZE + random.randint(1, BLOCK_SIZE - 1))

        for i in range(5):
            sizes.append(HEAD_SIZE + random.randint(1, 3) * BLOCK_SIZE)

        for i in range(5):
            sizes.append(
                HEAD_SIZE
                + random.randint(1, 3) * BLOCK_SIZE
                + random.randint(1, BLOCK_SIZE - 1)
            )

        for size in sorted(sizes):
            self._hash_file(size, self._file_id)
            self._file_id += 1
            print("")


if __name__ == "__main__":
    unittest.main()
