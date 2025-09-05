import random
import time
import unittest

from src.hash_db import HashDB


class TestFileHashDB(unittest.TestCase):
    def setUp(self):
        self.db = HashDB(":memory:")

    def tearDown(self):
        self.db.close()

    def test_file_insert_and_query(self):
        # 插入文件
        fid = self.db.add_file(
            path="/tmp", name="test1.txt", size=1234, mtime=time.time()
        )
        self.assertIsInstance(fid, int)

        # 查询文件
        file_record = self.db.get_file_by_id(fid)
        self.assertIsNotNone(file_record)
        self.assertEqual(file_record["name"], "test1.txt")

    def test_chunk_hash_insert_and_query(self):
        fid = self.db.add_file(
            path="/tmp", name="test2.txt", size=5678, mtime=time.time()
        )

        hashes = [(0, 128, "hash0"), (1, 64 * 1024, "hash1"), (2, 64 * 1024, "hash2")]
        self.db.add_chunk_hashes(fid=fid, hashes=hashes)

        queried_hashes = self.db.get_chunk_hashes(fid)
        self.assertEqual(len(queried_hashes), 3)
        self.assertEqual(queried_hashes[1]["hash"], "hash1")

    def test_delete_file_cascade(self):
        fid = self.db.add_file(
            path="/tmp", name="test3.txt", size=1000, mtime=time.time()
        )
        hashes = [(0, 128, "h0"), (1, 64 * 1024, "h1")]
        self.db.add_chunk_hashes(fid=fid, hashes=hashes)

        # 删除文件
        self.db.delete_file(fid)

        # 文件表应无记录
        self.assertIsNone(self.db.get_file_by_id(fid))

        # 分段 hash 应级联删除
        queried_hashes = self.db.get_chunk_hashes(fid)
        self.assertEqual(len(queried_hashes), 0)

    def test_get_file_by_size(self):
        self.db.add_file(path="/tmp", name="a.txt", size=100, mtime=time.time())
        self.db.add_file(path="/tmp", name="b.txt", size=100, mtime=time.time())
        self.db.add_file(path="/tmp", name="c.txt", size=200, mtime=time.time())

        files_100 = self.db.get_file_by_size(100)
        self.assertEqual(len(files_100), 2)

        names = [f["name"] for f in files_100]
        self.assertIn("a.txt", names)
        self.assertIn("b.txt", names)

        files_200 = self.db.get_file_by_size(200)
        self.assertEqual(len(files_200), 1)
        self.assertEqual(files_200[0]["name"], "c.txt")

        fid = self.db.add_file(path="/tmp", name="c.txt", size=400, mtime=time.time())
        self.assertEqual(fid, -1)

    def test_update_file(self):
        size = random.randint(20, 3000)
        mtime = time.time()
        delta_time = random.randint(10, 100)

        fid = self.db.add_file(path="/tmp", name="avg.txt", size=100, mtime=mtime)
        self.assertTrue(
            self.db.update_file(fid=fid, size=size + 5, mtime=mtime + delta_time)
        )
        row = self.db.get_file_by_id(fid)
        self.assertEqual(size + 5, row["size"])
        self.assertEqual(mtime + delta_time, row["mtime"])

    def test_get_file_details(self):
        size = random.randint(2000, 10000)
        mtime = time.time()
        fid = self.db.add_file(path="/tmp", name="detail.txt", size=size, mtime=mtime)
        hashes = [
            (1, 128 * 1024, "---hash0"),
            (2, 64 * 1024, "---hash1"),
            (3, 64 * 1024, "---hash2"),
        ]
        self.assertTrue(self.db.add_chunk_hashes(fid=fid, hashes=hashes))

        fid1, chunk_hashes = self.db.get_file_details(
            path="/tmp/detail.txt/", size=size, mtime=mtime
        )

        self.assertEqual(fid, fid1)
        for i, blk_hash in enumerate(hashes):
            self.assertEqual(
                blk_hash,
                (
                    chunk_hashes[i]["serial"],
                    chunk_hashes[i]["block_size"],
                    chunk_hashes[i]["hash"],
                ),
            )

        fid1, chunk_hashes = self.db.get_file_details(
            path="/tmp/detail.txt/", size=size + 1, mtime=mtime
        )
        self.assertEqual(fid, fid1)
        self.assertIsNone(chunk_hashes)


if __name__ == "__main__":
    unittest.main()
