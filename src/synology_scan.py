import os
import stat
from datetime import datetime

from src import ChunkHash, HashDB


class Scan:
    def __init__(self) -> None:
        self.counter = 0
        self.ch = ChunkHash()

        dir_db = os.path.dirname(os.path.abspath(__file__))
        dir_db = os.path.dirname(dir_db)
        self.db = HashDB(os.path.join(dir_db, "sweeper_v2.db"))

    def scan_dir(self, root: str):
        print(f"\n{datetime.now().strftime('%H:%M:%S')} scaning {root} ......")

        for folder, _, files in os.walk(root):
            for file in files:
                dir = os.path.join(root, folder)
                fp = os.path.join(dir, file)
                fs = os.stat(fp, follow_symlinks=False)
                if stat.S_ISREG(fs.st_mode) and fs.st_size > 0 and "@eaDir" not in dir:
                    row = self.db.get_file(fp)
                    if row:
                        fid = row["id"]
                        if row["size"] == fs.st_size and row["mtime"] == fs.st_mtime:
                            chunk_hashes = self.db.get_chunk_hashes(fid)
                            if chunk_hashes and self.db.verify_chunk_hashes(
                                chunk_hashes
                            ):
                                continue
                        self.db.delete_file(fid)

                    fid = self.db.add_file(
                        path=dir,
                        name=file,
                        size=fs.st_size,
                        mtime=fs.st_mtime,
                    )
                    if -1 != fid:
                        hash, blk_size = self.ch.block_hash(path=fp, serial=1)
                        self.db.add_chunk_hashes(fid=fid, hashes=[(1, blk_size, hash)])

                        self.counter += 1
                        if self.counter % 1000 == 0:
                            print(
                                f"{datetime.now().strftime('%H:%M:%S')} {self.counter:05d}: {fp}"
                            )
