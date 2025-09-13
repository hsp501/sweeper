import os
import sqlite3
import traceback
from typing import Dict, List, Optional, Tuple

from src import Util


class HashDB:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.create_tables()

    def create_tables(self):
        cursor = self.conn.cursor()

        # 文件表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS file (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT NOT NULL,
                name TEXT NOT NULL,
                size INTEGER NOT NULL,
                mtime REAL NOT NULL,
                UNIQUE(path, name)
            )
        """)

        # 分段 hash 表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chunk_hash (
                fid INTEGER NOT NULL,
                serial INTEGER NOT NULL,
                block_size INTEGER NOT NULL,
                hash TEXT NOT NULL,
                PRIMARY KEY (fid, serial),
                FOREIGN KEY (fid) REFERENCES file(id) ON DELETE CASCADE
            )
        """)

        self.conn.commit()

    # 插入文件信息，返回 fid
    def add_file(self, *, path: str, name: str, size: int, mtime: float) -> int:
        cursor = self.conn.cursor()

        try:
            cursor.execute(
                """
                INSERT INTO file (path, name, size, mtime)
                VALUES (?, ?, ?, ?)
            """,
                (os.path.abspath(path), name, size, mtime),
            )
            self.conn.commit()

            return cursor.lastrowid
        except Exception:
            Util.debug(
                f"add_file(path={path}, name={name}, size={size}, mtime: {mtime})",
                fmt_time=True,
            )
            traceback.print_exc()
            self.conn.rollback()

            return -1

    # 插入分段 hash（可以批量）
    def add_chunk_hashes(self, *, fid: int, hashes: List[Tuple[int, int, str]]) -> bool:
        cursor = self.conn.cursor()

        try:
            cursor.executemany(
                """
                INSERT INTO chunk_hash (fid, serial, block_size, hash)
                VALUES (?, ?, ?, ?)
            """,
                [(fid, serial, size, hash) for serial, size, hash in hashes],
            )
            self.conn.commit()

            return True
        except Exception:
            Util.debug(
                f"add_chunk_hashes(fid={fid}, hashes-{len(hashes)}={hashes[0]})",
                fmt_time=True,
            )
            traceback.print_exc()
            self.conn.rollback()

            return False

    # 按 path 查询文件信息
    def get_file(self, path: str) -> Optional[Dict]:
        fp = os.path.abspath(path)

        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM file WHERE path = ? AND name =?",
            (os.path.dirname(fp), os.path.basename(fp)),
        )
        row = cursor.fetchone()

        return dict(row) if row else None

    # 查询文件信息
    def get_file_by_id(self, fid: int) -> Optional[Dict]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM file WHERE id = ?", (fid,))
        row = cursor.fetchone()

        return dict(row) if row else None

    # 按 size 查询文件信息
    def get_file_by_size(self, size: int) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM file WHERE size = ? ORDER BY id, path, name", (size,)
        )
        rows = cursor.fetchall()

        if rows:
            rows = [dict(row) for row in rows]
        return rows

    def update_file(self, *, fid: int, size: int, mtime: float) -> bool:
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "UPDATE file SET size = ?, mtime = ? WHERE id = ?", (size, mtime, fid)
            )
            self.conn.commit()

            return True
        except Exception:
            Util.debug(f"update_file(fid={fid})", fmt_time=True)
            traceback.print_exc()
            self.conn.rollback()

            return False

    def get_file_details(
        self, *, path: str, size: int, mtime: float
    ) -> Tuple[int, List]:
        fid = -1
        chunk_hashes = None
        flag_delete = False

        row = self.get_file(path)
        if row:
            fid = row["id"]
            if row["size"] != size or row["mtime"] != mtime:
                flag_delete = True
            if not flag_delete:
                chunk_hashes = self.get_chunk_hashes(fid)
                for idx, block_hash in enumerate(chunk_hashes):
                    if idx + 1 != block_hash["serial"]:
                        flag_delete = True
                        break

            if (
                flag_delete
                and self.delete_chunk_hashes(fid)
                and self.update_file(fid=fid, size=size, mtime=mtime)
            ):
                return fid, None

            if flag_delete:
                self.delete_file(fid)
                return -1, None

        return fid, chunk_hashes

    # 查询分段 hash
    def get_chunk_hashes(self, fid: int) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT serial, block_size, hash FROM chunk_hash WHERE fid = ? ORDER BY serial",
            (fid,),
        )
        rows = cursor.fetchall()

        if rows:
            rows = [dict(row) for row in rows]
        return rows

    def delete_chunk_hashes(self, fid: int) -> bool:
        try:
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM chunk_hash WHERE fid = ?", (fid,))
            self.conn.commit()

            return True
        except Exception:
            Util.debug(f"delete_chunk_hashes(fid={fid})", fmt_time=True)
            traceback.print_exc()
            self.conn.rollback()

            return False

    # 删除文件及其关联的 hash
    def delete_file(self, fid: int) -> bool:
        try:
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM file WHERE id = ?", (fid,))
            self.conn.commit()

            return True
        except Exception:
            Util.debug(f"delete_file(fid={fid})", fmt_time=True)
            traceback.print_exc()
            self.conn.rollback()

            return False

    def close(self):
        self.conn.close()
