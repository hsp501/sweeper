# sweeper
find and clean duplicate files to release disk space

# log format
target directories:
    > local
        > redudant: X:\path\to\data
        > E:\pundi\FX6300-1
    > snc(example.com:8000)
        > /path/to/data
    > synology(192.168.25.100:8000)
        > /path/to/data

operation config:
    > mode: dry-run
    > max deletion: 10
    > hash algorithm: md5
    > block size: 16 * 1024 * 1024 (16.00 MB)

2023-10-04 20:53:12 operation started
2023-10-04 20:53:31 file scan finished(0:00:19), 44783 files spreading over 27615 size groups

1 10:32 2877535942 bytes (2.68 GB) 128 blocks e6c2bf57202263f317c4728fe8ce9f44(first block hex hash)
      local      E:\pundi\FX6300-2\淘宝学习文件\100CANON\ps121.tmp
      local      E:\pundi\FX6300-1\老电脑D盘\淘宝学习文件\100CANON\ps121.tmp
    - local      E:\pundi\FX6300-1\老电脑D盘\100CANON\ps121.tmp
      snc        /path/to/data/ps121.tmp
      synology   /path/to/data/ps121.tmp

2023-10-05 10:59:01 operation finished(0:26:58), space will be freed: 0.00 B (0), hash consumption: 94.97 GB (101977633371)

# controller xml format
<?xml version="1.0" encoding="UTF-8"?>
<ctrl ns="nameserver.com:1234">
    <host alias="snc" />
    <host alias="backup" />
</ctrl>

# worker xml format
<?xml version="1.0" encoding="UTF-8"?>
<worker alias="snc" expose="public.com:1234" ns="nameserver.com:1234">
    <dir>/path/to/data</dir>
    <dir redudant="true">/path/to/data/1</dir>
</worker>