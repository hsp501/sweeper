class Util:
    def bytes_readable(num_bytes: int) -> str:
        size = float(num_bytes)
        units = ["B", "KB", "MB", "GB", "TB", "PB"]
        for unit in units:
            if size < 1024:
                return f"{size:.2f} {unit}"
            size /= 1024

        return f"{size:.2f} PB"
