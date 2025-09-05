class hashConfig:
    @staticmethod
    def algorithm() -> str:
        return "md5"

    @staticmethod
    def block_kb(first: bool) -> int:
        return 128 if first else 64 * 1024
