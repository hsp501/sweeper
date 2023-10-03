class hash_config:
    @staticmethod
    def algorithm() -> str:
        return 'md5'

    @staticmethod
    def block_size() -> int:
        return 64 * 1024
