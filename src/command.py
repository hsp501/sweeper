try:
    from enum import StrEnum
except ImportError:
    from enum import Enum

    class StrEnum(str, Enum):
        def __str__(self) -> str:
            return str(self.value)


class Command(StrEnum):
    CHECK_SIZE = "check_size"
    ECHO_CHECK_SIZE = "echo_check_size"

    CHECK_HASH = "check_hash"
    ECHO_CHECK_HASH = "echo_check_hash"

    CALC_FILE_HASH = "calc_file_hash"
    ECHO_CALC_FILE_HASH = "echo_calc_file_hash"


class Key(StrEnum):
    COMMAND = "command"
    DEVICE_ID = "device_id"
    SERVER_ID = "server_id"
    REQUEST_ID = "request_id"
    LOCAL_MODE = "local_mode"
    PATH = "path"
    SIZE = "size"
    HASH = "hashes"
    RESULT = "result"
