from enum import StrEnum


class Command(StrEnum):
    CHECK_SIZE = "check_size"
    ECHO_CHECK_SIZE = "echo_check_size"

    CHECK_HASH = "check_hash"
    ECHO_CHECK_HASH = "echo_check_hash"


class Key(StrEnum):
    COMMAND = "command"
    DEVICE_ID = "device_id"
    REQUEST_ID = "request_id"
    LOCAL_MODE = "local_mode"
    PATH = "path"
    SIZE = "size"
    HASH = "hashes"
    RESULT = "result"
