import sys
from collections.abc import MutableMapping
from typing import Any, Literal

if sys.version_info >= (3, 10):
    from typing import TypeAlias

_KWArgsIO = MutableMapping[str, Any]
if sys.version_info >= (3, 10):
    KWArgsIO: TypeAlias = _KWArgsIO
else:
    KWArgsIO = _KWArgsIO


_IfExistsLiteral = Literal[
    "fail",
    "truncate",
    "append",
    "replace",
    "replace_file",
    "replace_database",
]

if sys.version_info >= (3, 10):
    IfExistsLiteral: TypeAlias = _IfExistsLiteral
else:
    IfExistsLiteral = _IfExistsLiteral
