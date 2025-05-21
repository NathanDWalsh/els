import sys
from collections.abc import MutableMapping, MutableSequence
from typing import Any, Literal, Union

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

_IfSheetExistsLiteral = Literal["overlay", "replace"]
if sys.version_info >= (3, 10):
    IfSheetExistsLiteral: TypeAlias = _IfSheetExistsLiteral
else:
    IfSheetExistsLiteral = _IfSheetExistsLiteral


def listify(v: Union[Any, MutableSequence[Any]]) -> list[Any]:
    return list(v) if isinstance(v, MutableSequence) else [v]


_FrameModeLiteral = Literal["s", "r", "a", "w", "m"]
# (s)oftread: only loads the name
# (m)edium read: sample/meta read reads the first rows_for_sampling
# (r)ead    : nothing yet to be written
# (a)ppend  : append df to df_target
# (w)rite   : overwrite df_target with df
if sys.version_info >= (3, 10):
    FrameModeLiteral: TypeAlias = _FrameModeLiteral
else:
    FrameModeLiteral = _FrameModeLiteral

_ContainerModeLiteral = Literal["r", "a", "w"]
if sys.version_info >= (3, 10):
    ContainerModeLiteral: TypeAlias = _ContainerModeLiteral
else:
    ContainerModeLiteral = _ContainerModeLiteral
