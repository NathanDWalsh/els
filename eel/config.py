from pydantic import BaseModel, root_validator
from typing import Optional, Union
import sqlalchemy as sa
from enum import Enum

from eel.pathprops import HumanPathPropertiesMixin


def generate_enum_from_properties(cls, enum_name):
    properties = {
        name.upper(): "_" + name
        for name, value in vars(cls).items()
        if isinstance(value, property)
    }
    return Enum(enum_name, properties)


DynamicPathValue = generate_enum_from_properties(
    HumanPathPropertiesMixin, "DynamicPathValue"
)


class DynamicColumnValue(Enum):
    ROW_INDEX = "_row_index"


class TargetConsistencyValue(Enum):
    STRICT = "strict"
    IGNORE = "ignore"


class TargetIfExistsValue(Enum):
    FAIL = "fail"
    REPLACE = "replace"
    APPEND = "append"
    TRUNCATE = "truncate"


class EnumToValueMixin(BaseModel):
    @root_validator(skip_on_failure=True)
    def convert_enums(cls, values):
        for key, value in values.items():
            if isinstance(value, Enum):
                values[key] = value.value
        return values


class ToSql(BaseModel, extra="allow"):
    chunksize: Optional[int] = None


class Frame(BaseModel):
    type: Optional[str] = None
    server: Optional[str] = None
    database: Optional[str] = None
    dbschema: Optional[str] = None
    table: Optional[str] = None
    file_path: Optional[str] = None


class Target(Frame, EnumToValueMixin, extra="forbid"):
    consistency: TargetConsistencyValue = TargetConsistencyValue.STRICT
    if_exists: TargetIfExistsValue = TargetIfExistsValue.FAIL
    to_sql: to_sql = None

    table: Optional[str] = "_" + HumanPathPropertiesMixin.leaf_name.fget.__name__

    @property
    def db_connection_string(self) -> str:
        # Define the connection string based on the database type
        if self.type == "mssql":
            res = (
                f"mssql+pyodbc://{self.server}/{self.database}"
                "?driver=ODBC+Driver+17+for+SQL+Server"
            )
        elif self.type == "postgres":
            res = (
                "Driver={PostgreSQL};" f"Server={self.server};Database={self.database};"
            )
        elif self.type == "duckdb":
            res = f"Driver={{DuckDB}};Database={self.database};"
        else:
            res = None
        return res

    @property
    def sqn(self):
        if self.dbschema:
            res = "[" + self.dbschema + "].[" + self.table + "]"
        else:
            res = "[" + self.table + "]"
        return res

    @property
    def table_exists(self) -> bool:
        with sa.create_engine(self.db_connection_string).connect() as sqeng:
            inspector = sa.inspect(sqeng)
            res = inspector.has_table(self.table, self.dbschema)
        return res

    @property
    def preparation_action(self) -> str:
        if not self.table_exists or self.if_exists == TargetIfExistsValue.REPLACE.value:
            res = "create_replace"
        elif self.if_exists == TargetIfExistsValue.TRUNCATE.value:
            res = "truncate"
        elif self.if_exists == TargetIfExistsValue.FAIL.value:
            res = "fail"
        else:
            res = "no_action"
        return res


class ReadCsv(BaseModel, extra="allow"):
    encoding: Optional[str] = None
    low_memory: Optional[bool] = None
    sep: Optional[str] = None


class ReadExcel(BaseModel, extra="allow"):
    sheet_name: Optional[str] = "_" + HumanPathPropertiesMixin.leaf_name.fget.__name__
    dtype: Optional[dict] = None
    names: Optional[list] = None


class Stack(BaseModel, extra="forbid"):
    fixed_columns: int
    stack_header: int = 0
    stack_name: str = "stack_column"


class Source(Frame, extra="forbid"):
    type: Optional[str] = "_" + HumanPathPropertiesMixin.file_extension.fget.__name__
    file_path: Optional[str] = (
        "_" + HumanPathPropertiesMixin.file_path_abs.fget.__name__
    )

    load_parallel: bool = False
    nrows: Optional[int] = None
    read_csv: Optional[ReadCsv] = None
    read_excel: Optional[ReadExcel] = None
    stack: Optional[Stack] = None


class AddColumns(BaseModel, extra="allow"):
    additionalProperties: Optional[
        Union[DynamicPathValue, DynamicColumnValue, str, int, float]
    ] = None


class Config(BaseModel, extra="forbid"):
    sub_path: str = "."
    target: Target = Target()
    source: Source = Source()
    add_cols: AddColumns = AddColumns()

    # def dict(self):

    @property
    def nrows(self) -> int:
        if self.target:
            res = self.source.nrows
        else:
            res = 100
        return res


def del_nones_from_dict(base_dict: dict) -> dict:
    return {k: v for k, v in base_dict.items() if v is not None}


def del_nones_from_dict_recursive(base_dict: dict) -> dict:
    res = {k: v for k, v in base_dict.items() if v is not None}
    for k, v in res.items():
        if isinstance(v, dict):
            res[k] = del_nones_from_dict_recursive(v)
    return res
