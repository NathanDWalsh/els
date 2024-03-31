from pydantic import BaseModel
from typing import Optional, Union
import sqlalchemy as sa
from enum import Enum

from eel.pathprops import HumanPathPropertiesMixin


def generate_enum_from_properties(cls, enum_name):
    properties = {
        name.upper(): "_" + name
        for name, value in vars(cls).items()
        if isinstance(value, property)
        and not getattr(value, "__isabstractmethod__", False)
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


class ToSql(BaseModel, extra="allow"):
    chunksize: Optional[int] = None


class Stack(BaseModel, extra="forbid"):
    fixed_columns: int
    stack_header: int = 0
    stack_name: str = "stack_column"


class Frame(BaseModel):
    @property
    def db_connection_string(self):
        pass

    @property
    def sqn(self):
        pass

    stack: Optional[Stack] = None

    type: Optional[str] = None
    server: Optional[str] = None
    database: Optional[str] = None
    dbschema: Optional[str] = None
    table: Optional[str] = None
    file_path: Optional[str] = None


class Target(Frame, extra="forbid", use_enum_values=True, validate_default=True):

    consistency: TargetConsistencyValue = TargetConsistencyValue.STRICT
    if_exists: TargetIfExistsValue = TargetIfExistsValue.FAIL
    to_sql: Optional[ToSql] = None

    table: Optional[str] = "_" + HumanPathPropertiesMixin.leaf_name.fget.__name__
    type: Optional[str] = "pandas"

    @property
    def db_connection_string(self) -> Optional[str]:
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
    def sqn(self) -> Optional[str]:
        if self.dbschema and self.table:
            res = "[" + self.dbschema + "].[" + self.table + "]"
        elif self.table:
            res = "[" + self.table + "]"
        else:
            res = None
        return res

    @property
    def table_exists(self) -> Optional[bool]:
        if self.db_connection_string and self.table and self.dbschema:
            with sa.create_engine(self.db_connection_string).connect() as sqeng:
                inspector = sa.inspect(sqeng)
                res = inspector.has_table(self.table, self.dbschema)
        else:
            res = None
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
    # dtype: Optional[dict] = None


class ReadExcel(BaseModel, extra="allow"):
    sheet_name: Optional[str] = "_" + HumanPathPropertiesMixin.leaf_name.fget.__name__
    # dtype: Optional[dict] = None
    names: Optional[list] = None


class Source(Frame, extra="forbid"):
    # _parent: 'Config' = None

    # @property
    # def parent(self) -> 'Config':
    #     return self._parent

    type: Optional[str] = "_" + HumanPathPropertiesMixin.file_extension.fget.__name__
    file_path: Optional[str] = (
        "_" + HumanPathPropertiesMixin.file_path_abs.fget.__name__
    )

    load_parallel: bool = False
    nrows: Optional[int] = None
    dtype: Optional[dict] = None
    read_csv: Optional[ReadCsv] = None
    read_excel: Optional[ReadExcel] = None


class AddColumns(BaseModel, extra="allow"):
    additionalProperties: Optional[
        Union[DynamicPathValue, DynamicColumnValue, str, int, float]  # type: ignore
    ] = None


class Config(BaseModel, extra="forbid"):
    # sub_path: str = "."
    target: Target = Target()
    source: Source = Source()
    add_cols: AddColumns = AddColumns()
    children: Union[list[dict[str, "Config"]], list[str], str, None] = None

    @property
    def nrows(self) -> Optional[int]:
        if self.target:
            res = self.source.nrows
        else:
            res = 100
        return res

    # @property
    # def dtype(self):
    #     return self.source.dtype
