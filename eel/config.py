from pydantic import BaseModel, ConfigDict
from typing import Optional, Union
import sqlalchemy as sa
import os
import pandas as pd
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


class ToCsv(BaseModel, extra="allow"):
    pass


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

    @property
    def address(self):
        match self.type:
            case ".xlsx":
                return self.file_path + "!" + self.sheet_name
            case ".csv":
                return self.file_path
            case "pandas":
                return None
            case _:
                if self.dbschema and self.table:
                    return (
                        {self.server} / {self.database} / {self.dbschema} / {self.table}
                    )
                elif self.table:
                    return {self.server} / {self.database} / {self.table}

    @property
    def file_path_dynamic(self):
        if self.type in (".csv", ".tsv", ".xlsx") and not self.file_path.endswith(
            self.type
        ):
            return f"{self.file_path}{self.type}"
        else:
            return self.file_path

    stack: Optional[Stack] = None

    type: Optional[str] = None
    server: Optional[str] = None
    database: Optional[str] = None
    dbschema: Optional[str] = None
    table: Optional[str] = None
    file_path: Optional[str] = None

    @property
    def sheet_name(self):
        if self.type == ".xlsx":
            return self.table
        else:
            return None


class Target(Frame):

    model_config = ConfigDict(
        extra="forbid", use_enum_values=True, validate_default=True
    )

    consistency: TargetConsistencyValue = TargetConsistencyValue.STRICT
    if_exists: TargetIfExistsValue = TargetIfExistsValue.FAIL
    to_sql: Optional[ToSql] = None
    to_csv: Optional[ToCsv] = None

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
    def file_exists(self) -> Optional[bool]:
        if self.file_path_dynamic and self.type in (".csv", ".tsv", ".xlsx"):
            # check file exists
            res = os.path.exists(self.file_path_dynamic)
        else:
            res = None
        return res

    @property
    def table_exists(self) -> Optional[bool]:
        if self.db_connection_string and self.table and self.dbschema:
            with sa.create_engine(self.db_connection_string).connect() as sqeng:
                inspector = sa.inspect(sqeng)
                res = inspector.has_table(self.table, self.dbschema)
        elif self.db_connection_string and self.table:
            with sa.create_engine(self.db_connection_string).connect() as sqeng:
                inspector = sa.inspect(sqeng)
                res = inspector.has_table(self.table)
        elif self.type in (".csv", ".tsv"):
            res = self.file_exists
        elif (
            self.type in (".xlsx") and self.file_exists
        ):  # TODO: add other file types supported by Calamine
            # check if sheet exists
            with pd.ExcelFile(self.file_path_dynamic) as xls:
                res = self.sheet_name in xls.sheet_names
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

    @property
    def pipe_id(self) -> Optional[str]:
        if self.source and self.source.address and self.target and self.target.address:
            res = (self.source.address, self.target.address)
        elif self.source and self.source.address:
            res = (self.source.address,)
        elif self.target and self.target.address:
            res = (self.target.address,)
        else:
            res = None
        return res

    # @property
    # def dtype(self):
    #     return self.source.dtype
