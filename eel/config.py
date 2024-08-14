from pydantic import BaseModel, ConfigDict
from typing import Optional, Union
import sqlalchemy as sa
import os
import pandas as pd
import re
from enum import Enum
from urllib.parse import urlparse

from eel.pathprops import HumanPathPropertiesMixin

import yaml
from copy import deepcopy


# generate an enum in the format _rxcx for a 10 * 10 grid
def generate_enum_from_grid(cls, enum_name):
    properties = {f"R{r}C{c}": f"_r{r}c{c}" for r in range(10) for c in range(10)}
    return Enum(enum_name, properties)


DynamicCellValue = generate_enum_from_grid(HumanPathPropertiesMixin, "DynamicCellValue")


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
    REPLACE_FILE = "replace_file"


class ToSql(BaseModel, extra="allow"):
    chunksize: Optional[int] = None


class ToCsv(BaseModel, extra="allow"):
    pass


class ToExcel(BaseModel, extra="allow"):
    pass


class Stack(BaseModel, extra="forbid"):
    fixed_columns: int
    stack_header: int = 0
    stack_name: str = "stack_column"


class Melt(BaseModel, extra="forbid"):
    id_vars: list[str]
    value_vars: Optional[list[str]] = None
    value_name: str = "value"
    var_name: str = "variable"


class AsType(BaseModel, extra="forbid"):
    dtype: dict[str, str]


class Transform(BaseModel):

    melt: Optional[Melt] = None
    stack: Optional[Stack] = None
    astype: Optional[AsType] = None

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={"oneOf": [{"required": ["melt"]}, {"required": ["stack"]}]},
    )


class Frame(BaseModel):

    @property
    def db_connection_string(self) -> Optional[str]:
        # Define the connection string based on the database type
        if self.type == "mssql":
            res = self.url.replace("mssql://", "mssql+pyodbc://", 1)
            # res = (
            #     f"mssql+pyodbc://{self.server}/{self.database}"
            #     "?driver=ODBC+Driver+17+for+SQL+Server"
            # )
        elif self.type == "sqlite":
            res = self.url
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

    # @property
    # def file_path(self):
    #     if self.type in (
    #         ".csv",
    #         ".tsv",
    #         ".xlsx",
    #         ".xls",
    #     ) and not self.file_path.endswith(self.type):
    #         return f"{self.file_path}{self.type}"
    #     else:
    #         return self.file_path

    url: Optional[str] = None
    # type: Optional[str] = None
    # server: Optional[str] = None
    # database: Optional[str] = None
    dbschema: Optional[str] = None
    table: Optional[str] = "_" + HumanPathPropertiesMixin.leaf_name.fget.__name__

    @property
    def type(self):
        if not self.url:
            return "pandas"
        elif self.url_scheme == "file":
            return os.path.splitext(self.url)[-1]
        else:
            return self.url_scheme

    @property
    def url_scheme(self):
        if self.url:
            url_parse_scheme = urlparse(self.url, scheme="file").scheme
            drive_letter_pattern = re.compile(r"^[a-zA-Z]$")
            if drive_letter_pattern.match(url_parse_scheme):
                return "file"
            return url_parse_scheme
        else:
            return None

    @property
    def sheet_name(self):
        if self.type in (".xlsx", ".xls", ".xlsb", ".xlsm"):
            return self.table
        else:
            return None


class Target(Frame):

    model_config = ConfigDict(
        extra="forbid", use_enum_values=True, validate_default=True
    )

    consistency: TargetConsistencyValue = TargetConsistencyValue.STRICT
    if_exists: Optional[TargetIfExistsValue] = None
    to_sql: Optional[ToSql] = None
    to_csv: Optional[ToCsv] = None
    to_excel: Optional[ToExcel] = None

    @property
    def file_exists(self) -> Optional[bool]:
        if self.url and self.type in (".csv", ".tsv", ".xlsx"):
            # check file exists
            res = os.path.exists(self.url)
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
            with pd.ExcelFile(self.url) as xls:
                res = self.sheet_name in xls.sheet_names
        else:
            res = None
        return res

    @property
    def preparation_action(self) -> str:
        if not self.if_exists:
            res = "fail"
        elif self.url_scheme == "file" and (
            not self.file_exists
            or self.if_exists == TargetIfExistsValue.REPLACE_FILE.value
        ):
            res = "create_replace_file"
        elif (
            not self.table_exists or self.if_exists == TargetIfExistsValue.REPLACE.value
        ):
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


class ReadFwf(BaseModel, extra="allow"):
    names: Optional[list] = None


class ReadXml(BaseModel, extra="allow"):
    pass


class Source(Frame, extra="forbid"):
    # _parent: 'Config' = None

    # @property
    # def parent(self) -> 'Config':
    #     return self._parent

    # type: Optional[str] = "_" + HumanPathPropertiesMixin.file_extension.fget.__name__
    # file_path: Optional[str] = (
    #     "_" + HumanPathPropertiesMixin.file_path_abs.fget.__name__
    # )

    load_parallel: bool = False
    nrows: Optional[int] = None
    dtype: Optional[dict] = None
    read_csv: Optional[ReadCsv] = None
    read_excel: Optional[ReadExcel] = None
    read_fwf: Optional[ReadFwf] = None
    read_xml: Optional[ReadXml] = None


class AddColumns(BaseModel, extra="allow"):
    additionalProperties: Optional[
        Union[DynamicPathValue, DynamicColumnValue, DynamicCellValue, str, int, float]
    ] = None


class Config(BaseModel):
    # sub_path: str = "."
    config_path: Optional[str] = None
    source: Source = Source()
    target: Target = Target()
    add_cols: AddColumns = AddColumns()
    transform: Optional[Transform] = None
    children: Union[dict[str, Optional["Config"]], list[str], str, None] = None

    def schema_pop_children(s):
        s["properties"].pop("children")

    model_config = ConfigDict(extra="forbid", json_schema_extra=schema_pop_children)

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


def main():
    config_json = Config.model_json_schema()

    # keep enum typehints on an arbatrary number of elements in AddColumns
    # additionalProperties property attribute functions as a placeholder
    config_json["$defs"]["AddColumns"]["additionalProperties"] = deepcopy(
        config_json["$defs"]["AddColumns"]["properties"]["additionalProperties"]
    )
    del config_json["$defs"]["AddColumns"]["properties"]

    config_yml = yaml.dump(config_json, default_flow_style=False)

    with open("eel_schema.yml", "w") as file:
        file.write(config_yml)


if __name__ == "__main__":
    main()
