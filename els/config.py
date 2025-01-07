import logging
import os
import re
from copy import deepcopy
from enum import Enum
from functools import cached_property
from typing import Optional, Union
from urllib.parse import parse_qs, urlencode, urlparse

import pandas as pd
import pyodbc
import sqlalchemy as sa
import yaml
from pydantic import BaseModel, ConfigDict

from els.pathprops import HumanPathPropertiesMixin


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


supported_mssql_odbc_drivers = {
    "sql server native client 11.0",
    "odbc driver 17 for sql server",
    "odbc driver 18 for sql server",
}


def available_odbc_drivers():
    available = pyodbc.drivers()
    lcased = {v.lower() for v in available}
    return lcased


def supported_available_odbc_drivers():
    supported = supported_mssql_odbc_drivers
    available = available_odbc_drivers()
    return supported.intersection(available)


def lcase_dict_keys(_dict):
    return {k.lower(): v for k, v in _dict.items()}


def lcase_query_keys(query):
    query_parsed = parse_qs(query)
    return lcase_dict_keys(query_parsed)


class Frame(BaseModel):
    @cached_property
    def db_url_driver(self):
        url_parsed = urlparse(self.url)
        query = parse_qs(url_parsed.query)
        query_lcased = {k.lower(): v[0].lower() for k, v in query.items()}
        if "driver" in query_lcased.keys():
            return query_lcased["driver"]
        else:
            return False

    @cached_property
    def choose_db_driver(self):
        explicit_driver = self.db_url_driver
        if explicit_driver and explicit_driver in supported_mssql_odbc_drivers:
            return explicit_driver
        else:
            return None

    @cached_property
    def odbc_driver_supported_available(self):
        explicit_odbc = self.db_url_driver
        if explicit_odbc and explicit_odbc in supported_available_odbc_drivers():
            return True
        else:
            return False

    @cached_property
    def db_connection_string(self) -> Optional[str]:
        # Define the connection string based on the database type
        if self.type in (
            "mssql+pymssql",
            "mssql+pyodbc",
        ):  # assumes advanced usage and url must be correct
            return self.url
        elif (
            self.type == "mssql"
        ):  # try to automatically detect odbc drivers and falls back on tds/pymssql
            url_parsed = urlparse(self.url)._replace(scheme="mssql+pyodbc")
            if self.odbc_driver_supported_available:
                query = lcase_query_keys(url_parsed.query)
                query["driver"] = query["driver"][0]
                if query["driver"].lower() == "odbc driver 18 for sql server":
                    query["TrustServerCertificate"] = "yes"
                res = url_parsed._replace(query=urlencode(query)).geturl()
                # res = url_parsed.geturl()
            elif len(supported_available_odbc_drivers()):
                logging.info(
                    "No valid ODBC driver defined in connection string, choosing one."
                )
                query = lcase_query_keys(url_parsed.query)
                query["driver"] = list(supported_available_odbc_drivers())[0]
                logging.info(query["driver"].lower())
                if query["driver"].lower() == "odbc driver 18 for sql server":
                    query["TrustServerCertificate"] = "yes"
                res = url_parsed._replace(query=urlencode(query)).geturl()
            else:
                logging.info("No ODBC drivers for pyodbc, using pymssql")
                res = urlparse(self.url)._replace(scheme="mssql+pymssql").geturl()
        elif self.type in ("sqlite", "duckdb"):
            res = self.url
        elif self.type == "postgres":
            res = "Driver={{PostgreSQL}};Server={self.server};Database={self.database};"
        # elif self.type == "duckdb":
        #     res = f"Driver={{DuckDB}};Database={self.url.replace('duckdb://','')};"
        else:
            res = None
        return res

    @cached_property
    def sqn(self) -> Optional[str]:
        if self.type == "duckdb":
            res = '"' + self.table + '"'
        elif self.dbschema and self.table:
            res = "[" + self.dbschema + "].[" + self.table + "]"
        elif self.table:
            res = "[" + self.table + "]"
        else:
            res = None
        return res

    # @cached_property
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
    # table: Optional[str] = "_" + HumanPathPropertiesMixin.leaf_name.fget.__name__
    table: Optional[str] = None

    @cached_property
    def type(self):
        if not self.url:
            return "pandas"
        elif self.url_scheme == "file":
            ext = os.path.splitext(self.url)[-1]
            if ext in (".txt"):
                return ".csv"
            else:
                return ext
        else:
            return self.url_scheme

    @cached_property
    def type_is_db(self):
        if self.type in (
            "mssql",
            "mssql+pymssql",
            "mssql+pyodbc",
            "postgres",
            "duckdb",
            "sqlite",
        ):
            return True
        return False

    @cached_property
    def url_scheme(self):
        if self.url:
            url_parse_scheme = urlparse(self.url, scheme="file").scheme
            drive_letter_pattern = re.compile(r"^[a-zA-Z]$")
            if drive_letter_pattern.match(url_parse_scheme):
                return "file"
            return url_parse_scheme
        else:
            return None

    @cached_property
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

    @cached_property
    def file_exists(self) -> Optional[bool]:
        if self.url and self.type in (".csv", ".tsv", ".xlsx"):
            # check file exists
            res = os.path.exists(self.url)
        else:
            res = None
        return res

    @cached_property
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

    @cached_property
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

    # @cached_property
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

    @cached_property
    def nrows(self) -> Optional[int]:
        if self.target:
            res = self.source.nrows
        else:
            res = 100
        return res

    @cached_property
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

    # @cached_property
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

    with open("els_schema.yml", "w") as file:
        file.write(config_yml)


if __name__ == "__main__":
    main()
