import logging
import os
import re
from copy import deepcopy
from enum import Enum
from functools import cached_property
from typing import NewType, Optional, Union
from urllib.parse import parse_qs, urlencode, urlparse

import pyodbc
import sqlalchemy as sa
import yaml
from pydantic import BaseModel, ConfigDict

import els.core as el
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


class Transform(BaseModel, extra="forbid"):
    # THIS MAY BE USEFUL FOR CONTROLLING YAML INPUTS?
    # THE CODE BELOW WAS USED WHEN TRANSFORM CLASS HAD PROPERTIES INSTEAD OF A LIST
    # IT ONLY ALLOED EITHER MELT OR STACK TO BE SET (NOT BOTH)
    # model_config = ConfigDict(
    #     extra="forbid",
    #     json_schema_extra={"oneOf": [{"required": ["melt"]}, {"required": ["stack"]}]},
    # )
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._executed = False

    @property
    def executed(self):
        return self._executed

    @executed.setter
    def executed(self, v: bool):
        self._executed = v


class StackDynamic(Transform):
    stack_fixed_columns: int
    stack_header: int = 0
    stack_name: str = "stack_column"


class Melt(Transform):
    melt_id_vars: list[str]
    melt_value_vars: Optional[list[str]] = None
    melt_value_name: str = "value"
    melt_var_name: str = "variable"


class Pivot(Transform):
    pivot_columns: Optional[Union[str, list[str]]] = None
    pivot_values: Optional[Union[str, list[str]]] = None
    pivot_index: Optional[Union[str, list[str]]] = None


class AsType(Transform):
    as_dtypes: dict[str, str]


class AddColumns(Transform, extra="allow"):
    additionalProperties: Optional[
        Union[DynamicPathValue, DynamicColumnValue, DynamicCellValue, str, int, float]  # type: ignore
    ] = None


class PrqlTransform(Transform):
    prql: str


class FilterTransform(Transform):
    filter: str


class SplitOnColumn(Transform):
    split_on_column: str


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


def merge_configs(*configs: list[Union["Config", dict]]) -> "Config":
    dicts: list[dict] = []
    for config in configs:
        if isinstance(config, Config):
            dicts.append(
                config.model_dump(
                    exclude={"children"},
                    exclude_unset=True,
                )
            )
        elif isinstance(config, dict):
            # append all except children
            config_to_append = config.copy()
            if "children" in config_to_append:
                config_to_append.pop("children")
            dicts.append(config_to_append)
        else:
            raise Exception("configs should be a list of Configs or dicts")
    dict_result = merge_dicts_by_top_level_keys(*dicts)
    res = Config.model_validate(dict_result)  # type: ignore
    return res


def merge_dicts_by_top_level_keys(*dicts: dict) -> dict:
    merged_dict: dict = {}
    for dict_ in dicts:
        for key, value in dict_.items():
            if (
                key in merged_dict
                and isinstance(value, dict)
                and (merged_dict[key] is not None)
                and not isinstance(merged_dict[key], list)
            ):
                merged_dict[key].update(value)
            elif value is not None:
                # Add a new key-value pair to the merged dictionary
                merged_dict[key] = value
    return merged_dict


class Frame(BaseModel):
    @cached_property
    def file_exists(self) -> Optional[bool]:
        if self.url:
            res = os.path.exists(self.url)
        else:
            res = None
        return res

    @cached_property
    def query_lcased(self):
        url_parsed = urlparse(self.url)
        query = parse_qs(url_parsed.query)
        res = {k.lower(): v[0].lower() for k, v in query.items()}
        return res

    @cached_property
    def db_url_driver(self):
        query_lcased = self.query_lcased
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

    @property
    def df_dict_io(self):
        if self.type == "dict":
            dict_id = int(urlparse(self.url)[1])
            res = el.fetch_df_dict_io(dict_id)
            if res:
                return res
            else:
                raise Exception(f"dict_id {dict_id} not stored in collection.")
        else:
            raise Exception(
                f"not a dict frame type, but a {self.type} type and {self.url} url"
            )

    @property
    def df_dict(self):
        if self.type == "dict":
            return self.df_dict_io.df_dict
        return None

    @df_dict.setter
    def df_dict(self, _dict):
        self.url = f"dict://{id(_dict)}"
        el.fetch_df_dict_io(_dict)

    url: Optional[str] = None
    # type: Optional[str] = None
    # server: Optional[str] = None
    # database: Optional[str] = None
    dbschema: Optional[str] = None
    # table: Optional[str] = "_" + HumanPathPropertiesMixin.leaf_name.fget.__name__
    table: Optional[Union[str, list[str]]] = None

    @cached_property
    def type(self):
        if self.url_scheme == "file":
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
    def type_is_excel(self):
        if self.type in (
            ".xlsx",
            ".xls",
            ".xlsb",
            ".xlsm",
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
            return url_parse_scheme.lower()
        else:
            return None

    @cached_property
    def sheet_name(self):
        if self.type_is_excel:
            res = self.table or "Sheet1"
            res = re.sub(re.compile(r"[\\*?:/\[\]]", re.UNICODE), "_", res)
            return res[:31].strip()
        else:
            # raise Exception("Cannot fetch sheet name from non-spreadsheet format.")
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
        ):  # TODO: add other file types supported by Calamine, be careful not to support legacy excel
            # check if sheet exists
            xl_io = el.fetch_excel_io(self.url)
            sheet_names = xl_io.child_names
            res = self.sheet_name in sheet_names
        elif self.type == "dict":
            if (
                self.df_dict
                and self.table in self.df_dict
                and not self.df_dict[self.table].empty
            ):
                res = True
            else:
                res = False
        else:
            res = None
        return res

    @cached_property
    def build_action(self) -> str:
        if not self.if_exists:
            res = "fail"
        elif self.url_scheme == "file" and (
            self.if_exists == TargetIfExistsValue.REPLACE_FILE.value
            or not self.file_exists
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


class LAParams(BaseModel):
    line_overlap: Optional[float] = None
    char_margin: Optional[float] = None
    line_margin: Optional[float] = None
    word_margin: Optional[float] = None
    boxes_flow: Optional[float] = None
    detect_vertical: Optional[bool] = None
    all_texts: Optional[bool] = None


class ExtractPagesPdf(BaseModel):
    password: Optional[str] = None
    page_numbers: Optional[Union[int, list[int], str]] = None
    maxpages: Optional[int] = None
    caching: Optional[bool] = None
    laparams: Optional[LAParams] = None


class ReadXml(BaseModel, extra="allow"):
    pass


class Source(Frame, extra="forbid"):
    load_parallel: bool = False
    nrows: Optional[int] = None
    dtype: Optional[dict] = None
    read_csv: Optional[ReadCsv] = None
    read_excel: Optional[ReadExcel] = None
    read_fwf: Optional[ReadFwf] = None
    read_xml: Optional[ReadXml] = None
    extract_pages_pdf: Optional[Union[ExtractPagesPdf, list[ExtractPagesPdf]]] = None


TransformType = NewType(
    "TransformType",
    Union[
        SplitOnColumn,
        FilterTransform,
        PrqlTransform,
        Pivot,
        AsType,
        Melt,
        StackDynamic,
        AddColumns,
    ],
)


class Config(BaseModel):
    # KEEP config_path AROUND JUST IN CASE, can be used when priting yamls for debugging
    config_path: Optional[str] = None
    # source: Union[Source,list[Source]] = Source()
    source: Source = Source()
    target: Target = Target()
    transform: Optional[Union[TransformType, list[TransformType]]] = None  # type: ignore
    children: Union[dict[str, Optional["Config"]], list[str], str, None] = None

    @property
    def transform_list(self) -> list[Transform]:
        if isinstance(self.transform, list):
            return self.transform
        else:
            return [self.transform]

    @property
    def transforms_affect_target_count(self) -> bool:
        split_transform_count = 0
        for t in self.transform_list:
            if isinstance(t, SplitOnColumn):
                split_transform_count += 1
        if split_transform_count > 1:
            raise Exception("More then one split per source table not supported")
        elif split_transform_count == 1:
            return True
        else:
            return False

    @property
    def transforms_to_determine_target(self) -> list[Transform]:
        res = []
        for t in reversed(self.transform_list):
            if isinstance(t, SplitOnColumn) or res:
                res.append(t)
        res = list(reversed(res))
        return res

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

    def merge_with(self, config: "Config", in_place=False):
        merged = merge_configs(self, config)
        if in_place:
            self = merged
            return self
        return merged


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
