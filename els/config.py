from __future__ import annotations

import io
import os
import re
import sys
from abc import ABC, abstractmethod
from copy import deepcopy
from enum import Enum
from functools import cached_property
from typing import Literal, Optional, Union
from urllib.parse import urlparse

import duckdb
import pandas as pd
import prqlc
import yaml
from pydantic import BaseModel, ConfigDict

from els.pathprops import HumanPathPropertiesMixin

if sys.version_info >= (3, 10):
    from typing import TypeAlias


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


class ToSQL(BaseModel, extra="allow"):
    chunksize: Optional[int] = None


class ToCSV(BaseModel, extra="allow"):
    pass


class ToXML(BaseModel, extra="allow"):
    pass


class ToExcel(BaseModel, extra="allow"):
    pass


class Transform(BaseModel, ABC, extra="forbid"):
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

    def __call__(
        self,
        df: pd.DataFrame,
        mark_as_executed: bool = True,
    ) -> pd.DataFrame:
        if df.empty:
            raise Exception("Trying to transform an empty dataframe")
        res = self.transform(df)
        self.executed = mark_as_executed
        return res

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

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

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Define the primary column headers based on the first columns
        primary_headers = list(df.columns[: self.stack_fixed_columns])

        # Extract the top-level column names from the primary headers
        top_level_headers, _ = zip(*primary_headers)

        # Set the DataFrame's index to the primary headers
        df = df.set_index(primary_headers)

        # Get the names of the newly set indices
        current_index_names = list(df.index.names[: self.stack_fixed_columns])

        # Create a dictionary to map the current index names to the top-level headers
        index_name_mapping = dict(zip(current_index_names, top_level_headers))

        # Rename the indices using the created mapping
        df.index.rename(index_name_mapping, inplace=True)

        # Stack the DataFrame based on the top-level columns
        df = df.stack(level=self.stack_header, future_stack=True)  # type: ignore

        # Rename the new index created by the stacking operation
        df.index.rename({None: self.stack_name}, inplace=True)

        # Reset the index for the resulting DataFrame
        df.reset_index(inplace=True)

        return df


class Melt(Transform):
    melt_id_vars: list[str]
    melt_value_vars: Optional[list[str]] = None
    melt_value_name: str = "value"
    melt_var_name: str = "variable"

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return pd.melt(
            df,
            id_vars=self.melt_id_vars,
            value_vars=self.melt_value_vars,
            value_name=self.melt_value_name,
            var_name=self.melt_var_name,
        )


class Pivot(Transform):
    pivot_columns: Optional[Union[str, list[str]]] = None
    pivot_values: Optional[Union[str, list[str]]] = None
    pivot_index: Optional[Union[str, list[str]]] = None

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        res = df.pivot(
            columns=self.pivot_columns,
            values=self.pivot_values,
            index=self.pivot_index,
        )
        res.columns.name = None
        res.index.name = None
        return res


class AsType(Transform):
    as_dtypes: dict[str, str]

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.astype(self.as_dtypes)


class AddColumns(Transform, extra="allow"):
    additionalProperties: Optional[  # type: ignore
        Union[DynamicPathValue, DynamicColumnValue, DynamicCellValue, str, int, float]  # type: ignore
    ] = None

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        model_dump = self.model_dump(exclude={"additionalProperties"})
        for k, v in model_dump.items():
            if v != DynamicColumnValue.ROW_INDEX.value:
                df[k] = v
        return df


class PrqlTransform(Transform):
    prql: str

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if os.path.isfile(self.prql):
            with io.open(self.prql) as file:
                prql = file.read()
        else:
            prql = self.prql
        prqlo = prqlc.CompileOptions(target="sql.duckdb")
        dsql = prqlc.compile(prql, options=prqlo)
        df = duckdb.sql(dsql).df()
        return df


class FilterTransform(Transform):
    filter: str

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.query(self.filter)


class SplitOnColumn(Transform):
    split_on_column: str

    def transform(self, df: pd.DataFrame) -> list[str]:  # type: ignore
        return list(df[self.split_on_column].drop_duplicates())


def merge_configs(*configs: Union[Config, dict]) -> Config:
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

    url: Optional[str] = None
    # type: ignore
    # Optional[str] = None
    # server: Optional[str] = None
    # database: Optional[str] = None
    dbschema: Optional[str] = None
    # table: Optional[str] = "_" + HumanPathPropertiesMixin.leaf_name.fget.__name__
    table: Optional[Union[str, list[str]]] = None

    @cached_property
    def type(self):
        if self.url_scheme == "file":
            ext = os.path.splitext(self.url)[-1]
            if ext == (".txt"):
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
    _if_exists_map = dict(
        fail=("append", "fail"),
        truncate=("append", "truncate"),
        append=("append", "append"),
        replace=("append", "replace"),
        replace_file=("replace", "append"),
        replace_database=("replace", "append"),
    )
    model_config = ConfigDict(
        extra="forbid",
        use_enum_values=True,
        validate_default=True,
        json_schema_extra={
            "oneOf": [
                {"required": ["to_sql"]},
                {"required": ["to_csv"]},
                {"required": ["to_excel"]},
                {"required": ["to_xml"]},
            ]
        },
    )
    consistency: Literal[
        "strict",
        "ignore",
    ] = "strict"
    if_exists: Optional[
        Literal[
            "fail",
            "truncate",
            "append",
            "replace",
            "replace_file",
            "replace_database",
        ]
    ] = None
    to_sql: Optional[ToSQL] = None
    to_csv: Optional[ToCSV] = None
    to_excel: Optional[ToExcel] = None
    to_xml: Optional[ToXML] = None

    @property
    def kw_for_push(self):
        return self.to_sql or self.to_csv or self.to_excel or self.to_xml

    @property
    def kw_for_pull(self):
        to_x = self.to_excel

        kwargs = {}
        if to_x:
            kwargs = to_x.model_dump(exclude_none=True)

        root_kwargs = (
            "nrows",
            "dtype",
            "sheet_name",
            "names",
            "encoding",
            "low_memory",
            "sep",
        )
        for k in root_kwargs:
            if hasattr(self, k) and getattr(self, k):
                kwargs[k] = getattr(self, k)
        # TODO: rethink this for samples, should targets only be sampled?
        if "nrows" not in kwargs:
            kwargs["nrows"] = 100

        if self.type in (".tsv"):
            if "sep" not in kwargs.keys():
                kwargs["sep"] = "\t"
        if self.type in (".csv"):
            if "sep" not in kwargs.keys():
                kwargs["sep"] = ","
        if self.type in (".csv", ".tsv"):
            kwargs["clean_last_column"] = False

        if self.type_is_excel:
            if "startrow" in kwargs:
                startrow = kwargs.pop("startrow")
                if startrow > 0:
                    kwargs["skiprows"] = startrow + 1
        return kwargs

    @property
    def replace_container(self) -> bool:
        if self.if_container_exists == "replace":
            return True
        else:
            return False

    @property
    def if_container_exists(self):
        if self.if_exists:
            return self._if_exists_map[self.if_exists][0]
        else:
            return "append"

    @property
    def if_table_exists(self):
        if self.if_exists:
            return self._if_exists_map[self.if_exists][1]
        else:
            return "fail"


class ReadCSV(BaseModel, extra="allow"):
    encoding: Optional[str] = None
    low_memory: Optional[bool] = None
    sep: Optional[str] = None
    # dtype: Optional[dict] = None


class ReadExcel(BaseModel, extra="allow"):
    sheet_name: Optional[str] = "_" + HumanPathPropertiesMixin.leaf_name.fget.__name__  # type: ignore
    # dtype: Optional[dict] = None
    names: Optional[list] = None


class ReadFWF(BaseModel, extra="allow"):
    names: Optional[list] = None


class ReadSQL(BaseModel, extra="allow"):
    pass


class LAParams(BaseModel):
    line_overlap: Optional[float] = None
    char_margin: Optional[float] = None
    line_margin: Optional[float] = None
    word_margin: Optional[float] = None
    boxes_flow: Optional[float] = None
    detect_vertical: Optional[bool] = None
    all_texts: Optional[bool] = None


class ReadPDF(BaseModel):
    password: Optional[str] = None
    page_numbers: Optional[Union[int, list[int], str]] = None
    maxpages: Optional[int] = None
    caching: Optional[bool] = None
    laparams: Optional[LAParams] = None


class ReadXML(BaseModel, extra="allow"):
    pass


class Source(Frame, extra="forbid"):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "oneOf": [
                {"required": ["read_csv"]},
                {"required": ["read_excel"]},
                {"required": ["read_sql"]},
                {"required": ["read_fwf"]},
                {"required": ["read_xml"]},
                {"required": ["read_pdf"]},
            ]
        },
    )
    load_parallel: bool = False
    nrows: Optional[int] = None
    dtype: Optional[dict] = None
    read_csv: Optional[Union[ReadCSV, list[ReadCSV]]] = None
    read_excel: Optional[Union[ReadExcel, list[ReadExcel]]] = None
    read_sql: Optional[Union[ReadSQL, list[ReadSQL]]] = None
    read_fwf: Optional[Union[ReadFWF, list[ReadFWF]]] = None
    read_xml: Optional[Union[ReadXML, list[ReadXML]]] = None
    read_pdf: Optional[Union[ReadPDF, list[ReadPDF]]] = None

    @property
    def read_x(self):
        return (
            self.read_csv
            or self.read_excel
            or self.read_sql
            or self.read_fwf
            or self.read_xml
            or self.read_pdf
        )

    @read_x.setter
    def read_x(self, x):
        if self.read_csv:
            self.read_csv = x
        elif self.read_excel:
            self.read_excel = x
        elif self.read_sql:
            self.read_sql = x
        elif self.read_fwf:
            self.read_fwf = x
        elif self.read_xml:
            self.read_xml = x
        elif self.read_pdf:
            self.read_pdf = x

    @property
    def kw_for_pull(self):
        if self.read_pdf:
            return self.read_pdf.model_dump(exclude_none=True)

        kwargs = {}
        if self.read_x:
            kwargs = self.read_x.model_dump(exclude_none=True)

        for k, v in kwargs.items():
            if v == "None":
                kwargs[k] = None

        root_kwargs = (
            "nrows",
            "dtype",
            "sheet_name",
            "names",
            "encoding",
            "low_memory",
            "sep",
        )
        for k in root_kwargs:
            if hasattr(self, k) and getattr(self, k):
                if k == "dtype":
                    dtypes = getattr(self, "dtype")
                    kwargs["dtype"] = {k: v for k, v in dtypes.items() if v != "date"}
                else:
                    kwargs[k] = getattr(self, k)

        if self.nrows:
            kwargs["nrows"] = self.nrows

        if self.type in (".tsv"):
            if "sep" not in kwargs.keys():
                kwargs["sep"] = "\t"
        if self.type in (".csv"):
            if "sep" not in kwargs.keys():
                kwargs["sep"] = ","
        if self.type in (".csv", ".tsv"):
            kwargs["clean_last_column"] = True

        return kwargs


TransformType_ = Union[
    SplitOnColumn,
    FilterTransform,
    PrqlTransform,
    Pivot,
    AsType,
    Melt,
    StackDynamic,
    AddColumns,
]
if sys.version_info >= (3, 10):
    TransformType: TypeAlias = TransformType_
else:
    TransformType = TransformType_


class Config(BaseModel):
    # KEEP config_path AROUND JUST IN CASE, can be used when printing yamls for debugging
    config_path: Optional[str] = None
    # source: Union[Source,list[Source]] = Source()
    source: Source = Source()
    target: Target = Target()
    transform: Optional[
        Union[
            TransformType,  # type: ignore
            list[TransformType],  # type: ignore
        ]
    ] = None
    children: Union[dict[str, Optional["Config"]], list[str], str, None] = None

    @property
    def transform_list(self) -> list[TransformType]:
        if isinstance(self.transform, list):
            return self.transform
        else:
            return [self.transform]  # type: ignore

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
    def transforms_to_determine_target(self) -> list[TransformType]:
        res: list = []
        for t in reversed(self.transform_list):
            if isinstance(t, SplitOnColumn) or res:
                res.append(t)
        res = list(reversed(res))
        return res

    def schema_pop_children(s) -> None:
        s["properties"].pop("children")  # type: ignore

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra=schema_pop_children,  # type: ignore
    )

    @cached_property
    def nrows(self) -> Optional[int]:
        if self.target:
            res = self.source.nrows
        else:
            res = 100
        return res

    # @cached_property
    # def pipe_id(self) -> Optional[str]:
    #     if self.source and self.source.address and self.target and self.target.address:
    #         res = (self.source.address, self.target.address)
    #     elif self.source and self.source.address:
    #         res = (self.source.address,)
    #     elif self.target and self.target.address:
    #         res = (self.target.address,)
    #     else:
    #         res = None
    #     return res

    def merge_with(
        self,
        config: Config,
        in_place: bool = False,
    ):
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
