from __future__ import annotations

import io
import os
import re
import sys
from abc import ABC, abstractmethod
from enum import Enum
from functools import cached_property
from typing import Any, Literal, Optional, Union
from urllib.parse import urlparse

import duckdb
import pandas as pd
import prqlc
import yaml
from pydantic import BaseModel, ConfigDict, field_serializer
from pydantic.json_schema import SkipJsonSchema

from els.pathprops import HumanPathPropertiesMixin

if sys.version_info >= (3, 10):
    from typing import TypeAlias


from els._typing import IfExistsLiteral, KWArgsIO, listify


def generate_enum_from_properties(
    cls: type[HumanPathPropertiesMixin],
    enum_name: str,
) -> Enum:
    properties: dict[str, str] = {
        name.upper(): "_" + name
        for name, value in vars(cls).items()
        if isinstance(value, property)
        and not getattr(value, "__isabstractmethod__", False)
    }
    return Enum(enum_name, properties)


DynamicPathValue: Enum = generate_enum_from_properties(
    HumanPathPropertiesMixin,  # type:ignore
    "DynamicPathValue",
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


class TransformABC(BaseModel, ABC, extra="forbid"):
    # THIS MAY BE USEFUL FOR CONTROLLING YAML INPUTS?
    # THE CODE BELOW WAS USED WHEN TRANSFORM CLASS HAD PROPERTIES INSTEAD OF A LIST
    # IT ONLY ALLOED EITHER MELT OR STACK TO BE SET (NOT BOTH)
    # model_config = ConfigDict(
    #     extra="forbid",
    #     json_schema_extra={"oneOf": [{"required": ["melt"]}, {"required": ["stack"]}]},
    # )
    def __init__(self, **kwargs: Any):
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
    def executed(self) -> bool:
        return self._executed

    @executed.setter
    def executed(self, v: bool) -> None:
        self._executed = v


class StackDynamicTransform(TransformABC):
    fixed_columns: int
    header: int = 0
    name: str = "stack_column"

    def transform(
        self,
        df: Union[pd.DataFrame, pd.Series[Any]],
    ) -> pd.DataFrame:
        # Define the primary column headers based on the first columns
        primary_headers = list(df.columns[: self.fixed_columns])

        # Extract the top-level column names from the primary headers
        top_level_headers, _ = zip(*primary_headers)

        # Set the DataFrame's index to the primary headers
        df.set_index(primary_headers, inplace=True)

        # Get the names of the newly set indices
        current_index_names = list(df.index.names[: self.fixed_columns])

        # Create a dictionary to map the current index names to the top-level headers
        index_name_mapping = dict(zip(current_index_names, top_level_headers))

        # Rename the indices using the created mapping
        df.index.rename(index_name_mapping, inplace=True)

        # Stack the DataFrame based on the top-level columns
        df = df.stack(level=self.header, future_stack=True)

        # Rename the new index created by the stacking operation
        df.index.rename({None: self.name}, inplace=True)

        # Reset the index for the resulting DataFrame
        df.reset_index(inplace=True)

        if isinstance(df, pd.Series):
            df = pd.DataFrame(df)

        return df


class MeltTransform(TransformABC):
    id_vars: list[str]
    value_vars: Optional[list[str]] = None
    value_name: str = "value"
    var_name: str = "variable"

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return pd.melt(
            df,
            id_vars=self.id_vars,
            value_vars=self.value_vars,
            value_name=self.value_name,
            var_name=self.var_name,
        )


class PivotTransform(TransformABC):
    columns: Optional[Union[str, list[str]]] = None
    values: Optional[Union[str, list[str]]] = None
    index: Optional[Union[str, list[str]]] = None

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        res = df.pivot(
            columns=self.columns,
            values=self.values,
            index=self.index,
        )
        res.columns.name = None
        res.index.name = None
        return res


class AsTypeTransform(TransformABC):
    as_dtypes: dict[str, str]

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.astype(self.as_dtypes)


def fix_additional_properties(s: dict[Any, Any]) -> None:
    # keep enum typehints on an arbatrary number of elements in AddColumns
    # additionalProperties property attribute functions as a placeholder
    s["additionalProperties"] = s["properties"].pop("additionalProperties")


class AddColumnsTransform(TransformABC):
    model_config = ConfigDict(
        extra="allow",
        json_schema_extra=fix_additional_properties,
    )

    additionalProperties: Optional[
        Union[DynamicColumnValue, str, int, float]  # also DynamicPathValue variable
    ] = None

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        model_dump = self.model_dump(exclude={"additionalProperties"})
        for k, v in model_dump.items():
            if v != DynamicColumnValue.ROW_INDEX.value:
                df[k] = v
        return df


class PRQLTransform(TransformABC):
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


class FilterTransform(TransformABC):
    filter: str

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.query(self.filter)


class SplitTransform(TransformABC):
    on_column: str

    def transform(self, df: pd.DataFrame) -> list[str]:  # type:ignore
        return list(df[self.on_column].drop_duplicates())


def merge_configs(*configs: Union[Config, dict[str, Any]]) -> Config:
    dicts: list[dict[str, Any]] = []
    for config in configs:
        if isinstance(config, Config):
            dicts.append(
                config.model_dump(
                    exclude_unset=True,
                    mode="json",
                )
            )
        elif isinstance(config, dict):
            dicts.append(config.copy())
        else:
            raise Exception("configs should be a list of Configs or dicts")
    dict_result = merge_dicts_by_top_level_keys(*dicts)
    res = Config.model_validate(dict_result)
    return res


def merge_dicts_by_top_level_keys(*dicts: dict[str, Any]) -> dict[str, Any]:
    merged_dict: dict[str, Any] = {}
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
    def file_exists(self) -> bool:
        if self.url:
            return os.path.exists(self.url)
        else:
            return False

    url: Optional[str] = None
    # Optional[str] = None
    # server: Optional[str] = None
    # database: Optional[str] = None
    dbschema: Optional[str] = None
    # table: Optional[str] = "_" + HumanPathPropertiesMixin.leaf_name.fget.__name__
    table: Optional[Union[str, list[str]]] = None

    @property
    def table_list(self) -> list[str]:
        # if no source table defined explicitly, assumes to be last element in url
        # (after last / and (before first .))
        if not self.table and self.url:
            return [self.url.split("/")[-1].split(".")[0]]
        else:
            return listify(self.table)

    @cached_property
    def type(self) -> Optional[str]:
        if self.url_scheme == "file":
            assert self.url
            ext = os.path.splitext(self.url)[-1].lower()
            if ext == (".txt"):
                return ".csv"
            else:
                return ext
        else:
            return self.url_scheme

    @cached_property
    def type_is_db(self) -> bool:
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
    def type_is_excel(self) -> bool:
        if self.type in (
            ".xlsx",
            ".xls",
            ".xlsb",
            ".xlsm",
        ):
            return True
        return False

    @cached_property
    def url_scheme(self) -> Optional[str]:
        if self.url:
            url_parse_scheme = urlparse(self.url, scheme="file").scheme
            drive_letter_pattern = re.compile(r"^[a-zA-Z]$")
            if drive_letter_pattern.match(url_parse_scheme):
                return "file"
            return url_parse_scheme.lower()
        else:
            return None

    @cached_property
    def sheet_name(self) -> Optional[str]:
        if self.type_is_excel:
            res = self.table or "Sheet1"
            assert isinstance(res, str)
            res = re.sub(re.compile(r"[\\*?:/\[\]]", re.UNICODE), "_", res)
            return res[:31].strip()
        else:
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
    )
    consistency: Literal[
        "strict",
        "ignore",
    ] = "strict"
    if_exists: Optional[IfExistsLiteral] = None
    write_args: Optional[
        Union[
            ToSQL,
            ToCSV,
            ToExcel,
            ToXML,
        ]
    ] = None

    @property
    def kwargs_push(self) -> KWArgsIO:
        # to_x = self.to_sql or self.to_csv or self.to_excel or self.to_xml
        to_x = self.write_args
        return to_x.model_dump(exclude_none=True) if to_x else {}

    @property
    def kwargs_pull(self) -> KWArgsIO:
        assert self.type

        kwargs = {}
        # ensures same args are applied for read as for write
        if self.write_args:
            kwargs = self.write_args.model_dump(exclude_none=True)

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
    def if_container_exists(self) -> str:
        if self.if_exists:
            return self._if_exists_map[self.if_exists][0]
        else:
            return "append"

    @property
    def if_table_exists(self) -> str:
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
    sheet_name: Optional[str] = "_" + HumanPathPropertiesMixin.leaf_name.fget.__name__  # type:ignore
    # dtype: Optional[dict] = None
    names: Optional[list[str]] = None


class ReadFWF(BaseModel, extra="allow"):
    names: Optional[list[str]] = None


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


class Source(Frame):
    load_parallel: bool = False
    nrows: Optional[int] = None
    dtype: Optional[dict[str, str]] = None
    read_args: Optional[
        Union[
            ReadCSV,
            ReadExcel,
            ReadSQL,
            ReadFWF,
            ReadXML,
            ReadPDF,
            list[ReadCSV],
            list[ReadExcel],
            list[ReadSQL],
            list[ReadFWF],
            list[ReadXML],
            list[ReadPDF],
        ]
    ] = None

    @property
    def kwargs_pull(self) -> KWArgsIO:
        if self.type == ".pdf" and self.read_args:
            assert not isinstance(self.read_args, list)
            return self.read_args.model_dump(exclude_none=True)
        assert self.type
        kwargs = {}
        if self.read_args:
            assert not isinstance(self.read_args, list)
            kwargs = self.read_args.model_dump(exclude_none=True)

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
    SplitTransform,
    FilterTransform,
    PRQLTransform,
    PivotTransform,
    AsTypeTransform,
    MeltTransform,
    StackDynamicTransform,
    AddColumnsTransform,
]
if sys.version_info >= (3, 10):
    TransformType: TypeAlias = TransformType_
else:
    TransformType = TransformType_


class Transform_(BaseModel):
    model_config = ConfigDict(
        # force json schema to only allow one field:
        # json_schema_extra={"oneOf": [{"type":"filter", "split_on_column"]}
        json_schema_extra={
            "oneOf": [
                {"required": ["filter"]},
                {"required": ["split_on_column"]},
                {"required": ["prql"]},
                {"required": ["pivot"]},
                {"required": ["as_type"]},
                {"required": ["melt"]},
                {"required": ["stack_dynamic"]},
                {"required": ["add_columns"]},
            ]
        },
    )
    filter: Optional[FilterTransform] = None
    split_on_column: Optional[SplitTransform] = None
    prql: Optional[PRQLTransform] = None
    pivot: Optional[PivotTransform] = None
    as_type: Optional[AsTypeTransform] = None
    melt: Optional[MeltTransform] = None
    stack_dynamic: Optional[StackDynamicTransform] = None
    add_columns: Optional[AddColumnsTransform] = None

    @property
    def _transform(self) -> TransformType:
        res = (
            self.filter
            or self.split_on_column
            or self.prql
            or self.pivot
            or self.as_type
            or self.melt
            or self.stack_dynamic
            or self.add_columns
        )
        assert res is not None
        return res

    @_transform.setter
    def _transform(self, value: TransformType) -> None:
        if isinstance(value, FilterTransform):
            self.filter = value
        elif isinstance(value, SplitTransform):
            self.split_on_column = value
        elif isinstance(value, PRQLTransform):
            self.prql = value
        elif isinstance(value, PivotTransform):
            self.pivot = value
        elif isinstance(value, AsTypeTransform):
            self.as_type = value
        elif isinstance(value, MeltTransform):
            self.melt = value
        elif isinstance(value, StackDynamicTransform):
            self.stack_dynamic = value
        elif isinstance(value, AddColumnsTransform):
            self.add_columns = value


class Config(BaseModel, extra="forbid"):
    # KEEP config_path AROUND JUST IN CASE, can be used when printing yamls for debugging
    config_path: Optional[str] = None
    # source: Union[Source,list[Source]] = Source()
    source: Source = Source()
    target: Target = Target()
    transforms: Optional[
        list[
            Union[
                Transform_,
                SkipJsonSchema[TransformType],
            ]
        ]
    ] = None

    @property
    def transform_list(self) -> list[TransformType]:
        # if self.transform:
        #     return listify(self.transform)
        if self.transforms:
            res = []
            # raise Exception(self.transforms)
            for d in self.transforms:
                if isinstance(d, TransformABC):
                    res.append(d)
                elif isinstance(d, Transform_):
                    res.append(d._transform)
            return res
        else:
            return []

    @property
    def transforms_vary_target_columns(self) -> bool:
        pivot_count = 0
        for t in self.transform_list:
            if isinstance(t, PivotTransform):
                pivot_count += 1
        # if pivot_count > 1:
        #     raise Exception("More then one pivot per source table not supported")
        if pivot_count == 1:
            return True
        else:
            return False

    @property
    def transforms_affect_target_count(self) -> bool:
        split_count = 0
        for t in self.transform_list:
            if isinstance(t, SplitTransform):
                split_count += 1
        if split_count > 1:
            raise Exception("More then one split per source table not supported")
        elif split_count == 1:
            return True
        else:
            return False

    @property
    def transforms_to_determine_target(self) -> list[TransformType]:
        res: list[TransformType] = []
        for t in reversed(self.transform_list):
            if isinstance(t, SplitTransform) or res:
                res.append(t)
        res = list(reversed(res))
        return res

    @field_serializer("transforms", when_used="json")
    def serialize_transforms(
        self,
        transforms: Optional[
            list[
                Union[
                    Transform_,
                    TransformType,
                ]
            ]
        ],
    ) -> Optional[list[Transform_]]:
        if transforms is None:
            return None
        res = []
        for t in transforms:
            if isinstance(t, Transform_):
                res.append(t)
            else:
                t_ = Transform_()
                t_._transform = t
                res.append(t_)
        return res

    def merge_with(
        self,
        config: Config,
        in_place: bool = False,
    ) -> Config:
        merged = merge_configs(self, config)
        if in_place:
            self = merged
            return self
        return merged


def main() -> None:
    config_json = Config.model_json_schema()
    config_yml = yaml.dump(config_json, default_flow_style=False)

    with open("../els_schema.yml", "w") as file:
        file.write(config_yml)


if __name__ == "__main__":
    main()
