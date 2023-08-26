from pydantic import BaseModel, constr
from typing import Optional
import sqlalchemy as sa


class ToSql(BaseModel):
    chunksize: Optional[int] = None


class Frame(BaseModel):
    type: str = None
    server: str = None
    database: str = None
    dbschema: str = None
    table: str = None
    file_path: str = None


class Target(Frame):
    consistency: constr(pattern="^(strict|ignore)$") = None
    if_exists: constr(pattern="^(fail|replace|append|truncate)$") = None
    to_sql: to_sql = None

    @property
    def db_connection_string(self) -> str:
        # Define the connection string based on the database type
        if self.type == "mssql":
            res = f"mssql+pyodbc://localhost/bitt?driver=ODBC+Driver+17+for+SQL+Server"
        elif self.type == "postgres":
            res = (
                f"Driver={{PostgreSQL}};Server={self.server};Database={self.database};"
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
        if not self.table_exists or self.if_exists == "replace":
            res = "create_replace"
        elif self.if_exists == "truncate":
            res = "truncate"
        elif self.if_exists == "fail":
            res = "fail"
        else:
            res = "no_action"
        return res


class ReadCsv(BaseModel):
    encoding: Optional[str] = None
    low_memory: Optional[bool] = None
    sep: Optional[str] = None


class ReadExcel(BaseModel):
    sheet_name: Optional[str] = None
    dtype: Optional[dict] = None
    names: Optional[list] = None


class Source(Frame):
    load_parallel: bool = None
    nrows: int = None
    read_csv: ReadCsv = None
    read_excel: ReadExcel = None


class AddColumns(BaseModel):
    row_index: str = None


class Config(BaseModel):
    sub_path: str = "."
    target: Target = None
    source: Source = None
    add_cols: AddColumns = None


def del_nones_from_base(base: BaseModel) -> dict:
    base_dict = base.model_dump()
    return del_nones_from_dict(base_dict)


def del_nones_from_dict(base_dict: dict) -> dict:
    return {k: v for k, v in base_dict.items() if v is not None}


def del_nones_from_dict_recursive(base_dict: dict) -> dict:
    res = {k: v for k, v in base_dict.items() if v is not None}
    for k, v in res.items():
        if isinstance(v, dict):
            res[k] = del_nones_from_dict_recursive(v)
    return res


def deep_merge(base: BaseModel, update: BaseModel) -> BaseModel:
    # model_dump triggers some Pydantic serializer warnings
    base_dict = base.model_dump(warnings=False)
    update_dict = del_nones_from_base(update)

    for k, v in update_dict.items():
        if isinstance(v, dict) and k in base_dict and isinstance(base_dict[k], dict):
            v = del_nones_from_dict(v)
            base_dict[k] = del_nones_from_dict(base_dict[k])
            base_nested_model = base.model_fields[k].annotation(**base_dict[k])
            update_nested_model = base.model_fields[k].annotation(**v)
            base_dict[k] = deep_merge(base_nested_model, update_nested_model)
        else:
            base_dict[k] = v
    res = base.model_copy(update=base_dict)
    return res
