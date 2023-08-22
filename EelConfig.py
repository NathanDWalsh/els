from pydantic import BaseModel, constr


class ToSql(BaseModel):
    chunksize: int = None


class Target(BaseModel):
    type: str = None
    server: str = None
    database: str = None
    dbschema: str = None
    consistency: constr(pattern="^(strict|ignore)$") = None
    if_exists: constr(pattern="^(fail|replace|append|truncate)$") = None
    to_sql: to_sql = None
    table: str = None


class ReadCsv(BaseModel):
    encoding: str = None
    low_memory: bool = None
    sep: str = None


class ReadExcel(BaseModel):
    sheet_name: str = None
    dtype: dict = None
    names: list = None


class Source(BaseModel):
    load_parallel: bool = None
    nrows: int = None
    type: str = None
    file_path: str = None
    read_csv: ReadCsv = None
    read_excel: ReadExcel = None


class AddColumns(BaseModel):
    row_index: str = None


class Config(BaseModel):
    sub_path: str = "."
    target: Target = None
    source: Source = None
    add_cols: AddColumns = None


def remove_titles_from_schema(schema):
    if "title" in schema:
        del schema["title"]
    for key, value in schema.items():
        if isinstance(value, dict):
            remove_titles_from_schema(value)


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


# import yaml
# json_schema = Config.model_json_schema()
# remove_titles_from_schema(json_schema)
# yaml_schema = yaml.dump(json_schema, default_flow_style=False)
# print(yaml_schema)
# with open('eel_schema.yml', 'w') as file:
#     file.write(yaml_schema)
