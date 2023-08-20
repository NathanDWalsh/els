from pydantic import BaseModel, constr
from typing import Optional, Union

class to_sql(BaseModel):
    chunksize: int = None

class target(BaseModel):
    type: str
    server: str = None
    database: str = None
    dbschema: str = None
    consistency: constr(regex='^(strict|ignore)$') = 'strict'  
    if_exists: constr(regex='^(fail|replace|append|truncate)$') = 'fail'
    to_sql: Optional[to_sql]
    table: str = '_file_system_base'

class read_csv(BaseModel):
    encoding: str = None
    low_memory: bool = False
    sep: str = None

class read_excel(BaseModel):
    sheet_name: str = '_file_system_base'
    dtype: dict = None
    names: list = None

class source(BaseModel):
    load_parallel: bool = False
    nrows: int = None
    type: Union[str, None] = '_file_extension'
    file_path: str = '_file_path'
    read_csv: Optional[read_csv] 
    read_excel: Optional[read_excel]

class add_cols(BaseModel):
    row_index: str = None 

class Config(BaseModel):
    sub_path: str = '.'
    target: Optional[target]
    source: Optional[source] 

def remove_titles_from_schema(schema):
    if "title" in schema:
        del schema["title"]
    for key, value in schema.items():
        if isinstance(value, dict):
            remove_titles_from_schema(value)

def deep_merge(base: BaseModel, update: BaseModel) -> BaseModel:
    base_dict = base.dict()
    update_dict = {k: v for k, v in update.dict().items() if v is not None}

    for k, v in update_dict.items():
        if isinstance(v, dict) and k in base_dict and isinstance(base_dict[k], dict):
            base_nested_model = base.__fields__[k].type_(**base_dict[k])
            update_nested_model = base.__fields__[k].type_(**v)
            base_dict[k] = deep_merge(base_nested_model, update_nested_model)

        else:
            base_dict[k] = v
    res = base.copy(update=base_dict)
    return res

# import yaml

# json_schema = Eel.schema()
# remove_titles_from_schema(json_schema)

# yaml_schema = yaml.dump(json_schema, default_flow_style=False)
# print(yaml_schema)