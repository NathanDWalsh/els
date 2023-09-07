def merge_configs(*configs: list[Union[ec.Config, dict]]) -> ec.Config:
    dicts = []
    for config in configs:
        if isinstance(config, ec.Config):
            dicts.append(config.model_dump())
        elif isinstance(config, dict):
            dicts.append(config)
        else:
            raise Exception(
                "merge_configs: configs should be a list of Configs or dicts"
            )
    dict_result = merge_dicts_by_top_level_keys(*dicts)
    res = ec.Config(**dict_result)
    return res


def get_dict_item(dict_: dict, item_path: list, default=None):
    current_key = item_path[0]
    if current_key in dict_:
        next_dict = dict_[current_key]
        remaining_path = item_path[1:]
        if remaining_path:
            return get_dict_item(next_dict, remaining_path, default)
        else:
            return next_dict
    else:
        return default
