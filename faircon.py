import os
import yaml
import copy

def read_yaml_file(file_path):
    if not os.path.isfile(file_path):
        return False

    with open(file_path) as f:
        yaml_data = yaml.safe_load(f)

    return yaml_data

def merge_yaml_objects(yaml_obj1, yaml_obj2):
    merged_data = {**yaml_obj1, **yaml_obj2}
    return yaml.dump(merged_data)

def get_parent_path(path):
    parent_path = os.path.dirname(path.rstrip('/'))
    return parent_path

def get_file_ext(file_path):
    return os.path.splitext(file_path)[1][1:]

def is_file_of_types(file_path, file_types):
    ext = get_file_ext(file_path)
    return ext.lower() in file_types

def filter_dict(dictionary, type_to_filter):
    filtered_dict = {}

    for key in dictionary.keys():
        if type_to_filter == "file" and os.path.isfile(key):
            filtered_dict[key] = dictionary[key]
        elif type_to_filter == "dir" and os.path.isdir(key):
            filtered_dict[key] = dictionary[key]

    return filtered_dict

def merge_dicts_by_top_level_keys(*dicts):
    merged_dict = {}
    for dict_ in dicts:
        for key, value in dict_.items():
            if key in merged_dict:
                # Perform merge operation on the value of the existing key
                if isinstance(value, dict) and isinstance(merged_dict[key], dict):
                    merged_dict[key].update(value)
            else:
                # Add a new key-value pair to the merged dictionary
                merged_dict[key] = value
    return merged_dict

def inherit_config(config_dict, item_path, parent_path, config_path=None):
    config_dict[item_path] = copy.deepcopy(config_dict[parent_path])
    if not config_path:
        config_path = item_path
    dict_yaml = read_yaml_file(config_path)
    if dict_yaml:
        config_dict[item_path] = merge_dicts_by_top_level_keys(config_dict[item_path],dict_yaml)
        #= merge_yaml_objects(result[item_path] ,folder_yaml)

def recursive_listdir(path):
    result = []
    for root, _, files in os.walk(path):
        result.append(root)
        for file in files:
            file_path = os.path.join(root, file)
            result.append(file_path)
    return result

def process_folder(path,file_types=None,folder_config='.yaml',file_config='.yaml',type_to_filter=None):
    result = {}

    for item in recursive_listdir(path):
        # item_path = os.path.join(path, item)
        if len(result) == 0:
            result[path] = read_yaml_file(os.path.join(item,folder_config))
        else:
            item_path = item
            parent_path = get_parent_path(item_path)

            if os.path.isdir(item_path):
                inherit_config(result,item_path,parent_path,os.path.join(item_path,folder_config))
                print('item_path')
                print(item_path)
                # result[item_path] = copy.deepcopy(result[parent_path]) #repeated x times
                # folder_yaml = read_yaml_file(os.path.join(item_path,folder_config))
                # if folder_yaml:
                #     # result[item_path].update(folder_yaml)
                #     result[item_path] = merge_dicts_by_top_level_keys(result[item_path],folder_yaml)
                #     #= merge_yaml_objects(result[item_path] ,folder_yaml)
            elif os.path.isfile(item_path):
                if item.endswith(file_config) and item != folder_config:
                    if not os.path.exists(os.path.join(path, item.replace(file_config,''))):    
                        inherit_config(result,item_path,parent_path)
                        # result[item_path] = copy.deepcopy(result[parent_path]) #repeated x times
                        # file_yaml = read_yaml_file(item_path)
                        # # result[item_path].update(file_yaml)
                        # result[item_path] = merge_dicts_by_top_level_keys(result[item_path],file_yaml)
                elif is_file_of_types(item,file_types):
                    inherit_config(result,item_path,parent_path,item_path + file_config)
                    # result[item_path] = copy.deepcopy(result[parent_path]) #repeated x times
                    # file_yaml = read_yaml_file(item_path + file_config)
                    # if file_yaml:
                    #     result[item_path] = merge_dicts_by_top_level_keys(result[item_path],file_yaml)
                    #     #result[item_path].update(file_yaml)
    if type_to_filter:
        return filter_dict(result,type_to_filter) 
    else:
        return result
    