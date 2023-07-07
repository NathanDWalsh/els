import os
import yaml

folder_config='.eel.yml'
file_config='.eel.yml'

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

def is_file_of_types(file_path, file_types):
    ext = os.path.splitext(file_path)[1][1:]
    return ext.lower() in file_types

def process_folder(path):
    result = {}

    result[path] = read_yaml_file(os.path.join(path,folder_config))

    for item in os.listdir(path):
        item_path = os.path.join(path, item)
        parent_path = get_parent_path(item_path)

        if os.path.isdir(item_path):
            result[item_path] = result[parent_path] #repeated x times
            folder_yaml = read_yaml_file(os.path.join(item_path,folder_config))
            if folder_yaml:
                result[item_path].update(folder_yaml)
                #= merge_yaml_objects(result[item_path] ,folder_yaml)
            
        elif os.path.isfile(item_path):
            if item.endswith(file_config):
                result[item_path] = result[parent_path]#repeated x times
                file_yaml = read_yaml_file(item_path)
                result[item_path].update(file_yaml)
            elif is_file_of_types(item,file_types):
                result[item_path] = result[parent_path]#repeated x times
                file_yaml = read_yaml_file(item_path + file_config)
                if file_yaml:
                    result[item_path].update(file_yaml)
    return result

folder_path = "D:\\Sync\\repos\\eel"
file_types = ['csv','tsv','xlsx','xlsm','xlsb','xls']
result = process_folder(folder_path)
print(result.keys())
