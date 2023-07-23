import os
import yaml
import copy 

def get_file_extension(file_path):
    return os.path.splitext(file_path)[1][1:]

def get_file_system_base(path):
    if os.path.isdir(path):
        folder_name = os.path.basename(path)
        return folder_name
    elif os.path.isfile(path):
        # os.path.dirname(path)
        file_full_name = os.path.basename(path)
        file_base_name = os.path.splitext(file_full_name)[0]
        return file_base_name
    else:
        return None

def read_config_file(file_path):
    if not os.path.isfile(file_path):
        return None
    with open(file_path) as f:
        config_data = yaml.safe_load(f)
    return config_data

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
    if config_path:
        dict_config = read_config_file(config_path)
        if dict_config:
            config_dict[item_path] = merge_dicts_by_top_level_keys(config_dict[item_path],dict_config)

def set_configs(file_hierarchy, configs):
    for path,props in file_hierarchy.items():
        if len(configs) == 0:
            configs[path] = read_config_file(props['config_path'])                    
        else:
            inherit_config(configs,path,props['parent'],props['config_path'])
        if props['type'] == 'folder':
            if 'contents' in props:
                set_configs(props['contents'], configs)

def find_replace_values(dictionary, find_replace_dict):
    for key, value in dictionary.items():
        if isinstance(value, dict):
            find_replace_values(dictionary[key], find_replace_dict)
        elif value in find_replace_dict:
            dictionary[key] = find_replace_dict[value]

def apply_special_attributes_configs(configs):
    for file_path, file_config in configs.items():
        file_base = get_file_system_base(file_path)
        file_extension = get_file_extension(file_path)
        find_replace = {'_file_system_base':file_base, '_file_extension':file_extension,'_file_path':file_path}
        find_replace_values(file_config,find_replace)

def get_configs(file_hierachy):
    configs = {}
    set_configs(file_hierachy, configs)
    apply_special_attributes_configs(configs)
    return configs

def get_dict_item(dict_:dict, item_path:list, default=None):

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

def sortkey_path_folder_first(entry):
        return (not os.path.isdir(entry), entry)

def sortkey_path_file_first(entry):
        return (os.path.isdir(entry), entry)

def sort_listdir(paths, folder_first=True):
    if folder_first:
        sorted_dirs = sorted(paths, key=sortkey_path_folder_first )
    else:   
        sorted_dirs = sorted(paths, key=sortkey_path_file_first )
    return sorted_dirs

def get_full_path_listdir(path):
    entries = os.listdir(path)
    full_paths = [os.path.join(path, entry) for entry in entries]
    return full_paths

def get_full_path_listdir_sorted(path, folder_first=True):
    paths = get_full_path_listdir(path)
    paths_sorted = sort_listdir(paths, folder_first)
    standardize_dir_paths(paths_sorted)
    return paths_sorted

def standardize_dir_paths(paths):
    for i in range(len(paths)):
        path = paths[i]
        if os.path.isdir(path):
            paths[i] = os.path.normpath(path)
            # paths[i] = os.path.join(path,'')

class fscon:

    def __init__(self, path,config_extension='.yaml'):
        self.config_extension = config_extension
        self.file_hierarchy = self.get_file_hierarchy(path)
        self.configs = get_configs(self.file_hierarchy)    
 
    def get_file_hierarchy(self, path):
        result = {}
        # path = os.path.join(path,'')

        if not hasattr(fscon.get_file_hierarchy, "in_recursion"):
            fscon.get_file_hierarchy.in_recursion = True
            items = [os.path.normpath(path)]
            parent = None
        else:
            # items = sorted(os.listdir(path)) 
            items = get_full_path_listdir_sorted(path, folder_first=False)
            parent = os.path.normpath(path)

        for path in items:
            path_props = {}
            if os.path.isfile(path):
                if not self.is_paired_config_file(path):
                    path_props['type'] = 'file'
                else:
                    continue
            else: #directory
                path_props['type'] = 'folder'
                path_props['contents'] = self.get_file_hierarchy(path)

            path_props['parent']=parent
            path_props['config_path']=self.get_paired_config_file(path)
            result[path] = path_props
        
        return result

    def set_custom_attributes(self,path,folder_attributes,hierarchy_value):
        for k,v in folder_attributes.items():
            if isinstance(v, not) dict:
                hierarchy_value[k] = v
            else:
                if 'default' in v:
                    default = v['default']
                else:
                    default = None
                dict_path = [path] + v['config_path']
                hierarchy_value[k] = get_dict_item(self.configs, dict_path, default)

    def get_custom_hierarchy(self, file_hierarchy , folder_attributes= {}, file_attributes= {}):

        # def get_eel_containers(file_hierarchy, eel_config):
        hierarchy = {}

        for path,props in file_hierarchy.items():
            path_props = {}
            if props['type'] == 'folder':
                # if 'contents' in props:
                self.set_custom_attributes(path,folder_attributes,path_props)

                folder_dict = self.get_custom_hierarchy(props['contents'],folder_attributes,file_attributes)
                path_props['contents'] = folder_dict

            else:
                self.set_custom_attributes(path,file_attributes,path_props)

            hierarchy[path] = path_props
            #TODO add file containers a la xlsx, possibly consider all ymls to be containers
        return hierarchy
    
    def get_paired_config_file(self,item_path):
        if os.path.isdir(item_path):
            config_path = os.path.join(item_path,self.config_extension)
        elif os.path.isfile(item_path):
            if item_path.endswith(self.config_extension):
                return item_path
            else:
                config_path = item_path + self.config_extension
        if os.path.exists(config_path):
            return config_path
        return None

    def is_paired_config_file(self,item_path):
        if item_path.endswith(self.config_extension) and os.path.isfile(item_path):
            if os.path.exists(item_path.replace(self.config_extension,'')):
                return True
        return False
    
