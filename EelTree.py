import os
import yaml
from openpyxl import load_workbook
import copy 

import EelIngest as ei

from joblib import Parallel, delayed
import multiprocessing

def parallel_str(load_parallel):
    return 'parallel' if load_parallel else 'serial'

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

def find_replace_values(dictionary, find_replace_dict):
    for key, value in dictionary.items():
        if isinstance(value, dict):
            find_replace_values(dictionary[key], find_replace_dict)
        elif value in find_replace_dict:
            dictionary[key] = find_replace_dict[value]

class GenericNode:
    def __init__(self, name, parent=None, config=None):
        self.name = name
        self.parent = parent
        self.config_explicit = config
        self.config_override = {}
        
    def get_file_path(self, absolute=False):
        path_parts = []
        node = self
        while node is not None:
            path_parts.insert(0, node.name)
            node = node.parent
        if not absolute:
            path_parts[0] = '.'
        file_path = os.path.normpath("/".join(path_parts))
        return file_path
    
    def apply_special_attributes_configs(self, config):
        file_extension = self.file_extension
        if self.parent is None:
            file_base = os.path.basename(self.name)
        else:
            file_base = self.name
            # if file_extension:
            #     file_base = file_base.replace('.'+file_extension,'')
        if isinstance(self,FilePart):
            file_path = self.parent.get_file_path(absolute=True)
        else:
            file_path = self.get_file_path(absolute=True)
        find_replace = {'_file_system_base':file_base, '_file_extension':file_extension,'_file_path':file_path}
        find_replace_values(config,find_replace)

    # def apply_flow_attributes_config(self):
    #     if self.has_children and self.is_homog:
    #         if get_dict_item(self.config, ['target', 'if_exists']) == 'replace':
    #             override_append={'target':{'if_exists':'append'}}

    @property
    def has_children(self):
        if isinstance(self,BranchNodeMixin) and len(self.children) > 0:
            return True
        else:
            return False

    @property
    def config_inherited(self):
        if self.parent is None:
            return {}
        else:
            return self.parent.config_combined
    
    @property
    def config_combined(self):
        config_inherited=copy.deepcopy(self.config_inherited)
        if self.config_explicit is None:
            config_combined = merge_dicts_by_top_level_keys(config_inherited,self.config_override)
        else:
            config_combined = merge_dicts_by_top_level_keys(config_inherited,self.config_explicit,self.config_override)
        return config_combined
    
    # def set_config_override(self,config_override):
    #     self.config_override = config_override

    @property
    def config(self):
        config=copy.deepcopy(self.config_combined)
        # config=self.config_combined
        self.apply_special_attributes_configs(config)
        # self.apply_flow_attributes_config(config)
        return config

    @property
    def file_extension(self):
        node = self
        file_extension = None
        while not isinstance(node, Folder):
            if isinstance(node, File):
                file_extension =  node.name.split(".")[-1]
                break
            else:
                node = node.parent
        return file_extension
    
    @property
    def file_base_name(self):
        node = self
        file_base_name = None
        while not isinstance(node, Folder):
            if isinstance(node, File):
                file_base_name, _ =  os.path.splitext(node.name)
                break
            else:
                node = node.parent
        return file_base_name
        

    @property
    def siblings(self):
        return self.parent.children  

class LeafNodeMixin:
    def __init__(self):
        # self.tree_part = TreePart.LEAF
        pass

    def display(self):
        file_path = self.get_file_path() 
        # if file_path == 'export_g2n_veeva_mx_w_prods.tsv\\0':
        print(file_path, self.config)

    @property
    def type(self):
        return get_dict_item(self.config, ['source', 'type'])
    
    @property
    def table(self):
        return get_dict_item(self.config, ['target', 'table'])
    
    @property
    def exec(self):
        return get_dict_item(self.config, ['source', 'load_parallel'])
    
    

class BranchNodeMixin:
    def __init__(self):
        # self.tree_part = TreePart.BRANCH
        self.children = {} 

    def display(self):
        LeafNodeMixin.display(self)
        for child in self.children.values():
            child.display()

    def add_child(self, child_node):
        self.children[child_node.name] = child_node  

    def remove_child(self, child_node):
        if child_node.name in self.children:
            del self.children[child_node.name]  

    @property
    def all_leafs(self):
        for child in self.children.values():
            if isinstance( child, LeafNodeMixin):
                yield child
            else:
                yield from child.all_leafs

    @property
    def leaf_count(self):
        count = 0
        for leaf in self.all_leafs:
            count+=1
        return count

    @property
    def is_homog_table(self):
        last_target = None
        for leaf in self.all_leafs:
            this_target = leaf.table
            if (not last_target is None) and last_target != this_target:
                return False
            last_target = leaf.table
        return True

    @property
    def is_homog_exec(self):
        last_target = None
        for leaf in self.all_leafs:
            this_target = leaf.exec
            if (not last_target is None) and last_target != this_target:
                return False
            last_target = leaf.exec
        return True


    @property
    def load_parallel(self):
        return get_dict_item(self.config, ['source', 'load_parallel'], False)
    
    @property
    def taskflow_container(self):
        result = []
        if isinstance(self,BranchNodeMixin) and self.is_homog_table and self.leaf_count > 0:
            for child in self.all_leafs:
                item_res = child.get_file_path()
                result.append(item_res)
                if len(result) > 1:
                    child.config_override={'target':{'if_exists':'append'}}
        else:        
            for child in self.children.values():
                if isinstance(child,BranchNodeMixin) and child.leaf_count > 0:
                    parallel = parallel_str(child.load_parallel)
                    gchildren = child.taskflow_container
                    item_res = (parallel, gchildren)
                    if parallel == 'parallel':
                        serial_starter = gchildren[0]
                        parallel_finish = gchildren[1:]
                        item_res = ('serial', [serial_starter, ('parallel', parallel_finish)])
                    result.append(item_res)

        return result

class Folder(GenericNode, BranchNodeMixin):
    def __init__(self, name, parent=None):
        super().__init__(name, parent)
        BranchNodeMixin.__init__(self)
    
class File(GenericNode, BranchNodeMixin):
    def __init__(self, name, parent=None):
        super().__init__(name, parent)
        BranchNodeMixin.__init__(self)
    
class FilePart(GenericNode, LeafNodeMixin):
    def __init__(self, name, parent):
        super().__init__(name, parent)
        LeafNodeMixin.__init__(self)
        # self.content = content

class EelTree():
    def __init__(self, path, config_ext='.eel.yml'):
        self.config_ext = config_ext
        self.populate_tree(path)

    def add_root_node(self, node):
        self.root = node
        self.index = {}
        self.index['.'] = self.root
        return node
    
    def add_node(self, node, parent, config):
        node.config_explicit = config
        if parent is None:
            return self.add_root_node(node)
        else:
            parent.add_child(node)
            self.add_index(node)
            return node
    
    def add_index(self, node):
        path = node.get_file_path()
        if path in self.index:
            print('Warning, duplicate data path found, index will reference most recent: ' + path)
        self.index[path] = node

    def remove_node(self, node):
        if node.parent_folder:
            node.parent_folder.remove_child(node)
        else:
            self.root.remove_child(node)
        del self.index[node.get_file_path()]

    def get_node_by_path(self, file_path):
        return self.index.get(file_path)

    def display_tree(self):
        self.root.display()

    def populate_tree(self, basepath, parent=None):
        if os.path.isdir(basepath):
            config = self.get_paired_config_doc(basepath)
            folder = self.add_folder(basepath, parent, config)
            skip_configs = [self.config_ext]
            for item in os.listdir(basepath):
                if not item.endswith(self.config_ext):
                    skip_configs.append(item + self.config_ext)
                    file_path = os.path.join(basepath, item)
                    self.populate_tree( file_path, parent=folder)
                elif not item in skip_configs:
                    #TODO complete
                    print('sole ymls not covered')
                    print(item)
        else:
            config = self.get_paired_config_doc(basepath)
            file = self.add_file(basepath, parent, config)
            file_path = file.get_file_path(absolute=True)
            if file.file_extension == 'xlsx':
                worksheets = get_worksheet_names(file_path)
                for ws in worksheets:
                    config = self.get_paired_config_doc(basepath,ws)
                    self.add_file_part(ws, file,config)
            else:
                self.add_file_part(file.file_base_name,file)

    def get_paired_config_doc(self,item_path,sub_path=None):
        config_path = self.get_paired_config_file(item_path)
        if config_path:
            documents = get_yml_docs(config_path)
            if sub_path is None:
                return documents[0] #TODO always assumes first doc is scoped to container (file/folder)
            else:
                for doc in documents:
                    if 'sub-path' in doc:
                        if sub_path == doc['sub-path']:
                            return doc                

    def get_paired_config_file(self,item_path):
        if os.path.isdir(item_path):
            config_path = os.path.join(item_path,self.config_ext)
        elif os.path.isfile(item_path):
            if item_path.endswith(self.config_ext):
                return item_path
            else:
                config_path = item_path + self.config_ext
        if os.path.exists(config_path):
            return config_path
        return None

    def add_folder(self, name, parent=None, config=None):
        if not parent is None:
            name = os.path.basename(name)
        folder = Folder(name, parent)
        return self.add_node(folder, parent, config)

    def add_file(self, name, parent=None, config=None):
        if not parent is None:
            name = os.path.basename(name)
        file = File(name, parent)
        return self.add_node(file, parent, config)

    def add_file_part(self, name, parent, config=None):
        file_part = FilePart(name, parent)
        return self.add_node(file_part, parent, config)

    @property
    def taskflow(self):
        return ('parallel',self.root.taskflow_container)
    
    def process_task(self, task):
        if isinstance(task, tuple):
            parallelizable = True if task[0] == 'parallel' else False
            subtasks = task[1]
            if parallelizable:
                cores = min(self.num_cores,len(subtasks))
                Parallel(n_jobs=cores)(
                    delayed(self.process_task)(t) for t in subtasks)
            else:
                for t in subtasks:
                    self.process_task(t)
        else:
            task_node = self.get_node_by_path(task)
            task_config = task_node.config
            ei.ingest(task_config)

    def process_tasks(self):
        self.num_cores = multiprocessing.cpu_count()
        self.process_task(self.taskflow)

def get_worksheet_names(file_path):
    workbook = load_workbook(filename=file_path, read_only=True)
    worksheet_names = workbook.sheetnames
    workbook.close()
    return worksheet_names

def get_yml_docs(file_path):
    with open(file_path, 'r') as file:
        yaml_text = file.read()
        documents = list(yaml.safe_load_all(yaml_text))
        return documents


root_path = 'D:\\test_data'
ft = EelTree(root_path)
# populate_tree(ft, root_path)

# for leaf in ft.root.all_leafs:
#     print(leaf)

# print('ft.get_taskflow()')
print(ft.taskflow)
ft.process_tasks()

# ft.display_tree()
