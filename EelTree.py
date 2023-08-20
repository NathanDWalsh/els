import os
import zipfile
import yaml
from openpyxl import load_workbook
import logging 
import re
import pandas as pd

from joblib import Parallel, delayed
import multiprocessing
from joblib.externals.loky import get_reusable_executor

import EelIngest as ei
import EelConfig as ec

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



def merge_configs(*configs: ec.Config):
    dicts = []
    for config in configs:
        dicts.append(config.dict())
    dict_result = merge_dicts_by_top_level_keys(*dicts)
    return ec.Config(**dict_result) 

def merge_dicts_by_top_level_keys(*dicts):
    merged_dict = {}
    for dict_ in dicts:
        for key, value in dict_.items():
            if key in merged_dict and isinstance(value, dict) and (not merged_dict[key] is None):
                merged_dict[key].update(value)
            elif not value is None:
                # Add a new key-value pair to the merged dictionary
                merged_dict[key] = value
    return merged_dict

def replace_dict_vals(dictionary, find_replace_dict):
    for key, value in dictionary.items():
        if isinstance(value, dict):
            replace_dict_vals(dictionary[key], find_replace_dict)
        elif isinstance(value, list):
            pass
        elif value in find_replace_dict:
            dictionary[key] = find_replace_dict[value]

class GenericNode:
    def __init__(self, name:str, parent:'GenericNode'=None, config:ec.Config=None):
        self.name = name
        self.parent = parent
        self.config_explicit = config
        
    def get_part_path(self, absolute=False):
        path_parts = []
        node = self
        while node is not None:
            path_parts.insert(0, node.name)
            node = node.parent
        if not absolute:
            path_parts[0] = '.'
        part_path = os.path.normpath("/".join(path_parts))
        return part_path
    
    def apply_special_attributes_configs(self, config: ec.Config):
        config_dict = config.dict()
        file_extension = self.file_extension
        if self.parent is None:
            file_base = os.path.basename(self.name)
        else:
            file_base = self.name
        if isinstance(self,FilePart):
            file_path = self.parent.get_part_path(absolute=True)
        else:
            file_path = self.get_part_path(absolute=True)
        find_replace = {'_file_system_base':file_base, '_file_extension':file_extension,'_file_path':file_path}
        replace_dict_vals(config_dict,find_replace)
        return ec.Config(**config_dict)

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
            return ec.Config()
        else:
            return self.parent.config_combined
    
    @property
    def config_combined(self):
        config_inherited=self.config_inherited.copy(deep=True)
        if self.config_explicit is None:
            return config_inherited
        else:
            # return merge_configs(config_inherited,self.config_explicit)
            return ec.deep_merge(config_inherited,self.config_explicit)
    
    @property
    def config(self):
        config=self.config_combined.copy(deep=True)
        # config=self.config_combined
        config = self.apply_special_attributes_configs(config)
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
    def file_name(self):
        node = self
        file_name = None
        while not isinstance(node, Folder):
            if isinstance(node, File):
                file_name = node.name
                break
            else:
                node = node.parent
        return file_name

    @property
    def siblings(self):
        return self.parent.children  
    
    @property
    def root(self):
        if self.parent is None:
            return self
        else:
            return self.parent.root
        
    @property
    def all_root_leafs(self):
        return self.root.all_leafs

class LeafNodeMixin:
    def __init__(self):
        # self.tree_part = TreePart.LEAF
        pass

    def display(self, item_path=None):
        file_path = self.get_part_path() 
        # if file_path == 'export_g2n_veeva_mx_w_prods.tsv\\0':
        if item_path:
            output = get_dict_item(self.config, item_path)
        else:
            output = self.config.dict()
        print(file_path, output)

    @property
    def type(self):
        return get_dict_item(self.config, ['source', 'type'])
    
    @property
    def table(self):
        return get_dict_item(self.config, ['target', 'table'])
    
    @property
    def load_parallel(self):
        return get_dict_item(self.config, ['source', 'load_parallel'], False)

class BranchNodeMixin:
    def __init__(self):
        # self.tree_part = TreePart.BRANCH
        self.children = {} 

    def display(self, item_path=None):
        LeafNodeMixin.display(self,item_path)
        for child in self.children.values():
            child.display(item_path)

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
    def size(self):
        res = 0
        for leaf in self.all_leafs:
            res += leaf.size
        return res

    @property
    def size_weighted(self):
        res = 0
        for leaf in self.all_leafs:
            res += (leaf.size * len(leaf.siblings))
        return res
    
    @property
    def all_file_count(self):
        res = 0
        for file in self.all_files:
            res += 1
        return res

    @property
    def all_files(self):
        if isinstance(self, File):
            yield self
        else:
            for child in self.children.values():
                if isinstance( child, File):
                    yield child
                else:
                    yield from child.all_files


    @property
    def leaf_count(self):
        count = 0
        for leaf in self.all_leafs:
            count+=1
        return count

    @property
    def leaf_tables_same(self):
        last_target = None
        for leaf in self.all_leafs:
            this_target = leaf.table
            if (not last_target is None) and last_target != this_target:
                return False
            last_target = leaf.table
        return True

    @property
    def leaf_parallels_same(self):
        last_target = None
        for leaf in self.all_leafs:
            this_target = leaf.load_parallel
            if (not last_target is None) and last_target != this_target:
                return False
            last_target = leaf.load_parallel
        return True

    @property
    def first_leaf(self):
        for child in self.children.values():
            if isinstance( child, LeafNodeMixin):
                return child
            else:
                return child.first_leaf

    @property
    def load_parallel(self):
        if self.config and self.config.source:
            return self.config.source.load_parallel
        else:
            return False
        # return get_dict_item(self.config, ['source', 'load_parallel'], False)
    
    @property 
    def par_cores(self):
        if self.load_parallel:
            cpu_cores = os.cpu_count()
            # proportion = self.size_weighted / self.root.size_weighted
            # res = min(max(round(proportion * cpu_cores),1),self.leaf_count)

            res = min(self.leaf_count,cpu_cores)
            # print( self.size_weighted, self.root.size_weighted, self.leaf_count,res,self.name)
            return res
        else:
            return 1
    
    @property
    def children_size_asc(self):
        # Yield the current node's children sorted by size
        for child in sorted(self.children.values(), key=lambda x: x.size, reverse=True):
            yield child

    @property
    def taskflow_container(self):
        result = []
        # isBranchNode =  isinstance(self,BranchNodeMixin)
        if isinstance(self,BranchNodeMixin) and self.leaf_tables_same and self.leaf_count > 0:
            build_target = True
            build_result = []
            for file in self.all_files:
                sub_result = []
                for leaf in file.all_leafs:
                    item_res = leaf.get_part_path()
                    sub_result.append(item_res)
                    # if len(result) > 0 or len(sub_result) > 1:
                    if leaf.config_explicit:
                        leaf.config_explicit.target.if_exists = 'append'
                    # config_override={'target':{'if_exists':'append'}}
                    # leaf.display(['target','if_exists'])
                if file.file_extension == 'xlsx':
                    file_path = file.get_part_path(absolute=True)
                    # parallel = parallel_int(file.load_parallel)
                    parallel = file.par_cores
                    if build_target :
                        # result.append( (1, [file_path, 'build:' + sub_result[0], (parallel, sub_result), file_path ]) )
                        build_result = [ (1,[(parallel, sub_result), file_path]) ]
                        if self.load_parallel:
                            par_files = self.all_file_count
                        else:
                            par_files = 1
                        result.append( (1, [file_path, 'build:' + sub_result[0], (par_files, build_result ) ] ))
                    else:
                        build_result.append( (1, [file_path, (parallel, sub_result), file_path ]) )
                    build_target = False
                else:
                    result += sub_result
        else:        
            for child in self.children.values():
                if isinstance(child,BranchNodeMixin) and child.leaf_count > 0:
                    # parallel = parallel_int(child.load_parallel)
                    parallel = child.par_cores
                    gchildren = child.taskflow_container
                    # item_res = (parallel, gchildren)
                    item_res = (len(gchildren), gchildren)
                    # if parallel == 'parallel':
                    if parallel>1 and len(gchildren) > 1 and child.leaf_tables_same and child.leaf_parallels_same and child.first_leaf.load_parallel:
                        serial_starter = gchildren[0]
                        parallel_body = gchildren[1:]
                        item_res = (1, [serial_starter, (parallel-1, parallel_body)])
                    result.append(item_res)
        # result[0][0] = len(result[0][1])
        # if result[0][0] == 1 and len(result[0][1]) == 1:
        #     result = result[1]
        # if isinstance(result[0],tuple) and result[0][0] == 1 and len(result[0][1]) == 1:
        #     print(result)
        #     print(result[0][0])
        #     print(len(result[0][1]))
        return result

class Folder(GenericNode, BranchNodeMixin):
    def __init__(self, name:str, parent:BranchNodeMixin=None, config:ec.Config=None):
        super().__init__(name, parent, config)
        BranchNodeMixin.__init__(self)
    
class File(GenericNode, BranchNodeMixin):
    def __init__(self, name:str, parent:Folder=None, config:ec.Config=None):
        super().__init__(name, parent, config)
        BranchNodeMixin.__init__(self)
    
class FilePart(GenericNode, LeafNodeMixin):
    def __init__(self, name:str, parent:File, config:ec.Config=None):
        super().__init__(name, parent, config)
        LeafNodeMixin.__init__(self)
        self.size = None
        # self.content = content

def cleanup_worker():
    print("Worker cleanup")

class EelTree():
    def __init__(self, path, config_ext='.eel.yml'):
        self.config_ext = config_ext
        # self.pool = None
        self.populate_tree(path)

    # def initialize_pool(self):
    #     num_processes = multiprocessing.cpu_count()
    #     self.pool = multiprocessing.Pool(processes=num_processes)

    # def close_pool(self):
    #     if self.pool:
    #         self.pool.close()
    #         self.pool.join()

    def add_root_node(self, node):
        self.root = node
        self.index = {}
        self.index['.'] = self.root
        return node
    
    def add_node(self, node: GenericNode, parent: GenericNode) -> GenericNode:
        if parent is None:
            return self.add_root_node(node)
        else:
            parent.add_child(node)
            self.add_index(node)
            return node
    
    def add_index(self, node):
        path = node.get_part_path()
        if path in self.index:
            print('Warning, duplicate data path found, index will reference most recent: ' + path)
        self.index[path] = node

    def remove_node(self, node):
        if node.parent_folder:
            node.parent_folder.remove_child(node)
        else:
            self.root.remove_child(node)
        del self.index[node.get_part_path()]

    def get_node_by_path(self, file_path):
        relative_file_path = file_path.replace(os.path.join(self.root.name,'') ,'')
        return self.index.get(relative_file_path)

    def display_tree(self,item_path=None):
        self.root.display(item_path)

    def populate_tree(self, basepath: str, parent:BranchNodeMixin=None) -> None:
        if os.path.isdir(basepath):
            config = self.get_paired_config(basepath)
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
        elif basepath.find("~$") == -1 : # exclude hidden excel temps, TODO not ideal
            config = self.get_paired_config(basepath)
            file = self.add_file(basepath, parent, config)
            file_path = file.get_part_path(absolute=True)
            if file.file_extension == 'xlsx':
                sheet_deets = get_sheeet_deets(file_path)
                for ws_name, ws_size in sheet_deets.items():
                    config = self.get_paired_config(basepath,ws_name)
                    self.add_file_part(ws_name, file,config, ws_size)
            else:
                file_size = os.path.getsize(file_path)
                self.add_file_part(file.file_base_name,file, size=file_size)

    def get_paired_config(self,item_path : str,sub_path:str='.') -> ec.Config:
        config_path = self.get_paired_config_file(item_path)
        if config_path:
            configs = get_yml_configs(config_path)
            for config in configs:
                if sub_path == config.sub_path:
                    return config
            logging.error('ERROR: config sub path not found')
            return None
        else:
            logging.error("ERROR: config not found")
            return None

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

    def add_folder(self, name:str, parent=None, config:ec.Config=None) -> Folder:
        if not parent is None:
            name = os.path.basename(name)
        folder = Folder(name, parent, config)
        return self.add_node(folder, parent)

    def add_file(self, name:str, parent=None, config:ec.Config=None) -> File:
        if not parent is None:
            name = os.path.basename(name)
        file = File(name, parent, config)
        return self.add_node(file, parent)

    def add_file_part(self, name:str, parent=None, config:ec.Config=None, size:int=None) -> FilePart:
        file_part = FilePart(name, parent, config)
        file_part.size = size
        return self.add_node(file_part, parent)

    @property
    def taskflow(self):
        # parallel = parallel_int(self.root.load_parallel)
        # parallel = len(self.root.children)
        taskflow = self.root.taskflow_container
        if self.root.load_parallel:
            par_cor = len(taskflow)
            # self.reorganize_tasks(taskflow)
        else:
            par_cor = 1
        return (par_cor,taskflow)
    
    # def reorganize_tasks(self, taskflow):
    #     for task in taskflow:
    #         print(task)

    def process_task(self,task):
        logging.basicConfig(level=logging.INFO, format='%(relativeCreated)d - %(levelname)s - %(message)s')
        # reusable_executor = get_reusable_executor()
        # print(reusable_executor._timeout)
        # print('here')
        if isinstance(task, tuple):
            # parallelizable = True if task[0] == 'parallel' else False
            pjobs = task[0]
            subtasks = task[1]
            # if parallelizable:
            #     pjobs = min(self.num_cores,len(subtasks))
            # else:
            #     pjobs = 1
                # Parallel(n_jobs=cores)(
                #     delayed(self.process_task)(t) for t in subtasks)

            # if pjobs == 1:
            #     # prefer = 'threads'
            #     backend = 'threading'
            # else:
            #     # prefer = 'processes'
            #     backend = 'multiprocessing'

            # if pjobs == 6:
            #     pjobs = 3
            # executor = get_reusable_executor()


            with Parallel(n_jobs=pjobs,max_nbytes=None,mmap_mode=None,backend='loky') as parallel:
                # reusable_executor = get_reusable_executor()
                # print (reusable_executor._timeout)
                parallel(delayed(self.process_task)(t) for t in subtasks)
                # parallel._terminate_backend()
                get_reusable_executor().shutdown(wait=True)
                
            # reusable_executor = get_reusable_executor()
            # print (reusable_executor._timeout)
            # reusable_executor._timeout = 3

            # grouped_data = Parallel(n_jobs=14)(delayed(function)(group) for group in grouped_data)
                # for t in subtasks:
                # if self.pool is None:
                #     self.initialize_pool()

                    # self.pool.apply(self.process_task, args=(t,))
                # results = [self.pool.apply_async(self.process_task, args=(task,)) for t in subtasks]
                # self.pool.map(self.process_task, subtasks)

                    # pooled_task.wait()
                    # pooled_task.get()
                    # pooled.append(pooled_task)

                # for task in pooled:
                #     task.get()

                # Parallel(n_jobs=cores)(
                #     delayed(self.process_task)(t) for t in subtasks)
            # else:
            #     for t in subtasks:
            #         self.process_task(t)
        else:
            if task.startswith('build:'):
                build=True
                task = task[6:]
                # print(task)
            else:
                build=False
            
            task_node = self.get_node_by_path(task)
            if os.path.isfile(task) and task_node.file_extension == 'xlsx':
                if not task in ei.open_files:
                    logging.info('OPEN: ' + task)
                    file = pd.ExcelFile(task) 
                    ei.open_files[task]=file
                else:
                    file = ei.open_files[task]
                    file.close()
                    del ei.open_files[task]
                    logging.info('CLOSE: ' + task)
            else:
                task_config = task_node.config.dict()
                if build:
                    logging.info('BUILD: ' + task)
                else:
                    logging.info('INGEST: ' + task)
                if ei.ingest(task_config, build):
                    pass
                    # logging.info('SUCCESS: ' + task)
                else:
                    logging.info('FAILED: ' + task)
        # os._exit(0)

    def process_tasks(self):
        self.num_cores = multiprocessing.cpu_count()
        self.process_task(self.taskflow)

    def detect_dtypes(self):
        self.num_cores = multiprocessing.cpu_count()
        self.detect_dtype(self.taskflow)

    def detect_dtype(self, task):
        if isinstance(task, tuple):
            parallelizable = True if task[0] == 'parallel' else False
            subtasks = task[1]
            if parallelizable:
                cores = min(self.num_cores,len(subtasks))
                
                # current_process = psutil.Process()
                # subproc_before = set([p.pid for p in current_process.children(recursive=True)])
                Parallel(n_jobs=cores)(
                    delayed(self.detect_dtype)(t) for t in subtasks)
                # subproc_after_recursive = set([p.pid for p in current_process.children(recursive=True)])
                # # subproc_after = set([p.pid for p in current_process.children(recursive=False)])
                # # for subproc in (subproc_after_recursive - (subproc_before | subproc_after)):
                # for subproc in (subproc_after_recursive - (subproc_before)):
                #     # print('Killing process with pid {}'.format(subproc))
                #     psutil.Process(subproc).terminate()
                
                
            else:
                for t in subtasks:
                    self.detect_dtype(t)
        else:
            task_node = self.get_node_by_path(task)
            task_config = task_node.config.dict()
            ei.detect(task_config)

def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', s)]

def get_sheet_sizes(file_path):
    sheet_sizes = []

    with zipfile.ZipFile(file_path, 'r') as zip_file:
        sheet_names = zip_file.namelist()

        for sheet_name in sorted(sheet_names, key=natural_sort_key):
            if sheet_name.startswith('xl/worksheets/sheet'):
                file_info = zip_file.getinfo(sheet_name)
                sheet_sizes.append(file_info.file_size)

    return sheet_sizes

def get_sheet_names(file_path):
    workbook = load_workbook(filename=file_path, read_only=True, data_only=True, keep_links=False)
    worksheet_names = workbook.sheetnames
    workbook.close()
    return worksheet_names

def get_sheeet_deets(file_path):
    names = get_sheet_names(file_path)
    sizes = get_sheet_sizes(file_path)
    return dict(zip(names,sizes))

def get_yml_docs(file_path: str) -> list[dict]:
    with open(file_path, 'r') as file:
        yaml_text = file.read()
        documents = list(yaml.safe_load_all(yaml_text))
    return documents
        
def get_configs(ymls: dict) -> list[ec.Config]:
    configs = []
    for yml in ymls:
        config = ec.Config(**yml)
        configs.append(config)
    return configs

def get_yml_configs(file_path: str) -> list[ec.Config]:
    ymls = get_yml_docs(file_path)
    configs = get_configs(ymls)
    return configs

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(relativeCreated)d - %(levelname)s - %(message)s')
    logging.info('Getting Started')

    root_path = 'D:\\test_data3'

    ft = EelTree(root_path)

    # ft.display_tree()
    print('ft.root.first_leaf.config')
    # print(ft.root.first_leaf.config)
    logging.info('Tree Created')
    print(ft.taskflow)

    ft.process_tasks()

    logging.info('Fin')
