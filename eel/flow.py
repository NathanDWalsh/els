import logging
import pandas as pd
from anytree import NodeMixin, RenderTree
from typing import Callable

import eel.config as ec
import eel.execute as ee

from joblib import Parallel, delayed
from joblib.externals.loky import get_reusable_executor


class FlowNodeMixin(NodeMixin):
    def display_tree(self):
        for pre, fill, node in RenderTree(self):
            print("%s%s" % (pre, node.name))


class SerialNodeMixin:
    @property
    def n_jobs(self):
        return 1


class EelExecute(FlowNodeMixin):
    def __init__(
        self,
        parent: FlowNodeMixin,
        name: str,
        config: ec.Config,
        execute_fn: Callable = ee.ingest,
    ) -> None:
        if not isinstance(config, ec.Config):
            logging.error("INGEST without config")
        self.parent = parent
        self.name = name
        self.config = config
        self.execute_fn = execute_fn

    def execute(self):
        if self.execute_fn(self.config):
            pass
        else:
            logging.info("EXECUTE FAILED: " + self.name)


class EelFlow(FlowNodeMixin):
    def __init__(self, parent: FlowNodeMixin = None, n_jobs: int = 1) -> None:
        self.parent = parent
        self.n_jobs = n_jobs

    def execute(self):
        with Parallel(n_jobs=self.n_jobs, backend="loky") as parallel:
            parallel(delayed(t.execute)() for t in self.children)
            get_reusable_executor().shutdown(wait=True)

    @property
    def name(self):
        return "Flow"


class BuildWrapperMixin:
    def build_target(self) -> bool:
        flow_child = self.children[0]
        build_item = flow_child.children[0]
        if ee.build(build_item.config):
            res = True
        else:
            res = False
            logging.error("BUILD FAILED: " + build_item.name)
        return res


class EelFileWrapper(FlowNodeMixin, SerialNodeMixin, BuildWrapperMixin):
    def __init__(self, parent: FlowNodeMixin, file_path: str) -> None:
        self.parent = parent
        self.file_path = file_path

    def open(self):
        pass

    def execute(self):
        self.open()
        self.children[0].execute()
        self.close()

    def close(self):
        pass

    @property
    def name(self):
        return self.file_path


class EelXlsxWrapper(EelFileWrapper):
    def __init__(self, parent: FlowNodeMixin, file_path: str) -> None:
        super().__init__(parent, file_path)

    def open(self):
        if self.file_path not in ee.open_files:
            logging.info("OPEN: " + self.file_path)
            file = pd.ExcelFile(self.file_path)
            ee.open_files[self.file_path] = file

    def execute(self):
        self.open()
        self.children[0].execute()
        self.close()

    def close(self):
        file = ee.open_files[self.file_path]
        file.close()
        del ee.open_files[self.file_path]
        logging.info("CLOSED: " + self.file_path)


class EelFileGroupWrapper(FlowNodeMixin, SerialNodeMixin):
    def __init__(self, parent: FlowNodeMixin, exec_parallel: bool) -> None:
        self.parent = parent
        self.exec_parallel = exec_parallel

    def execute(self):
        flow_child = self.children[0]
        file_child = flow_child.children[0]
        file_child.open()
        if file_child.build_target():
            flow_child.execute()
        else:
            file_child.close()

    @property
    def name(self):
        return "FileGroupWrapper"
