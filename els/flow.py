import logging
from typing import Callable, Optional

from anytree import NodeMixin, RenderTree
from joblib import Parallel, delayed
from joblib.externals.loky import get_reusable_executor

import els.config as ec
import els.core as el
import els.execute as ee


class FlowNodeMixin(NodeMixin):
    def display_tree(self):
        for pre, fill, node in RenderTree(self):
            print("%s%s" % (pre, node.name))


class SerialNodeMixin:
    @property
    def n_jobs(self):
        return 1


class ElsExecute(FlowNodeMixin):
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
        source_name = config.source.table if execute_fn.__name__ == "ingest" else ""
        target_name = (
            config.target.table + "(" + config.target.type + ")"
            if execute_fn.__name__ == "ingest"
            else ""
        )
        self.name = f"{name} ({execute_fn.__name__}) {source_name} → {target_name}"
        self.config = config
        self.execute_fn = execute_fn

    def execute(self):
        if self.execute_fn(self.config):
            pass
        else:
            logging.info("EXECUTE FAILED: " + self.name)


class ElsFlow(FlowNodeMixin):
    def __init__(self, parent: Optional[FlowNodeMixin] = None, n_jobs: int = 1) -> None:
        self.parent = parent
        self.n_jobs = n_jobs

    def execute(self):
        with Parallel(n_jobs=self.n_jobs, backend="loky") as parallel:
            parallel(delayed(t.execute)() for t in self.children)
            get_reusable_executor().shutdown(wait=True)

    @property
    def name(self):
        if self.is_root:
            return ""
        else:
            return f"flow ({self.n_jobs} jobs)"


class BuildWrapperMixin(FlowNodeMixin):
    def build_target(self) -> bool:
        flow_child = self.children[0]
        build_item = flow_child.children[0]
        if ee.build(build_item.config):
            res = True
        else:
            res = False
            logging.error("BUILD FAILED: " + build_item.name)
        return res


class ElsFileWrapper(BuildWrapperMixin, SerialNodeMixin):
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
        return f"{self.file_path} ({type(self).__name__})"


class ElsXlsxWrapper(ElsFileWrapper):
    def __init__(self, parent: FlowNodeMixin, file_path: str) -> None:
        super().__init__(parent, file_path)

    def open(self):
        if self.file_path not in el.open_workbooks:
            el.fetch_excel_io(self.file_path)

    def execute(self):
        self.open()
        self.children[0].execute()
        self.close()

    def close(self):
        file = el.open_workbooks[self.file_path]
        file.close()
        del el.open_workbooks[self.file_path]


# groups files together that share a common target table so that target can be built once
class ElsTargetTableWrapper(FlowNodeMixin, SerialNodeMixin):
    def __init__(self, parent: FlowNodeMixin, name: str) -> None:
        self.parent = parent
        self.name = f"{name} ({self.__class__.__name__})"

    def execute(self):
        flow_child = self.children[0]
        file_child = flow_child.children[0]
        file_child.open()
        if file_child.build_target():
            # print("TARGET BUILT: " + file_child.name)
            flow_child.execute()
        else:
            file_child.close()
