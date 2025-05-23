from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Callable, Optional

from anytree import NodeMixin, RenderTree  # type:ignore
from joblib import Parallel, delayed  # type:ignore
from joblib.externals.loky import get_reusable_executor  # type:ignore

import els.core as el
import els.execute as ee

if TYPE_CHECKING:
    import els.config as ec
    import els.io.base as eio


class FlowNodeMixin(NodeMixin):
    def __getitem__(self, child_index: int) -> FlowNodeMixin:
        return self.children[child_index]

    def display_tree(self) -> None:
        for pre, fill, node in RenderTree(self):
            print("%s%s" % (pre, node.name))

    def execute(self) -> None:
        pass


class SerialNodeMixin:
    @property
    def n_jobs(self) -> int:
        return 1


class ElsExecute(FlowNodeMixin):
    def __init__(
        self,
        parent: FlowNodeMixin,
        name: str,
        config: ec.Config,
        execute_fn: Callable = ee.ingest,
    ) -> None:
        self.parent = parent
        if execute_fn.__qualname__ == ee.ingest.__qualname__:
            source_name = config.source.table
            target_name = f"{config.target.table}({config.target.type})"
        else:
            source_name = ""
            target_name = ""
        self.name = f"{name} ({execute_fn.__name__}) {source_name} → {target_name}"
        self.config = config
        self.execute_fn = execute_fn

    def execute(self) -> None:
        if self.execute_fn(self.config):
            pass
        else:
            logging.info("EXECUTE FAILED: " + self.name)


class ElsFlow(FlowNodeMixin):
    def __init__(self, parent: Optional[FlowNodeMixin] = None, n_jobs: int = 1) -> None:
        self.parent = parent
        self.n_jobs = n_jobs

    def execute(self) -> None:
        with Parallel(n_jobs=self.n_jobs, backend="loky") as parallel:
            parallel(delayed(t.execute)() for t in self)
            get_reusable_executor().shutdown(wait=True)

    @property
    def name(self) -> str:
        if self.is_root:
            return "FlowRoot"
        else:
            return f"flow ({self.n_jobs} jobs)"


class BuildWrapperMixin(FlowNodeMixin):
    def build_target(self) -> bool:
        flow_child = self[0]
        build_item: ElsExecute = flow_child[0]
        if ee.build(build_item.config):
            res = True
        else:
            res = False
            logging.error("BUILD FAILED: " + build_item.name)
        return res


class ElsContainerWrapper(BuildWrapperMixin, SerialNodeMixin):
    def __init__(
        self,
        parent: FlowNodeMixin,
        url: str,
        container_class: type[eio.ContainerWriterABC],
    ) -> None:
        self.parent = parent
        self.url = url
        self.container_class = container_class

    def open(self) -> None:
        el.fetch_df_container(self.container_class, self.url)

    def execute(self) -> None:
        self.open()
        self[0].execute()
        # self.close()

    # def close(self):
    #     file = el.df_containers[self.file_path]
    #     file.close()
    #     del el.df_containers[self.file_path]

    @property
    def name(self) -> str:
        return f"{self.url} ({type(self).__name__})"


# groups files together that share a common target table so that target can be built once
class ElsTargetTableWrapper(FlowNodeMixin, SerialNodeMixin):
    def __init__(self, parent: FlowNodeMixin, name: str) -> None:
        self.parent = parent
        self.name = f"{name} ({self.__class__.__name__})"

    def execute(self) -> None:
        flow_child: ElsExecute = self[0]
        file_child: ElsContainerWrapper = flow_child[0]
        file_child.open()
        if file_child.build_target():
            flow_child.execute()
        # else:
        #     file_child.close()
