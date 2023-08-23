import logging
import pandas as pd

import EelConfig as ec
import EelIngest as ei

from joblib import Parallel, delayed
from joblib.externals.loky import get_reusable_executor


class FlowNodeMixin:
    @property
    def to_tuple(self):
        # Check each item in task_list; if it's MyClass, get its tuple representation
        processed_list = [
            item.to_tuple if not isinstance(item, EelExecute) else item.name
            for item in self.items
        ]
        return (self.n_jobs, processed_list)


class SerialNodeMixin:
    @property
    def n_jobs(self):
        return 1


class EelExecute(FlowNodeMixin):
    def __init__(
        self, name: str, config: ec.Config = None, execute_fn=ei.ingest
    ) -> None:
        if config is None:
            logging.error("INGEST without config")
        self.name = name
        self.config = config
        self.execute_fn = execute_fn

    def execute(self):
        if self.execute_fn(self.config):
            pass
        else:
            logging.info(type(self.execute_fn) + " FAILED: " + self.name)


class EelFlow(FlowNodeMixin):
    def __init__(self, items: list[FlowNodeMixin], n_jobs) -> None:
        self.items = items
        self.n_jobs = n_jobs

    def execute(self):
        with Parallel(n_jobs=self.n_jobs, backend="loky") as parallel:
            parallel(delayed(t.execute)() for t in self.items)
            get_reusable_executor().shutdown(wait=True)


class BuildWrapperMixin:
    def build_target(self):
        if ei.build(self.eel_flow.items[0].config):
            pass
        else:
            logging.info("BUILD FAILED: " + self.name)


class EelCsvWrapper(FlowNodeMixin, BuildWrapperMixin):
    def __init__(self, file_path: str, eel_flow: FlowNodeMixin) -> None:
        self.file_path = file_path
        self.eel_flow = eel_flow

    def execute(self):
        self.eel_flow.execute()


class EelXlsxWrapper(FlowNodeMixin, SerialNodeMixin, BuildWrapperMixin):
    def __init__(self, file_path: str, eel_flow: FlowNodeMixin) -> None:
        self.file_path = file_path
        self.eel_flow = eel_flow
        self.items = [self.eel_flow]

    def open(self):
        if self.file_path in ei.open_files:
            # logging.info("File already opened: " + self.file_path)
            pass
        else:
            logging.info("OPEN: " + self.file_path)
            file = pd.ExcelFile(self.file_path)
            ei.open_files[self.file_path] = file

    def execute(self):
        self.open()
        self.eel_flow.execute()
        self.close()

    def close(self):
        file = ei.open_files[self.file_path]
        file.close()
        del ei.open_files[self.file_path]
        logging.info("CLOSED: " + self.file_path)


class EelFileGroupWrapper(FlowNodeMixin, SerialNodeMixin):
    def __init__(self, files: list[EelXlsxWrapper], exec_parallel: bool) -> None:
        self.items = files
        self.exec_parallel = exec_parallel

    def execute(self):
        self.items[0].open()
        self.items[0].build_target()

        n_jobs = len(self.items) if self.exec_parallel else 1
        ingest_files = EelFlow(self.items, n_jobs)
        ingest_files.execute()
