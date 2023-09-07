import logging
import pandas as pd

import eel.config as ec
import eel.execute as ee

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
        self, name: str, config: ec.Config = None, execute_fn=ee.ingest
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
            logging.info("EXECUTE FAILED: " + self.name)


class EelFlow(FlowNodeMixin):
    def __init__(self, items: list[FlowNodeMixin], n_jobs) -> None:
        self.items = items
        self.n_jobs = n_jobs

    def execute(self):
        with Parallel(n_jobs=self.n_jobs, backend="loky") as parallel:
            parallel(delayed(t.execute)() for t in self.items)
            get_reusable_executor().shutdown(wait=True)


class BuildWrapperMixin:
    def build_target(self) -> bool:
        build_item = self.eel_flow.items[0]
        if ee.build(build_item.config):
            res = True
        else:
            res = False
            logging.error("BUILD FAILED: " + build_item.name)
            # raise Exception("BUILD FAILED: " + build_item.name)
        return res


class EelCsvWrapper(FlowNodeMixin, BuildWrapperMixin):
    def __init__(self, file_path: str, eel_flow: FlowNodeMixin) -> None:
        self.file_path = file_path
        self.eel_flow = eel_flow

    def execute(self):
        self.eel_flow.execute()


# class EelFileWrapper(FlowNodeMixin, SerialNodeMixin, BuildWrapperMixin):
#     def __init__(self) -> None:
#         super().__init__()


class EelFileWrapper(FlowNodeMixin, SerialNodeMixin, BuildWrapperMixin):
    def __init__(self, file_path: str, eel_flow: FlowNodeMixin) -> None:
        self.file_path = file_path
        self.eel_flow = eel_flow
        self.items = [eel_flow]

    def open(self):
        pass

    def execute(self):
        self.open()
        self.eel_flow.execute()
        self.close()

    def close(self):
        pass


# class EelFileWriteWrapper(EelFileWrapper):
#     def __init__(self, file_path: str, eel_flow: FlowNodeMixin) -> None:
#         super().__init__(file_path, eel_flow)


class EelXlsxWrapper(EelFileWrapper):
    def __init__(self, file_path: str, eel_flow: FlowNodeMixin) -> None:
        super().__init__(file_path, eel_flow)

    def open(self):
        if self.file_path not in ee.open_files:
            logging.info("OPEN: " + self.file_path)
            file = pd.ExcelFile(self.file_path)
            ee.open_files[self.file_path] = file

    def execute(self):
        self.open()
        self.eel_flow.execute()
        self.close()

    def close(self):
        file = ee.open_files[self.file_path]
        file.close()
        del ee.open_files[self.file_path]
        logging.info("CLOSED: " + self.file_path)


class EelFileGroupWrapper(FlowNodeMixin, SerialNodeMixin):
    def __init__(self, files: list[EelFileWrapper], exec_parallel: bool) -> None:
        self.items = files
        self.exec_parallel = exec_parallel

    def execute(self):
        self.items[0].open()
        if self.items[0].build_target():
            n_jobs = len(self.items) if self.exec_parallel else 1
            ingest_files = EelFlow(self.items, n_jobs)
            ingest_files.execute()
        else:
            self.items[0].close()
