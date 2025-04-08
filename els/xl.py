import io
import os

import pandas as pd
from python_calamine import CalamineWorkbook, SheetTypeEnum, SheetVisibleEnum

import els.config as ec
import els.core as el
import els.pd as epd


def get_sheet_names(
    xl_io: io.BytesIO,
    sheet_states: list = [SheetVisibleEnum.Visible],
) -> list[str]:
    xl_io.seek(0)
    with CalamineWorkbook.from_filelike(xl_io) as workbook:
        worksheet_names = [
            sheet.name
            for sheet in workbook.sheets_metadata
            if (sheet.visible in sheet_states)
            and (sheet.typ == SheetTypeEnum.WorkSheet)
        ]
        return worksheet_names


def get_sheet_height(
    xl_io: io.BytesIO,
    sheet_name: str,
) -> int:
    xl_io.seek(0)
    with CalamineWorkbook.from_filelike(xl_io) as workbook:
        if sheet_name in workbook.sheet_names:
            return workbook.get_sheet_by_name(sheet_name).total_height
        else:
            return None


def get_sheet_row(
    xl_io: io.BytesIO,
    sheet_name: str,
    row_index: int,
) -> list:
    xl_io.seek(0)
    with CalamineWorkbook.from_filelike(xl_io) as workbook:
        if sheet_name in workbook.sheet_names:
            return workbook.get_sheet_by_name(sheet_name).to_python(
                nrows=row_index + 1
            )[-1]
        else:
            return None


class ExcelSheetIO(epd.DataFrameIO):
    def __init__(
        self,
        name,
        parent,
        if_exists="fail",
        mode="s",
        df=pd.DataFrame(),
        startrow=0,
        kw_for_pull={},
        kw_for_push={},
    ):
        super().__init__(
            df=df,
            name=name,
            parent=parent,
            mode=mode,
            if_exists=if_exists,
        )
        self._startrow = startrow
        self.kw_for_pull = kw_for_pull
        self.kw_for_push: ec.ToExcel = kw_for_push

    @property
    def if_sheet_exists(self):
        if self.mode == "a":
            return "overlay"
        elif self.mode == "w":
            return "replace"
        else:
            return None

    @property
    def startrow(self):
        if self.if_exists == "truncate" or self.mode == "w":
            # consider changing 0 to skiprows value if exists?
            # kw_for_push['skiprows']
            # TODO: test skiprow and truncate combinations
            return 0
        else:
            return self._startrow

    @startrow.setter
    def startrow(self, v):
        self._startrow = v

    @property
    def parent(self) -> "ExcelIO":
        return super().parent

    @parent.setter
    def parent(self, v):
        epd.DataFrameIO.parent.fset(self, v)

    def _read(self, kwargs, sample: bool = False):
        if kwargs.get("nrows") and kwargs.get("skipfooter"):
            del kwargs["nrows"]
        if not kwargs:
            kwargs = self.kw_for_pull
        if self.mode in ("r", "s") and self.kw_for_pull != kwargs:
            if "engine" not in kwargs:
                kwargs["engine"] = "calamine"
            if "sheet_name" not in kwargs:
                kwargs["sheet_name"] = self.name
            self.df = pd.read_excel(self.parent.file_io, **kwargs)
            self.kw_for_pull = kwargs


class ExcelIO(epd.DataFrameContainerMixinIO):
    def __init__(self, url, replace=False):
        self.child_class = ExcelSheetIO

        self.url = url
        self.replace = replace

        # load file and sheets
        self.file_io = el.fetch_file_io(self.url, replace=self.replace)
        # TODO: mode will never be write on init?
        self.children = [] if self.mode == "w" else self._children_init()

    @property
    def create_or_replace(self):
        if self.replace or not os.path.isfile(self.url):
            return True
        else:
            return False

    def get_child(self, child_name) -> ExcelSheetIO:
        return super().get_child(child_name)

    @property
    def childrens(self) -> tuple[ExcelSheetIO]:
        return super().children

    @property
    def write_engine(self):
        if self.mode == "a":
            return "openpyxl"
        else:
            return "xlsxwriter"

    def _children_init(self):
        self.file_io.seek(0)
        with CalamineWorkbook.from_filelike(self.file_io) as workbook:
            return [
                ExcelSheetIO(
                    startrow=workbook.get_sheet_by_name(sheet.name).total_height + 1,
                    name=sheet.name,
                    parent=self,
                )
                for sheet in workbook.sheets_metadata
                if (sheet.visible in [SheetVisibleEnum.Visible])
                and (sheet.typ == SheetTypeEnum.WorkSheet)
            ]

    def persist(self):
        if self.mode == "w":
            with pd.ExcelWriter(
                self.file_io, engine=self.write_engine, mode=self.mode
            ) as writer:
                for df_io in self.childrens:
                    df = df_io.df_target
                    to_excel = df_io.kw_for_push
                    if to_excel:
                        kwargs = to_excel.model_dump(exclude_none=True)
                    else:
                        kwargs = {}
                    # TODO integrate better into write method?
                    if isinstance(df.columns, pd.MultiIndex):
                        df = epd.multiindex_to_singleindex(df)
                    df.to_excel(writer, index=False, sheet_name=df_io.name, **kwargs)
                for sheet in writer.sheets.values():
                    sheet.autofit(500)
        elif self.mode == "a":
            sheet_exists = set()
            for df_io in self.childrens:
                if df_io.mode not in ("r", "s"):
                    sheet_exists.add(df_io.if_sheet_exists)
            for sheet_exist in sheet_exists:
                with pd.ExcelWriter(
                    self.file_io,
                    engine=self.write_engine,
                    mode=self.mode,
                    if_sheet_exists=sheet_exist,
                ) as writer:
                    for df_io in self.childrens:
                        if (
                            df_io.mode not in ("r", "s")
                            and df_io.if_sheet_exists == sheet_exist
                        ):
                            df = df_io.df_target
                            to_excel = df_io.kw_for_push
                            if df_io.mode == "a":
                                header = False
                            else:
                                header = True
                            if to_excel:
                                kwargs = to_excel.model_dump(exclude_none=True)
                            else:
                                kwargs = {}
                            # TODO integrate better into write method?
                            if isinstance(df.columns, pd.MultiIndex):
                                df = epd.multiindex_to_singleindex(df)
                            df.to_excel(
                                writer,
                                index=False,
                                sheet_name=df_io.name,
                                header=header,
                                startrow=df_io.startrow,
                                **kwargs,
                            )
        with open(self.url, "wb") as write_file:
            self.file_io.seek(0)
            write_file.write(self.file_io.getbuffer())

    def close(self):
        self.file_io.close()
        del el.open_files[self.url]
