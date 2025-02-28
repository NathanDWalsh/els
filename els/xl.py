from python_calamine import CalamineWorkbook, SheetTypeEnum, SheetVisibleEnum


def get_sheet_names(xlIO, sheet_states: list = [SheetVisibleEnum.Visible]) -> list[str]:
    xlIO.seek(0)
    if sheet_states is None:
        sheet_states = [SheetVisibleEnum.Visible]

    with CalamineWorkbook.from_filelike(xlIO) as workbook:
        worksheet_names = [
            sheet.name
            for sheet in workbook.sheets_metadata
            if (sheet.visible in sheet_states)
            and (sheet.typ == SheetTypeEnum.WorkSheet)
        ]
        return worksheet_names


def get_sheet_height(xlIO, sheet_name: str) -> int:
    xlIO.seek(0)
    with CalamineWorkbook.from_filelike(xlIO) as workbook:
        if sheet_name in workbook.sheet_names:
            return workbook.get_sheet_by_name(sheet_name).total_height
        else:
            return None


def get_sheet_row(xlIO, sheet_name: str, row_index: int) -> list:
    xlIO.seek(0)
    with CalamineWorkbook.from_filelike(xlIO) as workbook:
        if sheet_name in workbook.sheet_names:
            return workbook.get_sheet_by_name(sheet_name).to_python(
                nrows=row_index + 1
            )[-1]
        else:
            return None
