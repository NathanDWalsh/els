from __future__ import annotations

from collections.abc import Iterable, Mapping
from copy import deepcopy
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

import pandas as pd
from pdfminer.high_level import extract_pages
from pdfminer.layout import LAParams, LTChar, LTTextBox

from .base import ContainerReaderABC, FrameABC

if TYPE_CHECKING:
    from els._typing import FrameModeLiteral, IfExistsLiteral, KWArgsIO


def text_range_to_sorted_list(text: str) -> list[int]:
    res: list[int] = []
    segments = text.split(",")
    for segment in segments:
        if "-" in segment:
            start, end = map(int, segment.split("-"))
            res.extend(range(start, end + 1))
        else:
            res.append(int(segment))
    return sorted(res)


def clean_page_numbers(
    page_numbers: Union[int, str, Iterable[int]],
) -> list[int]:
    if isinstance(page_numbers, int):
        return [page_numbers]
    assert isinstance(page_numbers, Iterable)
    if isinstance(page_numbers, str):
        return text_range_to_sorted_list(page_numbers)
    return list(page_numbers)


def pull_pdf(
    file: str,
    laparams: Optional[Mapping[str, str]],
    **kwargs: Any,
) -> pd.DataFrame:
    def get_first_char_from_text_box(tb: LTTextBox) -> LTChar:
        for line in tb:
            for char in line:
                assert isinstance(char, LTChar)
                return char
        raise Exception("Character not found")

    lap = LAParams()
    if laparams:
        for k, v in laparams.items():
            lap.__setattr__(k, v)

    if "page_numbers" in kwargs.keys():
        kwargs["page_numbers"] = clean_page_numbers(kwargs["page_numbers"])

    pm_pages = extract_pages(file, laparams=lap, **kwargs)

    dict_res: dict[str, list[Any]] = {
        "page_index": [],
        "y0": [],
        "y1": [],
        "x0": [],
        "x1": [],
        "height": [],
        "width": [],
        "font_name": [],
        "font_size": [],
        "font_color": [],
        "text": [],
    }

    for p in pm_pages:
        for e in p:
            if isinstance(e, LTTextBox):
                first_char = get_first_char_from_text_box(e)
                dict_res["page_index"].append(
                    kwargs["page_numbers"][p.pageid - 1]
                    if "page_numbers" in kwargs
                    else p.pageid
                )
                dict_res["x0"].append(e.x0)
                dict_res["x1"].append(e.x1)
                dict_res["y0"].append(e.y0)
                dict_res["y1"].append(e.y1)
                dict_res["height"].append(e.height)
                dict_res["width"].append(e.width)
                dict_res["font_name"].append(first_char.fontname)
                dict_res["font_size"].append(first_char.height)
                dict_res["font_color"].append(
                    str(first_char.graphicstate.ncolor)
                    if not isinstance(first_char.graphicstate.ncolor, tuple)
                    else str(first_char.graphicstate.ncolor)
                )
                dict_res["text"].append(e.get_text().replace("\n", " ").rstrip())

    return pd.DataFrame(dict_res)


class PDFFrame(FrameABC):
    parent: PDFContainer  # for mypy

    def __init__(
        self,
        name: str,
        parent: PDFContainer,
        if_exists: IfExistsLiteral = "fail",
        mode: FrameModeLiteral = "s",
        df: pd.DataFrame = pd.DataFrame(),
        kwargs_pull: Optional[KWArgsIO] = None,
    ):
        super().__init__(
            df=df,
            name=name,
            parent=parent,
            mode=mode,
            if_exists=if_exists,
            kwargs_pull=kwargs_pull,
        )

    # TODO test sample scenarios
    # TODO sample should not be optional since it is always called by super.read()
    def _read(self, kwargs: KWArgsIO) -> None:
        if self.kwargs_pull != kwargs:
            kw_copy = deepcopy(kwargs)
            laparams = None
            if "laparams" in kw_copy:
                laparams = kw_copy.pop("laparams")
            if "nrows" in kw_copy:
                del kw_copy["nrows"]
            self.df = pull_pdf(self.parent.url, laparams, **kw_copy)
            self.kwargs_pull = kwargs


class PDFContainer(ContainerReaderABC[PDFFrame]):
    def __init__(
        self,
        url: str,
        replace: bool = False,
    ):
        super().__init__(PDFFrame, url)

    @property
    def create_or_replace(self) -> bool:
        return False

    def _children_init(self) -> None:
        self.children = [
            PDFFrame(
                name=Path(self.url).stem,
                parent=self,
            )
        ]

    def persist(self) -> None:
        pass  # not supported

    def close(self) -> None:
        pass  # not required / closes after read
