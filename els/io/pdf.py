from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Generator, Optional

import pandas as pd
from pdfminer.high_level import LAParams, extract_pages
from pdfminer.layout import LTChar, LTTextBox

from .base import ContainerReaderABC, FrameABC


def text_range_to_list(text: str):
    result: list = []
    segments = text.split(",")
    for segment in segments:
        if "-" in segment:
            start, end = map(int, segment.split("-"))
            result.extend(range(start, end + 1))
        else:
            result.append(int(segment))
    return result


def clean_page_numbers(page_numbers):
    if isinstance(page_numbers, int):
        res = [page_numbers]
    if isinstance(page_numbers, str):
        res = text_range_to_list(page_numbers)
    else:
        res = page_numbers
    return sorted(res)


def pull_pdf(
    file,
    laparams: Optional[dict],
    **kwargs,
) -> pd.DataFrame:
    def get_first_char_from_text_box(tb) -> LTChar:  # type: ignore
        for line in tb:
            for char in line:
                return char

    lap = LAParams()
    if laparams:
        for k, v in laparams.items():
            lap.__setattr__(k, v)

    if "page_numbers" in kwargs:
        kwargs["page_numbers"] = clean_page_numbers(kwargs["page_numbers"])

    pm_pages = extract_pages(file, laparams=lap, **kwargs)

    dict_res: dict[str, list] = {
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
    def __init__(
        self,
        name,
        parent,
        if_exists="fail",
        mode="s",
        df=pd.DataFrame(),
        kw_for_pull=None,
    ):
        super().__init__(
            df=df,
            name=name,
            parent=parent,
            mode=mode,
            if_exists=if_exists,
            kw_for_pull=kw_for_pull,
        )
        # self.kw_for_pull = kw_for_pull

    @property
    def parent(self) -> PDFContainer:
        return super().parent

    @parent.setter
    def parent(self, v):
        FrameABC.parent.fset(self, v)

    # TODO test sample scenarios
    # TODO sample should not be optional since it is always called by super.read()
    def _read(self, kwargs: dict):
        if not kwargs:
            kwargs = self.kw_for_pull
        if self.mode in ("r", "s") and self.kw_for_pull != kwargs:
            kw_copy = deepcopy(kwargs)
            laparams = None
            if "laparams" in kw_copy:
                laparams = kw_copy.pop("laparams")
            if "nrows" in kw_copy:
                del kw_copy["nrows"]
            self.df = pull_pdf(self.parent.url, laparams, **kw_copy)
            self.kw_for_pull = kwargs


class PDFContainer(ContainerReaderABC):
    def __init__(self, url, replace=False):
        super().__init__(PDFFrame, url)

    def __iter__(self) -> Generator[PDFFrame, None, None]:
        for child in super().children:
            yield child

    @property
    def create_or_replace(self):
        return False

    def _children_init(self):
        PDFFrame(
            name=Path(self.url).stem,
            parent=self,
        )

    def persist(self):
        pass  # not supported

    def close(self):
        pass  # not required / closes after read
