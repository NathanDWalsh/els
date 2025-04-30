import os

import pandas as pd

import els.config as ec
import els.core as el

from . import helpers as th


def test_fwf_read(pytester):
    pytester.copy_example("./tests/sources/fwf1.fwf")
    inbound = {}
    print(os.getcwd())
    config = ec.Config(
        source=ec.Source(url="fwf1.fwf"),
        target=ec.Target(
            url=el.urlize_dict(inbound),
        ),
    )
    th.config_execute(config)
    expected = pd.DataFrame(
        dict(
            a=[1, 4, 7],
            b=[2, 5, 8],
            c=[3, 6, 9],
        )
    )
    assert expected.equals(inbound["fwf1"])


def test_fwf_read2(pytester):
    pytester.copy_example("./tests/sources/fwf1.fwf")
    inbound = {}
    print(os.getcwd())
    config = ec.Config(
        source=ec.Source(
            url="fwf1.fwf",
            read_fwf=[ec.ReadFWF(colspecs="infer")],
        ),
        target=ec.Target(
            url=el.urlize_dict(inbound),
        ),
    )
    th.config_execute(config)
    expected = pd.DataFrame(
        dict(
            a=[1, 4, 7],
            b=[2, 5, 8],
            c=[3, 6, 9],
        )
    )
    assert expected.equals(inbound["fwf1"])


def test_fwf_read3(pytester):
    pytester.copy_example("./tests/sources/fwf1.fwf")
    inbound = {}
    print(os.getcwd())
    config = ec.Config(
        source=ec.Source(
            url="fwf1.fwf",
            read_fwf=[
                ec.ReadFWF(colspecs="infer"),
                ec.ReadFWF(colspecs="infer"),
            ],
        ),
        target=ec.Target(
            url=el.urlize_dict(inbound),
        ),
    )
    th.config_execute(config)
    expected = pd.DataFrame(
        dict(
            a=[1, 4, 7, 1, 4, 7],
            b=[2, 5, 8, 2, 5, 8],
            c=[3, 6, 9, 3, 6, 9],
        )
    )
    assert expected.equals(inbound["fwf1"])


def test_pdf_read(pytester) -> None:
    pytester.copy_example("./tests/sources/pdf1.pdf")
    inbound: dict[str, pd.DataFrame] = {}
    # print(os.getcwd())
    config = ec.Config(
        source=ec.Source(url="pdf1.pdf"),
        target=ec.Target(
            url=el.urlize_dict(inbound),
        ),
    )
    th.config_execute(config)
    expected = pd.DataFrame(
        dict(
            page_index=[1],
            y0=[756.46672],
            y1=[767.5067200000001],
            x0=[72.024],
            x1=[92.38512],
            height=[11.040000000000077],
            width=[20.36112],
            font_name=["BCDEEE+Aptos"],
            font_size=[11.040000000000077],
            # font_color=["0"], pytest reads as 0, tox as 0.0
            text=["test"],
        )
    )
    # inbound["pdf1"]["font_color"] = inbound["pdf1"]["font_color"].replace("0", "")
    inbound["pdf1"].drop("font_color", axis=1, inplace=True)
    th.assert_expected(expected, inbound["pdf1"])
