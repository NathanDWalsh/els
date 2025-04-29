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
