import numpy as np
import pandas as pd

from els.cli import execute
from els.config import Target
from els.path import get_config_default

from . import helpers as th


def push(tmp_path=None, target=None):
    el_config_o = get_config_default()
    el_config_o.source.df_dict = th.outbound
    el_config_o.target.df_dict = th.inbound
    # el_config_o.target.url = "pandas://"

    # TODO: BEWARE MODEL DUMP
    if target:
        el_config_o.target = Target.model_validate(
            el_config_o.target.model_dump(exclude_none=True) | target
        )
    execute(el_config_o)


def test_pd_frame_sends():
    source_df = pd.DataFrame({"z": [9, 8, 7]})
    source_dict = {"source_frame": source_df}
    target_dict = {}

    el_config = get_config_default()
    el_config.source.df_dict = source_dict
    # el_config.source.url = source_dict
    el_config.target.df_dict = target_dict

    execute(el_config)

    if "source_frame" not in target_dict:
        raise Exception([target_dict, source_dict])
    else:
        assert target_dict["source_frame"].equals(source_df)


def test_pd_frame_receives():
    # staged_frames.clear()
    source_df = pd.DataFrame({"x": [9, 8, 7]})
    source_dict = {"frame": source_df}

    target_df = pd.DataFrame(columns=source_df.columns, dtype=np.int64)
    target_dict = {"frame": target_df}

    el_config = get_config_default()
    el_config.source.df_dict = source_dict
    el_config.target.df_dict = target_dict
    el_config.target.if_exists = "append"
    execute(el_config)

    th.assert_expected(source_dict, target_dict)


def test_pd_one_to_one():
    th.f_1_to_1(push)


def test_pd_two_to_two():
    th.f_2_to_2(push)


def test_pd_two_to_one():
    th.f_2_to_1_together(push)


def test_append_fixed():
    th.append_fixed(push)


def test_append_mixed():
    th.append_mixed(push)


def test_truncate():
    th.truncate2(push)


def test_append_minus():
    th.append_minus(push)


def test_append_plus():
    th.append_plus(push)
