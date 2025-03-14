from els.cli import execute
from els.config import Config, Target

from . import helpers as th


def push(tmp_path=None, target=None):
    config = Config()
    config.source.df_dict = th.outbound
    config.target.df_dict = th.inbound

    if target:
        config.target = Target.model_validate(
            config.target.model_dump(exclude_none=True) | target
        )
    execute(config)


def test_pd_single():
    th.single(push)


def test_pd_double_together():
    th.double_together(push)


def test_pd_double_separate():
    th.double_separate(push)


def test_append_together():
    th.append_together(push)


def test_append_separate():
    th.append_separate(push)


def test_append_mixed():
    th.append_mixed(push)


def test_truncate_single():
    th.truncate_single(push)


def test_truncate_double():
    th.truncate_double(push)


def test_append_minus():
    th.append_minus(push)


def test_append_plus():
    th.append_plus(push)
