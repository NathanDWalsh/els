from els.cli import execute, tree
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

    tree(config)
    execute(config)
