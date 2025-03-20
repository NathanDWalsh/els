from els.cli import execute, tree
from els.config import Config, Source, Target

from . import helpers as th


def push(tmp_path=None, target=None, source=None, transform=None):
    config = Config()

    if source:
        config.source = Source.model_validate(source)
    if target:
        config.target = Target.model_validate(target)
    if transform:
        config.transform2 = transform

    config.source.df_dict = th.outbound
    config.target.df_dict = th.inbound

    tree(config)
    execute(config)
