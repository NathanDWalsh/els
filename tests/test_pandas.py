import els.config as ec
from els.cli import execute, tree

from . import helpers as th


def push(
    tmp_path=None,
    target=ec.Target(),
    source=ec.Source(),
    transform=None,
):
    config = ec.Config(
        source=source,
        target=target,
        transform=transform,
    )

    config.source.df_dict = th.outbound
    config.target.df_dict = th.inbound

    print(config.model_dump(exclude_unset=True))

    tree(config)
    execute(config)
