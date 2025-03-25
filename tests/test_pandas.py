import yaml

import els.cli as ei
import els.config as ec

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

    test_els = "__.els.yml"
    yaml.dump(
        config.model_dump(exclude_none=True),
        open(test_els, "w"),
        sort_keys=False,
        allow_unicode=True,
    )

    # ei.tree(config)
    ei.tree(test_els)
    ei.execute(test_els)

    # ei.execute(config)
