import els.core as el

from . import helpers as th


def flight_url():
    el.fetch_df_dict_io(th.inflight)
    return f"dict://{id(th.inflight)}"
