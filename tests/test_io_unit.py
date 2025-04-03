# import pandas as pd

# import els.core as el


# def get_test_df_dict():
#     df = pd.DataFrame(
#         dict(a=[1, 2, 3]),
#     )
#     df_dict = dict(
#         dfname=df,
#     )
#     return (df_dict, df)


# def test_df_dict_eq():
#     df_dict, _ = get_test_df_dict()
#     df_dict_io = el.fetch_df_dict_io(df_dict)
#     assert df_dict_io.df_dict == df_dict


# def test_df_dict_id():
#     df_dict, _ = get_test_df_dict()
#     df_dict_io = el.fetch_df_dict_io(df_dict)
#     assert id(df_dict_io.df_dict) == id(df_dict)


# def test_df_id():
#     df_dict, df = get_test_df_dict()
#     df_dict_io = el.fetch_df_dict_io(df_dict)
#     df_io = df_dict_io.get_child("dfname").df
#     assert id(df_io) == id(df)


# def test_df_id3():
#     df_dict, df = get_test_df_dict()
#     df_dict_io = el.fetch_df_dict_io(df_dict)
#     df_io = df_dict_io.get_child("dfname").df_target
#     assert id(df_io) == id(df)


# def test_df_id4():
#     df_dict, df = get_test_df_dict()
#     df_dict_io = el.fetch_df_dict_io(df_dict)
#     df_dict_io.set_df(
#         "dfname",
#         pd.DataFrame(
#             dict(a=[4, 5, 6]),
#         ),
#         if_exists="append",
#     )
#     df_ref = df_dict_io.get_child("dfname").df_target
#     assert id(df_ref) == id(df)
#     df_dict_io.write()

#     df_ref = df_dict_io.get_child("dfname").df_target
#     assert id(df_ref) != id(df)
#     # open_df = df_dict_io.get_child("dfname").ram_df
#     # assert id(open_df) == id(df)


# def test_df_dict_eq2():
#     df_dict, _ = get_test_df_dict()
#     df_dict_io = el.fetch_df_dict_io(df_dict)
#     df_dict_io.set_df(
#         "dfname",
#         pd.DataFrame(
#             dict(a=[4, 5, 6]),
#         ),
#         if_exists="append",
#     )
#     df_dict_io.write()
#     assert df_dict_io.df_dict == df_dict


# def test_df_dict_id2():
#     df_dict, _ = get_test_df_dict()
#     df_dict_io = el.fetch_df_dict_io(df_dict)
#     df_dict_io.set_df(
#         "dfname",
#         pd.DataFrame(
#             dict(a=[4, 5, 6]),
#         ),
#         if_exists="append",
#     )
#     df_dict_io.write()
#     assert id(df_dict_io.df_dict) == id(df_dict)
