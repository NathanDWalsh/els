import sys
import pandas as pd
import eel.config as ec
import eel.execute as ei


def process_input(yaml_string: str) -> pd.DataFrame:
    # For now, we're ignoring the yaml_string and returning a fixed DataFrame.
    # data = {"Column1": ["Value1", "Value2"], "Column2": ["Value3", "Value4"]}

    config = ec.Config(yaml_input)
    df = ei.get_df(config.source, nrows=100)
    return df


if __name__ == "__main__":
    # yaml_input = sys.stdin.read()  # Reading input from standard input
    if len(sys.argv) > 1:  # check if an argument was passed
        yaml_input = sys.argv[1]  # the first argument
    else:
        yaml_input = "default_value"
    # yaml_input = "sad"
    print(yaml_input)
    df = process_input(yaml_input)
    print(
        df.to_string(index=False)
    )  # Printing the DataFrame as a string to standard output
