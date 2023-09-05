import yaml
import eel.config as ec
from copy import deepcopy


def main():
    config_json = ec.Config.model_json_schema()

    # keep enum typehints on an arbatrary number of elements in AddColumns
    # additionalProperties property attribute functions as a placeholder
    config_json["$defs"]["AddColumns"]["additionalProperties"] = deepcopy(
        config_json["$defs"]["AddColumns"]["properties"]["additionalProperties"]
    )
    del config_json["$defs"]["AddColumns"]["properties"]

    config_yml = yaml.dump(config_json, default_flow_style=False)

    with open("eel_schema.yml", "w") as file:
        file.write(config_yml)


if __name__ == "__main__":
    main()
