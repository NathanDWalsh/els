import yaml
import EelConfig as ec


def remove_titles_from_schema(schema):
    if "title" in schema:
        del schema["title"]
    for key, value in schema.items():
        if isinstance(value, dict):
            remove_titles_from_schema(value)


def add_additional_properties(schema):
    if isinstance(schema, dict):
        # If the current dictionary represents an object schema
        if schema.get("type") == "object":
            schema["additionalProperties"] = False
        # Recurse into the dictionary's values
        for value in schema.values():
            add_additional_properties(value)


json_schema = ec.Config.model_json_schema()
remove_titles_from_schema(json_schema)
add_additional_properties(json_schema)
yaml_schema = yaml.dump(json_schema, default_flow_style=False)
print(yaml_schema)
with open("eel_schema.yml", "w") as file:
    file.write(yaml_schema)
