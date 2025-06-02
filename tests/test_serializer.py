import els.config as ec


def test_serialize_transforms():
    # Test the serialize_transforms function to ensure it correctly serializes a list of Transform_ objects.
    config = ec.Config(
        transforms=[
            ec.FilterTransform(filter="xyz"),
            ec.AddColumns(one="1"),
        ]
    )

    serialized = config.model_dump(exclude_none=True, mode="json")
    assert isinstance(serialized["transforms"], list)
    validated = ec.Config.model_validate(serialized)
    print(validated.transforms)
    assert all(isinstance(t, ec.Transform_) for t in validated.transforms)
    assert config.model_dump(exclude_none=True, mode="json") == validated.model_dump(
        exclude_none=True, mode="json"
    )
