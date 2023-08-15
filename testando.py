import json

def remove_optional_null(schema):
    if isinstance(schema, list):
        if "null" in schema:
            schema.remove("null")
            return remove_optional_null(schema[0])
        else:
            return [remove_optional_null(item) for item in schema]
    elif isinstance(schema, dict):
        new_schema = {}
        for key, value in schema.items():
            if key == "type" and isinstance(value, list) and "null" in value:
                new_schema[key] = remove_optional_null(value)
            elif key == "items":
                new_schema[key] = remove_optional_null(value)
            else:
                if key != "default":
                    new_schema[key] = remove_optional_null(value)
        return new_schema
    else:
        return schema

# Your Avro schema
avro_schema_input = {
    "type": "record",
    "name": "TestObject",
    "namespace": "ca.dataedu",
    "fields": [
        {
            "name": "address",
            "type": {
                "type": "record",
                "name": "Address",
                "fields": [
                    {"name": "city", "type": ["null", "string"]},
                    {"name": "street", "type": ["null", "string"]},
                    {"name": "zipcode", "type": ["null", "string"]}
                ]
            },
            "default": None
        },
        {
            "name": "emails",
            "type": ["null", {"type": "array", "items": ["null", "string"]}],
            "default": None
        },
        {
            "name": "id",
            "type": ["null", "int"],
            "default": None
        },
        {
            "name": "name",
            "type": ["null", "string"],
            "default": None
        },
        {
            "name": "phone_numbers",
            "type": [
                "null",
                {
                    "type": "array",
                    "items": [
                        "null",
                        {
                            "type": "record",
                            "name": "Phone_number",
                            "fields": [
                                {"name": "number", "type": ["null", "string"]},
                                {"name": "type", "type": ["null", "string"]}
                            ]
                        }
                    ]
                }
            ],
            "default": None
        },
        {
            "name": "scores",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "Scores",
                    "fields": [
                        {"name": "history", "type": ["null", "int"]},
                        {"name": "math", "type": ["null", "int"]},
                        {"name": "science", "type": ["null", "int"]}
                    ]
                }
            ],
            "default": None
        }
    ]
}

# Apply the function to the Avro schema
avro_schema_cleaned = remove_optional_null(avro_schema_input)

# Print the cleaned Avro schema
print(json.dumps(avro_schema_cleaned, indent=2))
