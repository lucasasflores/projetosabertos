def filter_data_by_avro_schema(data, avro_schema):
    def filter_fields(data, schema):
        if isinstance(schema, list):
            if len(schema) == 2 and "null" in schema:
                return filter_fields(data, schema[1])
            elif len(schema) == 1:
                return filter_fields(data, schema[0])
        elif isinstance(schema, dict):
            if schema["type"] == "record":
                if not isinstance(data, dict):
                    return None
                result = {}
                for field in schema["fields"]:
                    field_name = field["name"]
                    field_value = data.get(field_name)
                    if field_value is not None:
                        result[field_name] = filter_fields(field_value, field["type"])
                return result
            elif schema["type"] == "array":
                if not isinstance(data, list):
                    return None
                item_type = schema["items"]
                return [filter_fields(item, item_type) for item in data]
            else:
                return data
        else:
            return data

    return filter_fields(data, avro_schema)

# Teste com o exemplo de data e o schema avro fornecido anteriormente
data = {
    'address_city': 'New York',
    'address_street': '123 Main Street',
    'address_zipcode': '10001',
    'emails': ['12', 'doe@example.com'],
    'home': [{'type_home': 1}, {'type_home': 2, 'teste1': 321}, {'blabla': 123}],
    'person_id': 123,
    'full_name': 'John Doe',
    'phone_numbers': [{'number': '123', 'type_phone': 'home'}, {'number': '987654321', 'type_phone': 'work'}],
    'scores_history': 78,
    'scores_math': 85,
    'scores_science': 92,
    'extra_field': 'extra_value'
}

avro_schema = {
    "type": "record",
    "name": "TestObject",
    "namespace": "ca.dataedu",
    "fields": [
        {"name": "address_city", "type": "string", "default": None},
        {"name": "address_street", "type": ["null", "string"], "default": None},
        {"name": "address_zipcode", "type": ["null", "string"], "default": None},
        {"name": "emails", "type": ["null", {"type": "array", "items": ["null", "string"]}], "default": None},
        {"name": "home", "type": ["null", {"type": "array", "items": {"type": "record", "name": "Home", "fields": [
            {"name": "type_home", "type": ["null", "int"], "default": None}
        ]}}], "default": None},
        {"name": "person_id", "type": ["null", "int"], "default": None},
        {"name": "full_name", "type": ["null", "string"], "default": None},
        {"name": "phone_numbers", "type": ["null", {"type": "array", "items": ["null", {"type": "record", "name": "Phone_number", "fields": [
            {"name": "number", "type": ["null", "string"], "default": None},
            {"name": "type_phone", "type": ["null", "string"], "default": None}
        ]}]}], "default": None},
        {"name": "scores_history", "type": ["null", "int"], "default": None},
        {"name": "scores_math", "type": ["null", "int"], "default": None},
        {"name": "scores_science", "type": ["null", "int"], "default": None}
    ]
}

result = filter_data_by_avro_schema(data, avro_schema)
print(result)
