def apply_mapping(data, mapping):
    if isinstance(data, list):
        return [apply_mapping(item, mapping) for item in data]
    elif isinstance(data, dict):
        result = {}
        for key, value in data.items():
            if isinstance(value, (dict,list)):
                key_new = mapping.get(key, key)
                result[key_new] = apply_mapping(data[key], mapping[key_new])
            else:
                key_new = mapping.get(key, key)
                result[key_new] = value
        return result
    else:
        return data

data = {
    "city": "New York",
    "street": "123 Main Street",
    "zip": "10001",
    "emails": ["john@example.com", "doe@example.com"],
    "home_addres": [{"type_xpto": 1}, {"type_xpto": 2, "xpto": 321}],
    "person_id": 123,
    "full_name": "John Doe",
    "phone_data": [{"num": "123456789", "type_xpto": "home"}, {"num": "987654321", "type_xpto": "work"}],
    "scores_history": 78,
    "math_scores": 85,
    "science_scores": 92,
    "extra_field": "extra_value"
}

mapping = {
    "city": "address_city",
    "street": "address_street",
    "zip": "address_zipcode",
    "emails": "emails",
    "home_addres": "home",
    "home":{"type_xpto": "type_home"},
    "person_id": "person_id",
    "full_name": "full_name",
    "phone_data": "phone_numbers",
    "phone_numbers": {
        "num": "number",
        "type_xpto": "type_phone"
    },
    "scores_history": "scores_history",
    "math_scores": "scores_math",
    "science_scores": "scores_science"
}

resultado_esperado = apply_mapping(data, mapping)
print(resultado_esperado)
