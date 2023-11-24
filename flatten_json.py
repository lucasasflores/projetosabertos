def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, parent_key=new_key, sep=sep).items())
        elif isinstance(v, list):
            items.append((new_key, [flatten_dict(item, sep=sep) if isinstance(item, dict) else item for item in v]))
        else:
            items.append((new_key, v))
    return dict(items)

data_complex = {
    "id": 123,
    "name": "John Doe",
    "emails": ["john@example.com", "doe@example.com"],
    "phone_numbers": [
        {"type": "home", "number": "123456789"},
        {"type": "work", "number": "987654321"}
    ],
    "address": {
        "street": "123 Main Street",
        "city": "New York",
        "zipcode": "10001"
    },
    "scores": {
        "math": 85,
        "science": 92,
        "history": 78
    }
}

flattened_data = flatten_dict(data_complex)

print(flattened_data)