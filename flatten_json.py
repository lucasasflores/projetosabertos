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

def flatten_complex_data(input_data, sep='_'):
    if isinstance(input_data, dict):
        return flatten_dict(input_data, sep=sep)
    # elif isinstance(input_data, list):
    #     return [flatten_complex_data(item, sep=sep) for item in input_data]
    # else:
    #     return input_data

input_data = [
    "data": [
        {"key1": 1, "key2": [{"item1": {"xz": 1}}, {"item1": {"xz": 2}}], "key3": {"xv": 1, "xk": 2}},
        {"key1": 2, "key2": [{"item1": {"xz": 2}}, {"item1": {"xz": 3}}], "key3": {"xv": 4, "xk": 5}}
    ],
    "teste": "oi",
    "teste2": 4
]

flattened_data = flatten_complex_data(input_data)

print(flattened_data)