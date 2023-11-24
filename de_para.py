def apply_mapping_and_filter(json_data, mapping, schema_avro):
    def apply_mapping(data, mapping):
        mapped_data = {}
        for old_field, new_field in mapping.items():
            if old_field in data:
                mapped_data[new_field] = data[old_field]
                if old_field != new_field:
                    del data[old_field]
        return mapped_data

    def filter_data(data, schema):
        filtered_data = {}
        for field in schema["fields"]:
            field_name = field["name"]
            if field_name in data:
                if field["type"] == "record":
                    filtered_data[field_name] = filter_data(data[field_name], field)
                else:
                    filtered_data[field_name] = data[field_name]
        return filtered_data

    # Aplicar o mapeamento de nomes de campos e remover os campos mapeados dos dados originais
    json_data_mapped = apply_mapping(json_data, mapping)

    # Filtrar os campos mantendo apenas os campos definidos no schema Avro
    json_data_filtered = filter_data(json_data_mapped, schema_avro)

    return json_data_filtered

# Exemplo de schema Avro com tipos complexos
schema_avro = {
    "type": "record",
    "name": "Person",
    "fields": [
        {"name": "new_id", "type": ["null", "int"], "default": None},
        {"name": "full_name", "type": ["null", "string"], "default": None},
        {"name": "email_addresses", "type": ["null", {"type": "array", "items": "string"}], "default": None},
        {"name": "contact_phone", "type": ["null", {
            "type": "record",
            "name": "PhoneNumber",
            "fields": [
                {"name": "type", "type": ["null", "string"], "default": None},
                {"name": "number", "type": ["null", "string"], "default": None}
            ]
        }], "default": None}
    ]
}

# Exemplo de dados fornecido
data_avro = {
    "id": 1,
    "name": "João",
    "emails": ["joao@example.com"],
    "phone": {
        "type": "celular",
        "number": "987654321"
    },
    "additional_field": "valor adicional",
    "yet_another_field": "mais um valor adicional"
}

# Exemplo de mapeamento de nomes
mapping = {
    "id": "new_id",
    "name": "full_name",
    "emails": "email_addresses",
    "phone": "contact_phone"
}

# Chamar a função para aplicar o mapeamento e filtrar os campos
result = apply_mapping_and_filter(data_avro, mapping, schema_avro)
print(result)
