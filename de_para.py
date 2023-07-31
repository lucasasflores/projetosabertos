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

    # Aplica o mapeamento de nomes de campos e remove os campos mapeados dos dados originais
    json_data_mapped = apply_mapping(json_data, mapping)

    # Filtra os campos mantendo apenas os campos definidos no schema Avro
    json_data_filtered = filter_data(json_data_mapped, schema_avro)

    return json_data_filtered