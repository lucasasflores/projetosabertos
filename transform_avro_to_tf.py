def avro_to_athena_type(avro_type):
    if isinstance(avro_type, dict):
        if avro_type["type"] == "record":
            fields = avro_type["fields"]
            columns = []
            for field in fields:
                columns.append({
                    "name": field["name"],
                    "type": avro_to_athena_type(field["type"])
                })
            return "struct<{}>".format(', '.join(["{}:{}".format(col['name'], col['type']) for col in columns]))
        elif avro_type["type"] == "array":
            return "array<{}>".format(avro_to_athena_type(avro_type['items']))
        elif avro_type["type"] == "map":
            return "map<string, string>"
        else:
            return avro_type["type"]
    elif isinstance(avro_type, list):
        return avro_to_athena_type(avro_type[1])
    else:
        return avro_type

def avro_to_terraform_glue_table(avro_schema):
    fields = avro_schema["fields"]
    columns = []

    for field in fields:
        column = {
            "name": field["name"],
            "type": avro_to_athena_type(field["type"])
        }
        columns.append(column)

    return f'''resource "aws_glue_catalog_table" "avro_table" {{
  name          = "{avro_schema["name"]}"
  database_name = "your_database_name"
  table_type    = "EXTERNAL_TABLE"

  storage_descriptor {{
    location = "s3://path/to/your/data"
    columns = {columns}
  }}
}}'''

# Example Avro schema
avro_schema = {
    "type": "record",
    "name": "Employee",
    "fields": [
        {
            "name": "id",
            "type": "int"
        },
        {
            "name": "name",
            "type": "string"
        },
        {
            "name": "department",
            "type": {
                "type": "record",
                "name": "Department",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "location", "type": "string"}
                ]
            }
        },
        {
            "name": "skills",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "Skill",
                    "fields": [
                        {"name": "name", "type": "string"},
                        {"name": "level", "type": "string"}
                    ]
                }
            }
        }
    ]
}

terraform_config = avro_to_terraform_glue_table(avro_schema)
print(terraform_config)
