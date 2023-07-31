def formatar_campos(data, mapeamento, schema):
    if isinstance(data, list):
        return [formatar_campos(item, mapeamento, schema) for item in data]
    elif isinstance(data, dict):
        novo_dicionario = {}
        for chave, valor in data.items():
            if chave in mapeamento:
                novo_nome = mapeamento[chave]
                novo_valor = formatar_campos(valor, mapeamento, schema)
                if novo_nome in schema["fields"]:
                    novo_dicionario[novo_nome] = novo_valor
        return novo_dicionario
    else:
        return data