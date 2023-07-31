def formatar_campos(data, mapeamento, schema):
    if isinstance(data, list):
        return [formatar_campos(item, mapeamento, schema) for item in data]
    elif isinstance(data, dict):
        novo_dicionario = {}
        for chave, valor in data.items():
            if chave in mapeamento:
                novo_nome = mapeamento[chave]
                if isinstance(valor, list) or isinstance(valor, dict):
                    novo_valor = formatar_campos(valor, mapeamento, schema)
                    novo_dicionario[novo_nome] = novo_valor
                else:
                    novo_dicionario[novo_nome] = valor
            elif chave in schema["fields"]:
                if isinstance(valor, list) or isinstance(valor, dict):
                    novo_valor = formatar_campos(valor, mapeamento, schema["fields"][chave]["type"])
                    novo_dicionario[chave] = novo_valor
                else:
                    novo_dicionario[chave] = valor
        return novo_dicionario
    else:
        return data