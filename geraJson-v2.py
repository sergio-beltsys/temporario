import pymssql

import pandas as pd
import json
import sys

ambiente = sys.argv[4] if len(sys.argv) > 1 else ''
if (ambiente == 'p'):
    # Dados de conexão - PROD
    user = 'sqldb_Cobol'
    password = '6aBlc2ijt@'
    host = 'spobrsqlprd34'
    database = 'CONVERSAO_COBOL'
else:
    # Dados de conexão - DEV
    user = 'sa'
    password = 'master'
    host = 'localhost'
    database = 'Serasa'

nomesAdicionado = set()

tipos_objeto = {
    8: "T",
    9: "Sub"
}

tipos_chamada = {
    "CALLEST": "E",
    "EXCICSXCTL": "X",
    "EXCICS1INK": "LI",
    "EXCICSLOAD": "LO",
    "CALLDIN": "D",
    "EXCICSLINK": "LI",
    "MACCALL3": "D",
    "MACCALL2": "E",
    "MACCALL1": "E",
    "Transacao": "T"
}

try:
    def isTransacao(nome):
        conn = pymssql.connect(server=host, user=user, password=password, database=database)
        cursor = conn.cursor()

        # Executar a consulta SQL
        cursor.execute(f"select top 1 programa from tb_transacoes where nome = '{nome}'")
    
        # Obter o resultado da consulta
        row = cursor.fetchone()
         # Fechar a conexão
        conn.close()

        # Retornar o resultado
        return row[0] if row else nome

    def infoProg(programa):
        # Conectar ao banco de dados
        conn = pymssql.connect(server=host, user=user, password=password, database=database)
        
        # Criar um cursor para executar consultas
        cursor = conn.cursor()

        # Executar a consulta SQL
        cursor.execute(f"select top 1 nome, ext, parent, trim(siglasis) as siglasis from tb_programas where nome = '{programa}'")
        
        # Obter o resultado da consulta
        row = cursor.fetchone()

        # Fechar a conexão
        conn.close()

        # Verificar se a consulta retornou alguma linha
        if row:
            # Criar um DataFrame com o resultado
            df = pd.DataFrame([row], columns=['nome', 'ext', 'parent', 'siglasis'])

            # Converter o DataFrame para JSON
            json_data = df.to_json(orient='records')
        else:
            # Se não houver resultado, retornar um objeto vazio
            json_data = "{}"
        
        # Retornar o JSON
        return json_data

    def cadeia(parent, sigla, subprograma):
        # Conectar ao banco de dados
        conn = pymssql.connect(server=host, user=user, password=password, database=database)
        
        # Criar um cursor para executar consultas
        cursor = conn.cursor()

        # Executar a consulta SQL
        query = f"select '' as regra, trim(objeto) as nome, min(linha), 'Transacao' as call, 8 as tipo_objeto, min(linhaexp) from rl_labels where tipo_objeto = 8 and tipo_relacao = 'P' and origem = '{parent}.{sigla}.{subprograma}' group by objeto, tipo_objeto union select trim(regra), trim(chamado) as nome, min(linfonte), trim(tipo), 9, min(linhaexp) from tb_chamadas where fonte = '{subprograma}' group by trim(chamado), tipo, regra order by 3,1 desc"
        cursor.execute(query)

        # Obter o resultado da consulta
        rows = cursor.fetchall()

        # Fechar a conexão
        conn.close()

        # Converter o resultado em um DataFrame
        df = pd.DataFrame(rows, columns=['regra', 'nome', 'linha', 'call', 'tipo_objeto', 'linhaexp'])

        # Converter o DataFrame para JSON
        json_data = df.to_json(orient='records')

        # Converter o JSON para uma lista de dicionários
        data = json.loads(json_data)

        # Iterar sobre os objetos e adicionar informações adicionais
        for obj in data:
            info = json.loads(infoProg(obj.get('nome')))
            
            if len(info) > 0:
                obj['ext'] = info[0].get("ext")
            else:
                obj['ext'] = "SEM"

                if obj['tipo_objeto'] ==  8:
                    obj['ext'] = ""    
            obj['tipo'] = tipos_objeto.get(obj['tipo_objeto'], "Tipo nao definido")
            obj['call'] = tipos_chamada.get(obj.get("call"), '')
                
            try:
                if obj['call'] == "T"  or obj['call'] == "X"  or obj['nome'] in nomesAdicionado or len(info) < 1:
                    obj['chama'] = []
                else:
                    nomesAdicionado.add(obj['nome'])
                    parent = info[0].get("parent", '')
                    sigla = info[0].get("sigla", '')
                    nome = obj['nome']
                    obj['chama'] = cadeia(parent, sigla, nome)
            except Exception(e):
                print(e.with_traceback)

            obj.pop("tipo_objeto")
            obj.pop("linhaexp")
            obj.pop("linha")
            obj.pop("regra")
           


        return data
    
    def format_json_to_lines(data, level=-1, first=True):
        result = ""
        prefix = "" if first else "|"
        for item in data:
            call = item.get('call', "")
            if first:
                result += "{}\n".format(item["nome"])
            elif call == "T":
                result += "{}{}{} - {}\n".format(prefix, "|" * level, 'Transacao', item["nome"])
            else:
                result += "{}{}{} - {}\n".format(prefix, "|" * level, call, item["nome"])
                
            if item["chama"]:
                result += format_json_to_lines(item["chama"], level + 1, first=False)
        return result


    
    parent = sys.argv[1] if len(sys.argv) > 1 else ''
    sigla = sys.argv[2] if len(sys.argv) > 1 else ''
    nomePrograma = sys.argv[3] if len(sys.argv) > 1 else ''

    programa = isTransacao(nomePrograma)
    data = json.loads(
        infoProg(programa)
    )
    for obj in data: 
        obj['chama'] = [cadeia(obj['parent'], obj['siglasis'],obj['nome'])]
        obj.pop("parent")
        obj.pop("siglasis")
    json_data_com_info_estatica = json.dumps(data)

    with open(f'{nomePrograma}.json', 'w') as f:
        f.write(json_data_com_info_estatica)  
    print(json.dumps(data))
    formatted_lines = format_json_to_lines(data, first=True)

    with open(f'{nomePrograma}.txt', 'w') as f:
        f.write(formatted_lines)  

except Exception as e:
    print(f"Erro ao conectar: {e}")
