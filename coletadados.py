import requests
import os
from urls import *
import pandas as pd
from datetime import datetime
from func import *






def baixar_arquivo(url):
    nome_arquivo = os.path.basename(url)
    response = requests.get(url)
    with open(nome_arquivo, "wb") as f:
        f.write(response.content)












def listar_colunas_por_indice(caminho_arquivo, indice_aba):
    df = pd.read_excel(caminho_arquivo, sheet_name=indice_aba, engine='openpyxl', nrows=0)
    return list(df.columns)


















import pandas as pd

def filtrar_colunas_excel(caminho_arquivo, colunas_desejadas, indice_ou_nome_aba=0):
    """
    Lê um arquivo Excel e retorna um DataFrame com apenas as colunas desejadas.
    Se indice_ou_nome_aba == -1, lê de todas as abas e concatena os dados.

    Parâmetros:
    - caminho_arquivo (str): Caminho do arquivo Excel.
    - colunas_desejadas (list): Lista de colunas a manter.
    - indice_ou_nome_aba (int|str): Índice da aba (0 = primeira), nome da aba, ou -1 para todas.

    Retorna:
    - pd.DataFrame: Dados filtrados.
    """
    excel = pd.ExcelFile(caminho_arquivo)

    if indice_ou_nome_aba == -1:
        frames = []
        for aba in excel.sheet_names:
            df = excel.parse(aba)  # <-- Aqui usamos excel.parse()
            colunas_invalidas = [col for col in colunas_desejadas if col not in df.columns]
            if colunas_invalidas:
                print(f"[AVISO] A aba '{aba}' não contém as colunas: {colunas_invalidas}. Pulando essa aba.")
                continue
            df = df[colunas_desejadas]
            # Converter valores nulos para None e demais para string
            df = df.apply(lambda col: col.map(lambda v: None if pd.isna(v) else str(v)))
            frames.append(df)
        if not frames:
            raise ValueError("Nenhuma aba válida com as colunas desejadas foi encontrada.")
        return pd.concat(frames, ignore_index=True)
    
    # Se for índice, pega o nome da aba
    aba = excel.sheet_names[indice_ou_nome_aba] if isinstance(indice_ou_nome_aba, int) else indice_ou_nome_aba
    df = excel.parse(aba)  # <-- Também aqui usamos excel.parse()

    colunas_invalidas = [col for col in colunas_desejadas if col not in df.columns]
    if colunas_invalidas:
        raise ValueError(f"Colunas não encontradas na aba '{aba}': {colunas_invalidas}")

    df = df[colunas_desejadas]
    df = df.apply(lambda col: col.map(lambda v: None if pd.isna(v) else str(v)))

    return df



















def verificar_atualizacao(nome_banco, nome_tabela):
    getInf = consultar_todos_os_dados(nome_banco, 'atualizacao')
    tb = [i for i in getInf if i[1] == nome_tabela]
    if tb == []:
        inserir_dados_simples(nome_banco, 'atualizacao', [[nome_tabela, datetime.now().strftime('%m/%Y')]], ['TABELA', 'DATA_ATUALIZACAO'])
        inserir_dados_sqlite(filtrar_colunas_excel('SPDadosCriminais_2025.xlsx', ['NUM_BO', 'DATA_REGISTRO', 'DATA_OCORRENCIA_BO', 'HORA_OCORRENCIA_BO', 'NATUREZA_APURADA', 'LATITUDE', 'LONGITUDE', 'NOME_MUNICIPIO', 'BAIRRO'], -1), 'dados.db', 'dadoscriminais', 'NUM_BO')
    else:
        data_tb = datetime.strptime(tb[0][2], '%m/%Y')
        data_agora = datetime.now()
        if datetime(data_tb.year, data_tb.month, 1) > datetime(data_agora.year, data_agora.month, 1):
            atualizar_valor(nome_banco, 'atualizacao', 'DATA_ATUALIZACAO', datetime.now().strftime('%m/%Y'), 'TABELA', nome_tabela)
            inserir_dados_sqlite(filtrar_colunas_excel('SPDadosCriminais_2025.xlsx', ['NUM_BO', 'DATA_REGISTRO', 'DATA_OCORRENCIA_BO', 'HORA_OCORRENCIA_BO', 'NATUREZA_APURADA', 'LATITUDE', 'LONGITUDE', 'NOME_MUNICIPIO', 'BAIRRO'], -1), 'dados.db', 'dadoscriminais', 'NUM_BO')












