import requests
import os
from urlsColetas import *
import pandas as pd







def baixar_arquivo(url):
    nome_arquivo = os.path.basename(url)
    response = requests.get(url)
    with open(nome_arquivo, "wb") as f:
        f.write(response.content)










def obter_linhas_da_coluna(caminho_arquivo, coluna, abas_desejadas=None, por_indice=True):
    todas_abas = pd.read_excel(caminho_arquivo, sheet_name=None, engine='openpyxl')
    nomes_abas = list(todas_abas.keys())

    if abas_desejadas is None:
        abas_indices = list(range(len(nomes_abas)))
    elif isinstance(abas_desejadas, int):
        abas_indices = [abas_desejadas]
    else:
        abas_indices = abas_desejadas

    valores_coluna = []

    for i in abas_indices:
        if i < 0 or i >= len(nomes_abas):
            continue

        nome_aba = nomes_abas[i]
        df = todas_abas[nome_aba]

        try:
            if por_indice:
                if coluna < 0 or coluna >= df.shape[1]:
                    continue
                valores_coluna.extend(df.iloc[:, coluna].tolist())
            else:
                if coluna not in df.columns:
                    continue
                valores_coluna.extend(df[coluna].tolist())
        except:
            continue

    return valores_coluna









def listar_colunas_por_indice(caminho_arquivo, indice_aba):
    df = pd.read_excel(caminho_arquivo, sheet_name=indice_aba, engine='openpyxl', nrows=0)
    return list(df.columns)







