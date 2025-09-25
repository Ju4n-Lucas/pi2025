import sqlite3

def criar_tabela_sqlite(nome_banco, nome_tabela, colunas):
    """
    Cria uma tabela em um banco de dados SQLite.

    Parâmetros:
    - nome_banco (str): Caminho para o arquivo do banco SQLite (ex: 'meubanco.db').
    - nome_tabela (str): Nome da tabela a ser criada.
    - colunas (dict): Dicionário onde a chave é o nome da coluna e o valor é o tipo SQL (ex: {'nome': 'TEXT', 'idade': 'INTEGER'}).

    Exemplo:
    criar_tabela_sqlite('clientes.db', 'usuarios', {'id': 'INTEGER PRIMARY KEY', 'nome': 'TEXT', 'email': 'TEXT'})
    """
    # Monta a string de colunas (ex: "id INTEGER PRIMARY KEY, nome TEXT, email TEXT")
    definicao_colunas = ', '.join([f"{col} {tipo}" for col, tipo in colunas.items()])

    # Comando SQL de criação
    sql = f"CREATE TABLE IF NOT EXISTS {nome_tabela} ({definicao_colunas});"

    # Conecta e executa
    with sqlite3.connect(nome_banco) as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()









def inserir_dados_sqlite(df, nome_banco, nome_tabela, campo_chave):
    """
    Insere os dados de um DataFrame em uma tabela SQLite,
    evitando duplicatas com base em um campo chave (ex: 'DATA_REGISTRO' ou 'NUM_BO').

    Parâmetros:
    - df (pd.DataFrame): DataFrame com os dados a inserir.
    - nome_banco (str): Caminho do banco SQLite.
    - nome_tabela (str): Nome da tabela de destino.
    - campo_chave (str): Nome da coluna usada para verificar duplicação (comparação como string).
    """
    if campo_chave not in df.columns:
        raise ValueError(f"O campo chave '{campo_chave}' não existe no DataFrame.")

    # Conecta ao banco
    with sqlite3.connect(nome_banco) as conn:
        cursor = conn.cursor()

        # Recupera os valores únicos já existentes no banco para o campo chave
        cursor.execute(f"SELECT {campo_chave} FROM {nome_tabela}")
        registros_existentes = {str(row[0]) for row in cursor.fetchall()}

        # Filtra o DataFrame para manter apenas os registros NOVOS
        df_filtrado = df[~df[campo_chave].astype(str).isin(registros_existentes)]

        # Se houver registros novos, insere
        if not df_filtrado.empty:
            df_filtrado.to_sql(nome_tabela, conn, if_exists='append', index=False)
            print(f"{len(df_filtrado)} novos registros inseridos.")
        else:
            print("Nenhum novo registro a inserir — todos já existem.")












def consultar_todos_os_dados(nome_banco, nome_tabela, limite=None, campo_ordenacao='id'):
    """
    Executa SELECT * FROM nome_tabela e retorna os dados como lista de tuplas.

    Parâmetros:
    - nome_banco (str): Caminho para o banco de dados SQLite.
    - nome_tabela (str): Nome da tabela a consultar.
    - limite (int, opcional): Número de registros a retornar.
        * Se positivo: retorna os primeiros N registros.
        * Se negativo: retorna os últimos N registros (ordem decrescente, revertida para manter ordem original).
        * Se None: retorna todos.
    - campo_ordenacao (str): Nome da coluna para ordenar (usado ao buscar os últimos).

    Retorna:
    - list[tuple]: Lista de registros da tabela.
    """
    with sqlite3.connect(nome_banco) as conn:
        cursor = conn.cursor()

        if limite is None:
            query = f"SELECT * FROM {nome_tabela}"
        elif limite > 0:
            query = f"SELECT * FROM {nome_tabela} LIMIT {limite}"
        else:
            # Para negativos: ORDER BY decrescente + LIMIT, depois inverter resultado
            query = f"SELECT * FROM {nome_tabela} ORDER BY {campo_ordenacao} DESC LIMIT {abs(limite)}"

        cursor.execute(query)
        dados = cursor.fetchall()

        # Se limite for negativo, reverter resultado para manter ordem "natural"
        if limite and limite < 0:
            dados.reverse()

    return dados













def eliminar_tabela(nome_banco, nome_tabela):
    """
    Remove uma tabela do banco de dados SQLite, se ela existir.

    Parâmetros:
    - nome_banco (str): Caminho do banco de dados SQLite.
    - nome_tabela (str): Nome da tabela a ser removida.
    """
    with sqlite3.connect(nome_banco) as conn:
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {nome_tabela}")
        conn.commit()













