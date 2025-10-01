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
    definicao_colunas = ', '.join([f"{col} {tipo}" for col, tipo in colunas.items()])

    sql = f"CREATE TABLE IF NOT EXISTS {nome_tabela} ({definicao_colunas});"

    with sqlite3.connect(nome_banco) as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()










def inserir_dados_sqlite(df, nome_banco, nome_tabela, campo_chave):
    """
    Insere os dados de um DataFrame em uma tabela SQLite, com base apenas na ordem das colunas.
    O nome das colunas no DataFrame não é considerado — apenas a ordem.
    A função detecta automaticamente as colunas da tabela SQLite (exceto colunas autoincrementadas como 'id').

    Parâmetros:
    - df: DataFrame com os dados, na ordem exata esperada pela tabela (sem incluir colunas como 'id').
    - nome_banco: Caminho para o banco SQLite.
    - nome_tabela: Nome da tabela no banco.
    - campo_chave: Nome da coluna no df usada para evitar duplicatas.
    """
    if campo_chave not in df.columns:
        raise ValueError(f"O campo chave '{campo_chave}' não existe no DataFrame.")

    with sqlite3.connect(nome_banco) as conn:
        cursor = conn.cursor()

        # Verifica se a tabela existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (nome_tabela,))
        if not cursor.fetchone():
            raise ValueError(f"A tabela '{nome_tabela}' não existe no banco de dados.")

        # Descobre as colunas da tabela, exceto colunas AUTOINCREMENT (como 'id')
        cursor.execute(f"PRAGMA table_info({nome_tabela})")
        colunas_tabela = [col[1] for col in cursor.fetchall() if col[5] == 0]  # col[5] == 1 indica PK (ex: 'id')

        if len(colunas_tabela) != df.shape[1]:
            raise ValueError(f"Quantidade de colunas no DataFrame ({df.shape[1]}) não bate com a tabela ({len(colunas_tabela)}): {colunas_tabela}")

        # Prepara SQL
        placeholders = ','.join(['?'] * len(colunas_tabela))
        colunas_str = ','.join(colunas_tabela)
        sql = f"INSERT INTO {nome_tabela} ({colunas_str}) VALUES ({placeholders})"

        # Verifica se a tabela está vazia
        cursor.execute(f"SELECT COUNT(*) FROM {nome_tabela}")
        qtd_registros = cursor.fetchone()[0]

        # Prepara os dados na ordem original do DataFrame
        valores = df.itertuples(index=False, name=None)

        if qtd_registros == 0:
            cursor.executemany(sql, valores)
            conn.commit()
            print(f"Tabela '{nome_tabela}' estava vazia. {df.shape[0]} registros inseridos.")
            return

        # Tabela já tem dados: verificar duplicatas
        cursor.execute(f"SELECT {campo_chave} FROM {nome_tabela}")
        registros_existentes = {str(row[0]) for row in cursor.fetchall()}

        df_filtrado = df[~df[campo_chave].astype(str).isin(registros_existentes)]

        if not df_filtrado.empty:
            valores_filtrados = df_filtrado.itertuples(index=False, name=None)
            cursor.executemany(sql, valores_filtrados)
            conn.commit()
            print(f"{df_filtrado.shape[0]} novos registros inseridos.")
        else:
            print("Nenhum novo registro a inserir — todos já existem.")













def inserir_dados_simples(nome_banco, nome_tabela, dados, colunas):
    """
    Insere dados em uma tabela SQLite.

    Parâmetros:
    - nome_banco (str): caminho do banco SQLite.
    - nome_tabela (str): nome da tabela.
    - dados (list de listas ou tuplas): dados a inserir.
    - colunas (list de str): nomes das colunas, na mesma ordem dos dados.
    """
    with sqlite3.connect(nome_banco) as conn:
        cursor = conn.cursor()

        placeholders = ', '.join(['?'] * len(colunas))
        sql = f"INSERT INTO {nome_tabela} ({', '.join(colunas)}) VALUES ({placeholders})"

        cursor.executemany(sql, dados)
        conn.commit()

















def atualizar_valor(nome_banco, nome_tabela, coluna_alvo, novo_valor, coluna_filtro, valor_filtro):
    """
    Atualiza um valor específico em uma tabela SQLite.

    Parâmetros:
    - nome_banco (str): caminho do banco de dados.
    - nome_tabela (str): nome da tabela onde será feita a atualização.
    - coluna_alvo (str): nome da coluna que terá seu valor alterado.
    - novo_valor: novo valor a ser atribuído.
    - coluna_filtro (str): nome da coluna usada como condição (ex: 'nome').
    - valor_filtro: valor usado para filtrar qual registro atualizar.
    """
    with sqlite3.connect(nome_banco) as conn:
        cursor = conn.cursor()
        sql = f"""
        UPDATE {nome_tabela}
        SET {coluna_alvo} = ?
        WHERE {coluna_filtro} = ?
        """
        cursor.execute(sql, (novo_valor, valor_filtro))
        conn.commit()
        print(f"Atualizado: {coluna_alvo} = {novo_valor} onde {coluna_filtro} = {valor_filtro}")



















def consultar_todos_os_dados(nome_banco, nome_tabela, limite=None, campo_ordenacao='id', coluna=None):
    """
    Executa SELECT na tabela e retorna os dados como lista de tuplas ou valores únicos, conforme a coluna.

    Parâmetros:
    - nome_banco (str): Caminho para o banco de dados SQLite.
    - nome_tabela (str): Nome da tabela a consultar.
    - limite (int, opcional): Número de registros a retornar.
        * Se positivo: retorna os primeiros N registros.
        * Se negativo: retorna os últimos N registros (ordem decrescente, revertida para manter ordem original).
        * Se None: retorna todos.
    - campo_ordenacao (str): Nome da coluna para ordenar (usado ao buscar os últimos).
    - coluna (str, opcional): Nome de uma coluna específica a retornar. Se None, retorna todas.

    Retorna:
    - list: Lista de tuplas (com SELECT *) ou lista de valores (se coluna for usada).
    """
    with sqlite3.connect(nome_banco) as conn:
        cursor = conn.cursor()

        # Selecione a coluna específica ou todas
        seletor = coluna if coluna else '*'

        if limite is None:
            query = f"SELECT {seletor} FROM {nome_tabela}"
        elif limite > 0:
            query = f"SELECT {seletor} FROM {nome_tabela} LIMIT {limite}"
        else:
            query = f"SELECT {seletor} FROM {nome_tabela} ORDER BY {campo_ordenacao} DESC LIMIT {abs(limite)}"

        cursor.execute(query)
        dados = cursor.fetchall()

        # Inverte se limite negativo (para manter ordem original)
        if limite and limite < 0:
            dados.reverse()

        # Se for uma coluna única, achata a lista (tirar do formato [(a,), (b,), (c,)] → [a, b, c])
        if coluna:
            dados = [d[0] for d in dados]

    return dados



















def deletar_registro(nome_banco, nome_tabela, coluna_chave, valor_chave):
    """
    Deleta um registro da tabela onde coluna_chave == valor_chave.

    Parâmetros:
    - nome_banco (str): caminho do banco SQLite.
    - nome_tabela (str): nome da tabela.
    - coluna_chave (str): coluna usada como filtro (ex: 'id' ou 'nome').
    - valor_chave: valor para identificar o registro a deletar.
    """
    with sqlite3.connect(nome_banco) as conn:
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {nome_tabela} WHERE {coluna_chave} = ?", (valor_chave,))
        conn.commit()
        print(f"Registro(s) com {coluna_chave} = '{valor_chave}' removido(s).")















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

















def limpar_tabela(banco, nome_tabela):
    try:
        conn = sqlite3.connect(banco)
        cursor = conn.cursor()

        cursor.execute(f'DELETE FROM {nome_tabela};')

        cursor.execute(f'DELETE FROM sqlite_sequence WHERE name="{nome_tabela}";')

        conn.commit()
        conn.close()
        print(f'Tabela "{nome_tabela}" limpa com sucesso.')
    except sqlite3.Error as e:
        print(f'Erro ao limpar a tabela: {e}')

























def listar_tabelas(caminho_db='dados.db'):
    try:
        conn = sqlite3.connect(caminho_db)
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        tabelas = cursor.fetchall()

        conn.close()

        return [t[0] for t in tabelas]

    except sqlite3.Error as e:
        print(f'Erro ao listar tabelas: {e}')
        return []
    


























def listar_colunas(banco, nome_tabela):
    try:
        conn = sqlite3.connect(banco)
        cursor = conn.cursor()

        cursor.execute(f'PRAGMA table_info({nome_tabela});')
        colunas_info = cursor.fetchall()

        conn.close()

        nomes_colunas = [coluna[1] for coluna in colunas_info]
        return nomes_colunas

    except sqlite3.Error as e:
        print(f'Erro ao listar colunas da tabela "{nome_tabela}": {e}')
        return []










