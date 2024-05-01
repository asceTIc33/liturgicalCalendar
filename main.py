import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import requests
from bs4 import BeautifulSoup

def connectToDatabase(host, dbname, user, password):
    cnxnString = f"host='{host}' dbname='{dbname}' user='{user}' password='{password}'"
    conn = psycopg2.connect(cnxnString)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    print("Conexão com o banco de dados estabelecida com sucesso")
    return conn

def createDatabase(conn, newName):
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE {newName}")
    print("Banco de dados criado com sucesso")
    conn.close()
    print("Conexão com o banco de dados fechada")

def connectToNewDatabase(host, newDbName, user, password):
    cnxnString = f"host='{host}' dbname='{newDbName}' user='{user}' password='{password}'"
    conn = psycopg2.connect(cnxnString)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    print("Conexão com o banco de dados estabelecida com sucesso")
    return conn

def createTable(conn, tableName):
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {tableName} (
            id SERIAL PRIMARY KEY,
            dia VARCHAR,
            mes VARCHAR,
            ano VARCHAR,
            dia_semana VARCHAR,
            ciclo VARCHAR,
            numero_semana VARCHAR,
            tempo_liturgico VARCHAR,
            cor_liturgica VARCHAR,
            tipo_data_liturgica VARCHAR,
            data_liturgica VARCHAR,
            nota VARCHAR
        )
    """)
    print("Tabela criada com sucesso")

def scrapeWebsite(url, headers):
    request = requests.get(url, headers=headers)
    site = BeautifulSoup(request.text, "html.parser")
    data = []
    rows = site.find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        data.append([cell.get_text(strip=True) for cell in cells])
    return data

def insertData(conn, tableName, data):
    cur = conn.cursor()
    for row in data:
        if len(row) == 11:
            cur.execute(f"""
                INSERT INTO {tableName}
                (dia, mes, ano, dia_semana, ciclo, numero_semana, tempo_liturgico, cor_liturgica, tipo_data_liturgica, data_liturgica, nota)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, row)
        else:
            print("Número incorreto de elementos na linha:", row)
    print("Dados inseridos com sucesso")

host = 'substituir'
dbname = 'substituir'
user = 'substituir'
password = 'substituir'
newDbName = 'substituir'

connMain = connectToDatabase(host, dbname, user, password)
createDatabase(connMain, newDbName)

connNew = connectToNewDatabase(host, newDbName, user, password)

tableName = 'calendario_liturgico'

createTable(connNew, tableName)

url = "https://www.sagradaliturgia.com.br/index2.php"
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"}

dataFromSite = scrapeWebsite(url, headers)

insertData(connNew, tableName, dataFromSite)

connMain.close()
connNew.close()
