import mysql.connector
import pandas as pd
import numpy as np
from config import config

def create_database(cursor, database_name):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")

def create_tables(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS restaurants (
        id INT AUTO_INCREMENT PRIMARY KEY,
        nombre TEXT NOT NULL,
        descripcion TEXT NOT NULL,
        pais TEXT,
        municipio TEXT,
        departamento TEXT,
        direccion TEXT,
        latitud DECIMAL(10, 8),
        longitud DECIMAL(11, 8),
        telefono TEXT,
        email TEXT,
        url TEXT,
        red_social TEXT,
        image TEXT,
        resena TEXT,
        hora_apertura TEXT,
        menu TEXT,
        destacado TINYINT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS categories (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS subcategories (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        category_id INT,
        FOREIGN KEY (category_id) REFERENCES categories(id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS restaurant_category (
        restaurant_id INT,
        category_id INT,
        PRIMARY KEY (restaurant_id, category_id),
        FOREIGN KEY (restaurant_id) REFERENCES restaurants(id),
        FOREIGN KEY (category_id) REFERENCES categories(id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS restaurant_subcategory (
        restaurant_id INT,
        subcategory_id INT,
        PRIMARY KEY (restaurant_id, subcategory_id),
        FOREIGN KEY (restaurant_id) REFERENCES restaurants(id),
        FOREIGN KEY (subcategory_id) REFERENCES subcategories(id)
    )
    """)

def insert_if_not_exists(cursor, table, column, value):
    cursor.execute(f"SELECT id FROM {table} WHERE {column} = %s", (value,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        try:
            cursor.execute(f"INSERT INTO {table} ({column}) VALUES (%s)", (value,))
            return cursor.lastrowid
        except mysql.connector.Error as err:
            print("Error al insertar:", err)
            return None
        
def insert_subcategory(cursor, subcategory_name, category_id):
    cursor.execute("SELECT id FROM subcategories WHERE name = %s AND category_id = %s", (subcategory_name, category_id))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        try:
            cursor.execute("INSERT INTO subcategories (name, category_id) VALUES (%s, %s)", (subcategory_name, category_id))
            return cursor.lastrowid
        except mysql.connector.Error as err:
            print("Error al insertar subcategoría:", err)
            return None
    
def upload_dataframe(cursor, df):
    query = """
    INSERT INTO restaurants (nombre, descripcion, pais, municipio, departamento, direccion, latitud, longitud, telefono, email, url, red_social, image, resena, hora_apertura, menu, destacado)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for _, row in df.iterrows():
        
        category_id = insert_if_not_exists(cursor, 'categories', 'name', row['categoria'])
        subcategory_id = insert_subcategory(cursor, row['subcategoria'], category_id)

        cursor.execute(query, (
            row['nombre'], row['descripcion'], row['pais'], row['municipio'], row['departamento'], 
            row['direccion'], row['latitud'], row['longitud'], row['telefono'], row['email'], 
            row['url'], row['red_social'], row['image'], row['resena'], row['hora_apertura'], 
            row['menu'], row['destacado']
        ))

        restaurant_id = cursor.lastrowid

        cursor.execute("""
            INSERT INTO restaurant_category (restaurant_id, category_id)
            VALUES (%s, %s)
        """, (restaurant_id, category_id))

        cursor.execute("""
            INSERT INTO restaurant_subcategory (restaurant_id, subcategory_id)
            VALUES (%s, %s)
        """, (restaurant_id, subcategory_id))

username = config['development'].MYSQL_USER
password = config['development'].MYSQL_PASSWORD
server = config['development'].MYSQL_HOST
database = config['development'].MYSQL_DB

df = pd.read_excel('full.xlsx')
df = df.replace({pd.NaT: None, np.nan: None})

connection = mysql.connector.connect(host=server, user=username, password=password)
cursor = connection.cursor()

create_database(cursor, database)
connection.commit()

connection.database = database

create_tables(cursor)
upload_dataframe(cursor, df)

connection.commit()
cursor.close()
connection.close()


