import logging

import psycopg2

from config import *


logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s - %(message)s')

def database_creation():
	while True:
		try:
			logging.info(f"Conectando con la base de datos '{DB}'.")
			connection = psycopg2.connect(
				dbname=DB, user=USER, password=PASSWORD, host=HOST, port=PORT)
		except psycopg2.OperationalError:
			try:
				connection = psycopg2.connect(
					user=USER, password=PASSWORD, host=HOST, port=PORT)
			except psycopg2.OperationalError:
				logging.error('Datos introducidos incorrectos.')
				break
			else:
				logging.info(f"Base de datos '{DB}' no encontrada.")
				connection.autocommit = True
				cursor = connection.cursor()
				cursor.execute(f"CREATE DATABASE {DB}")
				logging.info(f"Base de datos '{DB}' creada correctamente.")
		else:
			cursor=connection.cursor()
			try:
				with open("ddl.sql", 'r') as file:
					data = file.read()
					cursor.execute(data)
			except psycopg2.errors.DuplicateTable:
				logging.info("Tablas existentes actualmente.")
			else:
				logging.info("Las tablas se han creado correctamente.")
			finally:
				connection.commit()
				connection.close() 
				break

if __name__ == "__main__":

	database_creation()