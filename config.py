from decouple import config


USER = config("POSTGRESQL_USER", default="postgres", cast=str)
PASSWORD = config("POSTGRESQL_PASSWORD")
HOST = config("POSTGRESQL_HOST", default="localhost", cast=str)
PORT = config("POSTGRESQL_PORT", default="5432", cast=str)
DB = config("POSTGRESQL_DB", default="postgres", cast=str)

BIBLIOTECAS = config("URL_BIBLIOTECAS")
CINES = config("URL_CINES")
MUSEOS = config("URL_MUSEOS")