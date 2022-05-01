import os
import logging
import datetime
import locale

import requests
import pandas as pd
from sqlalchemy import create_engine

from config import *
from ddl import *


logging.basicConfig(level=logging.INFO, format="%(asctime)s: %(levelname)s - %(message)s")

locale.setlocale(locale.LC_ALL, "es")
dt = datetime.datetime.now()
date_1 = dt.strftime("%Y-%B")
date_2 = dt.strftime("%d-%m-%Y")

logging.info("Obteniendo datos y guardandolos.")
file = requests.get(MUSEOS)
os.makedirs(f"museos/{date_1}", exist_ok=True)
open(f"museos/{date_1}/museos-{date_2}.csv", "wb").write(file.content)
file = requests.get(CINES)
os.makedirs(f"cines/{date_1}", exist_ok=True)
open(f"cines/{date_1}/cines-{date_2}.csv", "wb").write(file.content)
file = requests.get(BIBLIOTECAS)
os.makedirs(f"bibliotecas/{date_1}", exist_ok=True)
open(f"bibliotecas/{date_1}/bibliotecas-{date_2}.csv", "wb").write(file.content)

logging.info("Normalizando datos.")
df_museums = pd.read_csv(f"museos/{date_1}/museos-{date_2}.csv").set_axis(["cod_localidad", 
	"id_provincia", "id_departamento", "observaciones", "categoria", "subcategoria", 
	"provincia", "localidad", "nombre", "domicilio", "piso", "cod_postal", "cod_area", 
	"num_telefono", "mail", "web", "latitud", "longitud", "tipolatitudlongitud", 
	"inf_adicional", "fuente", "jurisdiccion", "año_inauguracion", "año_actualizacion"], 
	axis="columns")
df_cinemas = pd.read_csv(f"cines/{date_1}/cines-{date_2}.csv").set_axis(["cod_localidad", 
	"id_provincia", "id_departamento", "observaciones", "categoria", "provincia", 
	"departamento", "localidad", "nombre", "domicilio", "piso", "cod_postal", "cod_area", 
	"num_telefono", "mail", "web", "inf_adicional", "latitud", "longitud", 
	"tipolatitudlongitud", "fuente", "tipo_gestion", "pantallas", "butacas", "espacio_incaa", 
	"año_actualizacion"], axis="columns")
df_libraries = pd.read_csv(f"bibliotecas/{date_1}/bibliotecas-{date_2}.csv").set_axis(
	["cod_localidad", "id_provincia", "id_departamento", "observaciones", "categoria", 
	"subcategoria", "provincia", "departamento", "localidad", "nombre", "domicilio", "piso", 
	"cod_postal", "cod_area", "num_telefono", "mail", "web", "inf_adicional", "latitud", 
	"longitud", "tipolatitudlongitud", "fuente", "tipo_gestion", "año_inauguracion", 
	"año_actualizacion"], axis="columns")
logging.info("Concatenando tablas.")
concat_table = pd.concat([df_museums, df_cinemas, df_libraries], axis=0)
logging.info("Generando tabla 1.")
main_table = concat_table.loc[:,["cod_localidad", "id_provincia", "id_departamento", 
	"categoria", "provincia", "localidad", "nombre", "domicilio", "cod_postal", 
	"num_telefono", "mail", "web"]]

logging.info("Generando tabla 2.")
records_table_1 = concat_table.groupby(['categoria']).size().to_frame(
	name = "registros_categoria")
records_table_2 = concat_table.groupby(['categoria', 'fuente']).size().to_frame(
	name = "registros_fuente")
records_table_3 = concat_table.groupby(['categoria', 'provincia']).size().to_frame(
	name = "registros_provincia_categoria")
records_table = records_table_1.merge(
	records_table_2, how='outer', left_index=True, right_index=True)
records_table = records_table.merge(
	records_table_3, how='outer', left_index=True, right_index=True)
records_table.reset_index(inplace=True)
records_table.set_index("categoria", inplace=True)
records_table = records_table.loc[:,["registros_categoria", "fuente", "registros_fuente", 
	"provincia", "registros_provincia_categoria"]]

logging.info("Generando tabla 3.")
cinema_inventory_table = df_cinemas.loc[:,["provincia", "pantallas", "butacas", "espacio_incaa"]]
cinema_inventory_table['espacio_incaa'] = cinema_inventory_table["espacio_incaa"].replace(["SI","si"], 1).fillna(0).astype("int")
cinema_inventory_table = cinema_inventory_table.groupby("provincia").sum()


engine = create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}")

main_table=main_table.assign(fecha_carga=date_2)
records_table=records_table.assign(fecha_carga=date_2)
cinema_inventory_table=cinema_inventory_table.assign(fecha_carga=date_2)

try:
	engine.execute("DELETE FROM tabla_principal")
	engine.execute("DELETE FROM tabla_registros")
	engine.execute("DELETE FROM tabla_inventario_cines")
	engine.execute("ALTER SEQUENCE tabla_registros_indice_seq RESTART WITH 1")
	engine.execute("ALTER SEQUENCE tabla_principal_indice_seq RESTART WITH 1")
except:
	database_creation()
else:
	logging.info("Actualizando base de datos.")
	main_table.to_sql(
		name="tabla_principal",	con=engine,	if_exists="append",	index=False)
	records_table.to_sql(
		name="tabla_registros",	con=engine,	if_exists="append",	index=True)
	cinema_inventory_table.to_sql(
		name="tabla_inventario_cines",	con=engine,	if_exists="append",	index=True)
