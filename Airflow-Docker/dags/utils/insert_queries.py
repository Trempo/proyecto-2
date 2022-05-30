from utils.file_util import cargar_datos

# city insertion
def insert_query_departamento(**kwargs):
    insert = f"INSERT INTO departamento (dpto_codigo,dpto_nombre) VALUES "
    insertQuery = "DELETE FROM departamento;\n"
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.dpto_codigo},\'{row.dpto_nombre}\');\n"
    return insertQuery

# customer insertion
def insert_query_entidad(**kwargs):
    # To Do
    insert = f"INSERT INTO entidad (entidad_codigo,entidad_nombre) VALUES "
    insertQuery = "DELETE FROM entidad;\n"
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.entidad_codigo},\'{row.entidad_nombre}\');\n"
    return insertQuery

# date insertion
def insert_query_date(**kwargs):
    # To Do
    insert = f"INSERT INTO date_table (Date_key,Calendar_Month_Number,Calendar_Year) VALUES "
    insertQuery = "DELETE FROM date_table;\n"
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"(\'{row.Date_key}\',{row.Calendar_Month_Number},{row.Calendar_Year});\n"
    return insertQuery
    
# fact order insert
def insert_query_fact_indicador(**kwargs):
    # To Do
    insert = f"INSERT INTO fact_indicador (nombre,subcategoria,dato_numerico,dato_cualitativo,unidad,fuente,fecha_key,dpto_key,entidad_key) VALUES "
    insertQuery = "DELETE FROM fact_indicador;\n"
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.order_key},\'{row.nombre}\',\'{row.subcategoria}\',{row.dato_numerico},\'{row.dato_cualitativo}\',\'{row.unidad}\',\'{row.fuente}\',{row.fecha_key},{row.dpto_key},{row.entidad_key});\n"
    return insertQuery
