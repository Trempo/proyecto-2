
# city insertion
def insert_query_departamento(**kwargs):
    from utils.file_util import cargar_datos

    insert = f"INSERT INTO departamento (dpto_codigo,dpto_nombre) VALUES "
    insertQuery = "DELETE FROM departamento;\n"
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.dpto_codigo},\'{row.dpto_nombre}\');\n"
    return insertQuery

# customer insertion
def insert_query_entidad(**kwargs):
    from utils.file_util import cargar_datos

    # To Do
    insert = f"INSERT INTO entidad (entidad_codigo,entidad_nombre) VALUES "
    insertQuery = "DELETE FROM entidad;\n DELETE FROM fact_indicador;\n"
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.entidad_codigo},\'{row.entidad_nombre}\');\n"
    
    insert2 = f"INSERT INTO fact_indicador (nombre,subcategoria,dato_numerico,dato_cualitativo,unidad,fuente,fecha_key,dpto_key,entidad_key) VALUES "
    dataframe2 =cargar_datos("fact_indicador")
    for index, row2 in dataframe2.iterrows():
        insertQuery += insert2 + f"(\'{row2.nombre}\',\'{row2.subcategoria}\',{row2.dato_numerico},\'{row2.dato_cualitativo}\',\'{row2.unidad}\',\'{row2.fuente}\',{row2.fecha_key},{row2.dpto_key},{row2.entidad_key});\n"
    
    return insertQuery

# date insertion
def insert_query_date(**kwargs):
    from utils.file_util import cargar_datos

    # To Do
    insert = f"INSERT INTO date_table (Date_key,Calendar_Month_Number,Calendar_Year) VALUES "
    insertQuery = "DELETE FROM date_table;\n"
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"(\'{row.Date_key}\',{row.Calendar_Month_Number},{row.Calendar_Year});\n"
    return insertQuery

"""
# fact order insert
def insert_query_fact_indicador(**kwargs):
    from utils.file_util import cargar_datos

    # To Do
    insert = f"INSERT INTO fact_indicador (nombre,subcategoria,dato_numerico,dato_cualitativo,unidad,fuente,fecha_key,dpto_key,entidad_key) VALUES "
    insertQuery = "DELETE FROM fact_indicador;\n"
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"(\'{row.nombre}\',\'{row.subcategoria}\',{row.dato_numerico},\'{row.dato_cualitativo}\',\'{row.unidad}\',\'{row.fuente}\',{row.fecha_key},{row.dpto_key},{row.entidad_key});\n"
    return insertQuery
    """   
