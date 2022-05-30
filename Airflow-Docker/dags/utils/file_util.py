import pandas as pd
#import dload


"""def crear_archivos():
    
    df = pd.read_excel("/opt/airflow/data/TerriData_Dim4.xlsx")
    
    df = df[df['Código Departamento'].notna()]
    df = df[df['Código Entidad'].notna()]
    df = df[df['Mes'].notna()]
    df = df[df['Año'].notna()]

    df_dpto = pd.DataFrame()
    df_dpto["dpto_codigo"] = df["Código Departamento"].astype('int')
    df_dpto['dpto_nombre'] = df['Departamento']
    df_dpto.drop_duplicates(inplace=True)
    guardar_datos(df_dpto, "dimension_departamento")
    
    df_entidad = pd.DataFrame()
    df_entidad["entidad_codigo"] = df["Código Entidad"].astype('int')
    df_entidad['entidad_nombre'] = df['Entidad']
    df_entidad.drop_duplicates(inplace=True)
    guardar_datos(df_entidad, "dimension_entidad")
    
    df_fecha = pd.DataFrame()
    df_fecha["Calendar_Month_Number"] = df["Mes"]
    df_fecha["Calendar_Year"] = df["Año"]
    df_fecha["Date_key"] = df["Mes"].astype(str) + '-' + df.pop('Año').astype(str)
    df_fecha.drop_duplicates(inplace=True)
    guardar_datos(df_fecha, "dimension_date_table")



    df_indicador = pd.DataFrame()
    df_indicador["nombre"] = df["Indicador"]
    df_indicador["subcategoria"] = df["Subcategoría"]
    df_indicador["dato_numerico"] = df["Dato Numérico"]
    df_indicador["dato_cualitativo"] = df["Dato Cualitativo"]
    df_indicador["unidad"] = df["Unidad de Medida"]
    df_indicador["fuente"] = df["Fuente"]
    df_indicador["fecha_key"] = df_fecha["Date_key"]
    df_indicador["dpto_key"] = df["Código Departamento"].astype('int')
    df_indicador["entidad_key"] = df["Código Entidad"].astype('int')
    guardar_datos(df_indicador, "fact_indicador")


def guardar_datos(df, nombre):
    df.to_csv('/opt/airflow/data/' + nombre + '.csv' , encoding = 'utf-8', sep=',', index=False)

def descargar_datos():
    dload.save_unzip("https://terridata.dnp.gov.co/assets/docs/excel/dimensiones/TerriData_Dim4.xlsx.zip", '/opt/airflow/data/', delete_after=True)
"""
def cargar_datos(name):
    df = pd.read_csv('/opt/airflow/data/' + name + '.csv', encoding = 'utf-8', sep=',')
    return df