import pandas as pd
import numpy as np
def cargar_datos(name):

    #df = pd.read_csv("http://bigdata-cluster4-01.virtual.uniandes.edu.co:50070/webhdfs/v1/datalakeBI/" + name + ".csv?op=OPEN&user.name=cursobi20", sep=',', encoding = 'latin-1', index_col=False)
    df = pd.read_csv('/opt/airflow/data/' + name + '.csv' , encoding = 'latin-1', sep=',', index_col=False)

    #numerics = df.select_dtypes(include=np.number).columns.tolist()
    
    """if name == "dimension_stock_item":
        df = df.rename(columns={"Stock_Item":"WWI_Stock_Item_ID"})
        df["Tax_Rate"] = df["Tax_Rate"].str.replace(",","")
        df["Unit_Price"] = df["Unit_Price"].str.replace(",",".")
        df["Recommended_Retail_Price"] = df["Recommended_Retail_Price"].str.replace(",",".")
        df["Typical_Weight_Per_Unit"] = df["Typical_Weight_Per_Unit"].str.replace(",","")
        df["WWI_Stock_Item_ID"] = df["WWI_Stock_Item_ID"].str.replace("'", "''")
    if name == "dimension_customer":
        df["Customer"] = df["Customer"].str.replace("'", "''")
    for j in df:
        if j in numerics:
            df[j].fillna(0, inplace=True)
        else:
            df[j].fillna("N/A", inplace=True)
    """
    return df


def guardar_datos(df, nombre):
    df.to_csv('/opt/airflow/data/' + nombre + '.csv' , encoding = 'latin-1', sep=',', index=False)
