from re import T
import pandas as pd
import numpy as np
import file_util

def cargar_datos(name):

    df = pd.read_csv("http://bigdata-cluster4-01.virtual.uniandes.edu.co:50070/webhdfs/v1/datalakeBI/dimension_" + name + ".csv?op=OPEN&user.name=cursobi20", sep=',', encoding = 'latin-1', index_col=False)

    numerics = df.select_dtypes(include=np.number).columns.tolist()
    
    if name == "stock_item":
        df["WWI_Stock_Item_ID"] = df["Stock_Item_Key"]
        df["Tax_Rate"] = df["Tax_Rate"].str.replace(",","")
        df["Unit_Price"] = df["Unit_Price"].str.replace(",",".")
        df["Recommended_Retail_Price"] = df["Recommended_Retail_Price"].str.replace(",",".")
        df["Typical_Weight_Per_Unit"] = df["Typical_Weight_Per_Unit"].str.replace(",","")
        df["Stock_Item"] = df["Stock_Item"].str.replace("'", "''")
    if name == "customer":
        df["Customer"] = df["Customer"].str.replace("'", "''")
    for j in df:
        if j in numerics:
            df[j].fillna(0, inplace=True)
        else:
            df[j].fillna("N/A", inplace=True)

    df.to_csv('./data/dimension_' + name + '.csv' , encoding = 'latin-1', sep=',', index=False)


dimensions=["city", "customer", "date", "employee", "stock_item"]

for i in dimensions:
    cargar_datos(i)


