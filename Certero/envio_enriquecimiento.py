import pandas as pd
import sys
import datetime
import numpy as np

sys.path.append("/home/tetotille/Escritorio/MOPC")
from utils.functions_ia import tipo_doc_1, standard_dni, dni_characters, cuit,dni_zero, tipo_doc_2


# PARA CERTERO
df = pd.read_csv("Datos_Enriquecimiento.csv")
df = df.drop(["ESTADO","IDCLIENTE","DATOADIC","FILTROA"],axis=1)
df = df.reindex(columns=["IDMOROSO","DOCUMENTO","NOMBRE","TIPO"])
df_juridica = df[df["TIPO"]=="CUIT"]
df_fisica = df[df["TIPO"]!="CUIT"]

path1 = "Datos_mandados/enriquecimiento_fis_"+str(datetime.date.today())+".csv"
path2 = "Datos_mandados/enriquecimiento_jur_"+str(datetime.date.today())+".csv"

df_fisica.to_csv(path1,index=False)
df_juridica.to_csv(path2,index=False)

print("Los archivos fueron creado con éxito")
print("Se guardaron en:")
print(path1)
print(path2)
