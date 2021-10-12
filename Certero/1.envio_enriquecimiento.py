import pandas as pd
import sys
import datetime
import numpy as np

sys.path.append("/home/tetotille/Escritorio/MOPC")
from utils.functions_ia import tipo_doc_1, standard_dni, dni_characters, cuit,dni_zero, tipo_doc_2


# PARA CERTERO
path_enr = "Datos_Enriquecimiento/Datos_Enriquecimiento_"+str(datetime.date.today())+".csv"
try:
    df = pd.read_csv(path_enr)
except:
    print("No se encontró el archivo, puede que los datos no sean de hoy.\nIngrese una fecha válida o presione enter para salir. (AAAA-MM-DD)")
    fecha_nueva = input()
    if fecha_nueva == '': sys.exit()
    path_enr = "Datos_Enriquecimiento/Datos_Enriquecimiento_"+fecha_nueva+".csv"
    df = pd.read_csv(path_enr)


df = df.drop(["ESTADO","IDCLIENTE","DATOADIC","FILTROA"],axis=1)
df = df.reindex(columns=["IDMOROSO","DOCUMENTO","NOMBRE","TIPO"])
df_juridica = df[df["TIPO"]=="CUIT"]
df_fisica = df[df["TIPO"]!="CUIT"]

path1 = "Datos_mandados/enriquecimiento_fis_"+str(datetime.date.today())+".csv"
path2 = "Datos_mandados/enriquecimiento_jur_"+str(datetime.date.today())+".csv"

df_fisica = df_fisica.dropna(subset=['DOCUMENTO'])

df_fisica = df_fisica[df_fisica['TIPO']!='LE']
df_fisica = df_fisica[df_fisica['TIPO']!='LC']
df_fisica.to_csv(path1,index=False)
df_juridica.to_csv(path2,index=False)

print("Los archivos fueron creado con éxito")
print("Se guardaron en:")
print(path1)
print(path2)
