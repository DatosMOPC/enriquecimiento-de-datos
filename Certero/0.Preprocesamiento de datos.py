import pandas as pd
import sys
import datetime
import numpy as np

sys.path.append("/home/tetotille/Escritorio/MOPC")
from utils.functions_ia import tipo_doc_1, standard_dni, dni_characters, cuit,dni_zero, tipo_doc_2

path_enr = "Datos_Enriquecimiento/Datos_Enriquecimiento_"+str(datetime.date.today())+".csv"
try:
    df = pd.read_csv(path_enr)
except:
    print("No se encontró el archivo, puede que los datos no sean de hoy.\nIngrese una fecha válida o presione enter para salir. (AAAA-MM-DD)")
    fecha_nueva = input()
    if fecha_nueva == '': sys.exit()
    path_enr = "Datos_Enriquecimiento/Datos_Enriquecimiento_"+fecha_nueva+".csv"
    df = pd.read_csv(path_enr)
df.columns = [c.upper().strip() for c in df.columns] # TODO QUIERO EN MAYÚSCULA

df['IDMOROSO'] = df['IDMOROSO'].str.strip()

df.loc[(df['IDCLIENTE'].str.startswith('AMEX')) , 'NRO_DOC'] = df['DATOADIC']
df.loc[~(df['IDCLIENTE'].str.startswith('AMEX')) , 'NRO_DOC'] = df['IDMOROSO']

df = df.dropna(subset=['NRO_DOC'])

df['NRO_DOC']=df['NRO_DOC'].str.strip()

#implementamos las funciones de limpieza de documento
df['TIPO_DOC'] = df['NRO_DOC'].map(tipo_doc_1)
df['NRO_DOC'] = df['NRO_DOC'].map(standard_dni)
df['NRO_DOC'] = df['NRO_DOC'].map(dni_characters)
df['NRO_DOC'] = df['NRO_DOC'].map(cuit)
df['NRO_DOC'] = df['NRO_DOC'].map(dni_zero)

#Para corregir el TIPO DE DOCUMENTO sobre el dataframe resultante
df['TIPO_DOC'] = df.apply(lambda x: tipo_doc_2(x.NRO_DOC, x.TIPO_DOC), axis=1)

#Acá hay que eliminar los duplicados
df = df.drop_duplicates()

df = df.dropna(subset=['NRO_DOC'])

#export
df_export = df.rename(columns={'NRO_DOC':'DOCUMENTO','TIPO_DOC':'TIPO', 'NOMBRE':'NOMBRE'})

df_export.to_csv(path_enr)

print(f'El archivo en la dirección {path_enr} se actualizó con los DNI y el tipo de dato.')
