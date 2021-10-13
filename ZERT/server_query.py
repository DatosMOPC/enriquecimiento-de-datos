import pandas as pd
import sys
import datetime

sys.path.append("/home/marcos-rago/Documents/IA/")
from utils.functions_ia import *
#Enriquecimiento de datos Masivo
## Trae los datos en el formato de envío que generalmente es
#  IDMOROSO     NOMBRE      IDCLIENTE       ESTADO      FILTROA     DATOADIC

df = pd.read_csv(path)
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

#export
df_export = df.rename(columns={'NRO_DOC':'DOCUMENTO','TIPO_DOC':'TIPO', 'NOMBRE':'NOMBRE'})

fecha = datetime.today()

df_export.to_csv('Datos_Enriquecimiento_2021-10-13')
