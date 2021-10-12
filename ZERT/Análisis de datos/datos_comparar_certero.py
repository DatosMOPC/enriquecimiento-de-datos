import pandas as pd
import sys
import datetime

sys.path.append("/home/marcos-rago/Documents/IA/")
from utils.functions_ia import *

cnxn = connect_database("active")
query = """WITH CONTACTABILIDAD AS (
	SELECT IDMOROSO,TIPO_DOC ,NRO_DOC FROM Active.dbo.DAM_CONTACTABILIDAD_FILTRADA dcf
	GROUP BY IDMOROSO,TIPO_DOC, NRO_DOC
)
SELECT c.IDMOROSO, c.IDCLIENTE, dcf.TIPO_DOC, dcf.NRO_DOC, m.DATOADIC
FROM Active.dbo.CARTERA c
LEFT JOIN CONTACTABILIDAD dcf
ON dcf.IDMOROSO = c.IDMOROSO
LEFT JOIN Active.dbo.MOROSO m
ON m.IDMOROSO = c.IDMOROSO
WHERE c.FASIGNACION > '2021-01-01'
AND c.IDCLIENTE IN ('FRANCES_2012', 'BSR_TARDIA', 'AMEX_ALHR','AMEX_ALLR','AMEX_PALM','AMEX_PARM','AMEX_PBLM',
'AMEX_PBRM','AMEX_PELM','AMEX_PFLM','AMEX_PNCM','AMEX_QRNM','AMEX_QRPM','AMEX_TNCM','AMEX_TNLM','AMEX_TNRM',
'HSBC_LEGALES14', 'HSBC_LEGALES_TEMP', 'BBVA_LEGALES_PREJU', 'BBVA_LEGALES_PREJU_5', 'HSBC_FOGAR')
AND dcf.NRO_DOC IS NOT NULL"""

df = pd.read_sql(query, cnxn).dropna(how = "all")
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


filename_envio ="Datos_Enriquecimiento.xlsx".format(datetime.datetime.now().strftime('%d%m%y'))
df_export.to_excel(filename_envio, index = None)

send_mail(subject="Enriquecimiento MOPC",
 body="Los datos para enriquecimiento.",
 receivers=["jorge.tilleria@mopc.cc"], files= [filename_envio])

os.remove(filename_envio)
