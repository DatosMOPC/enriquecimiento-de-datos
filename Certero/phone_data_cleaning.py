import pandas as pd
import numpy as np
from datetime import date


def procesar(path_sumario,path_enriquecimiento,tipo):
    df = pd.read_csv(path_sumario)

    # ## 1.2) Data Cleaning: Datasets
    # Vamos a dropear aquellas columnas y filas que no tengan ningún valor (es decir, que sean todos NaNs)
    df = df.dropna(axis=0, how='all')
    #df = df.dropna(axis=1, how='all')

    # Ahora todas las filas tienen al menos 1 dato.

    # # 2) Teléfonos
    # Vamos a obtener los teléfonos de las columnas donde pueden aparecer, que son:
    #
    # - Teléfono Fijo 1, 2 y 3.
    # - Teléfono Movil 1, 2, 3, 4, 5, 6 y 7.
    # - Teléfono Laboral 1, 2 y 3.
    # - Contactación Opcional (Familiar): Tel Fijo 1, Tel Fijo 2, Celular 1, Celular 2, Celular 3, Celular 4, Celular 5.
    # - Contactación Opcional (Vecino): Telefono.
    if tipo == 1:
        df_reduced = df[["DOCUMENTO","CUIL","APELLIDO","NOMBRE","TELEFONO_CELULAR1","TELEFONO_CELULAR2",
                        "TELEFONO_CELULAR3","TELEFONO_CELULAR4","TELEFONO_FIJO1","TELEFONO_FIJO2",
                        "TELEFONO_FIJO3","TELEFONO_FIJO4"]]

    # Eliminamos el multilevel de las columnas, y las renombramos:
        df_reduced = df_reduced.rename(columns={"CUIL": "CUIT_CUIL", 'DOCUMENTO': 'NRO_DOC'})
    else:
        df_reduced = df[["#ID","CUIT","DENOMINACION","TELEFONO_CELULAR1","TELEFONO_CELULAR2",
                        "TELEFONO_CELULAR3","TELEFONO_CELULAR4","TELEFONO_FIJO1","TELEFONO_FIJO2",
                        "TELEFONO_FIJO3","TELEFONO_FIJO4"]]
        df_reduced = df_reduced.rename(columns={"CUIT": "CUIT_CUIL", '#ID': 'ID'})

    # # 3) Concatenación de Teléfonos

    # Como PyP envía un teléfono por celda, tengo que concatenar las columnas de teléfonos en una lista dentro de una nueva columna denominada "Dato"
    if tipo == 1:
        df_melt = pd.melt(df_reduced, id_vars=['CUIT_CUIL', 'NRO_DOC', 'APELLIDO','NOMBRE']
                        , value_vars=['TELEFONO_CELULAR1',
            'TELEFONO_CELULAR2', 'TELEFONO_CELULAR3', 'TELEFONO_CELULAR4',
            'TELEFONO_FIJO1', 'TELEFONO_FIJO2', 'TELEFONO_FIJO3', 'TELEFONO_FIJO4'],value_name='Dato')
    else:
        df_melt = pd.melt(df_reduced, id_vars=['CUIT_CUIL', 'ID', 'DENOMINACION']
                        , value_vars=['TELEFONO_CELULAR1',
            'TELEFONO_CELULAR2', 'TELEFONO_CELULAR3', 'TELEFONO_CELULAR4',
            'TELEFONO_FIJO1', 'TELEFONO_FIJO2', 'TELEFONO_FIJO3', 'TELEFONO_FIJO4'],value_name='Dato')

    # # 4) Drop NaNs

    # Dropeamos aquellas filas que tengan NaN en la columna Dato, ya que no nos aporta nada:
    df_melt = df_melt.dropna(subset=['Dato'])


    # # 5) Propietario del Dato

    # En función de la columna "variable" creamos una nueva columna PROPIETARIO_DATO
    df_melt.loc[((df_melt['variable'] == 'TELEFONO_FIJO1') | (df_melt['variable'] == 'TELEFONO_FIJO2')
                | (df_melt['variable'] == 'TELEFONO_FIJO3')
                | (df_melt['variable'] == 'TELEFONO_FIJO4') | (df_melt['variable'] == 'TELEFONO_CELULAR1')
                | (df_melt['variable'] == 'TELEFONO_CELULAR2') | (df_melt['variable'] == 'TELEFONO_CELULAR3')
                | (df_melt['variable'] == 'TELEFONO_CELULAR4')
                ) , 'PROPIETARIO_DATO'] = 'Propio'

    """df_melt.loc[((df_melt['variable'] == 'Tel_familiar_fijo_1')
                | (df_melt['variable'] == 'Tel_familiar_fijo_2') | (df_melt['variable'] == 'celular_familiar_1')
                | (df_melt['variable'] == 'celular_familiar_2') | (df_melt['variable'] == 'celular_familiar_3')
                | (df_melt['variable'] == 'celular_familiar_4') | (df_melt['variable'] == 'celular_familiar_5')
                ) , 'PROPIETARIO_DATO'] = 'Ajeno_familiar'
    """
    """
    df_melt.loc[((df_melt['variable'] == 'Telefono_vecino')
                ) , 'PROPIETARIO_DATO'] = 'Ajeno'
    """
    """
    df_melt.loc[((df_melt['variable'] == 'Telefono Laboral 1') | (df_melt['variable'] == 'Telefono Laboral 2')
                | (df_melt['variable'] == 'Telefono Laboral 3')
                ) , 'PROPIETARIO_DATO'] = 'Ajeno_empleador'
    """


    # # 6) Columna Proveedor

    # Agregamos la columna PROVEEDOR_DATO, con el código correspondiente.
    cod_pyp = 'ZERT'
    df_clean = df_melt.assign(PROVEEDOR_DATO=cod_pyp)
    df_clean.tail()


    # # 7) Columna Tipo

    # Agregamos la columna TIPO, con el código correspondiente
    df_clean.loc[((df_melt['variable'] == 'TELEFONO_FIJO1') | (df_melt['variable'] == 'TELEFONO_FIJO2')
                | (df_melt['variable'] == 'TELEFONO_FIJO3')
                | (df_melt['variable'] == 'TELEFONO_FIJO4')
                ) , 'TIPO'] = 'TEL.FIJO'

    df_clean.loc[((df_melt['variable'] == 'TELEFONO_CELULAR1')
                | (df_melt['variable'] == 'TELEFONO_CELULAR2') | (df_melt['variable'] == 'TELEFONO_CELULAR3')
                | (df_melt['variable'] == 'TELEFONO_CELULAR4')
                ) , 'TIPO'] = 'TEL.CELULAR'


    # # 8) Columna Subtipo

    # Agregamos la columna SUBTIPO, que va a contener información sobre el tipo de teléfono que se encuentra en el campo Dato.
    #
    # - Personal: si es propio.
    # - Laboral: si es ajeno.

    """
    df_clean.loc[((df_clean['variable'] == 'Telefono Laboral 1') | (df_clean['variable'] == 'Telefono Laboral 2')
                | (df_clean['variable'] == 'Telefono Laboral 3')
                ) , 'SUBTIPO'] = 'LABORAL'
    """
    df_clean.loc[((df_melt['variable'] == 'TELEFONO_FIJO1') | (df_melt['variable'] == 'TELEFONO_FIJO2')
                | (df_melt['variable'] == 'TELEFONO_FIJO3')
                | (df_melt['variable'] == 'TELEFONO_FIJO4') | (df_melt['variable'] == 'TELEFONO_CELULAR1')
                | (df_melt['variable'] == 'TELEFONO_CELULAR2') | (df_melt['variable'] == 'TELEFONO_CELULAR3')
                | (df_melt['variable'] == 'TELEFONO_CELULAR4')
                ) , 'SUBTIPO'] = 'PERSONAL'

    df_clean = df_clean.drop(columns=['variable'])

    # # 9) Columna Score

    # El campo **Score** se construye en función de los datos obtenidos:
    #
    # - Nivel 4: Dato que otorga la propia persona.
    # - Nivel 3: Dato que otorgan los clientes (los bancos).
    # - Nivel 2: Dato que otorga un proveedor externo (Informes Digitales en este caso), con subtipo "Personal".
    # - Nivel 1: Dato que otorga un proveedor externo, pero tiene como subtipo "Laboral".

    # Nivel 1: Laboral, proveedor externo.

    df_clean.loc[(df_clean['SUBTIPO'] == 'LABORAL') , 'SCORE_DATO'] = 1

    # Nivel 2: Personal, proveedor externo.

    df_clean.loc[(df_clean['SUBTIPO'] == 'PERSONAL') , 'SCORE_DATO'] = 2


    # # 10) Columna Fecha

    # Para el campo FECHA_INGRESO_DATO, usamos la fecha en la que se hace la inserción de los datos:
    fecha_ingreso = pd.to_datetime('today')
    df_clean = df_clean.assign(FECHA_INGRESO_DATO=fecha_ingreso)


    # # 11) Campos adicionales

    # Creamos las columnas VERIFICADO, FECHA_VERIFICACION, OPERADOR_VERIFICACION, que en principio van en nulls.
    df_clean['VERIFICADO'] = 0
    df_clean['FECHA_VERIFICACION'] = np.nan
    df_clean['OPERADOR_VERIFICACION'] = np.nan


    # # 12) Concatenación con .csv enviado

    # Vamos a concatenar nuestro dataframe con el dataframe enviado para enriquecer. Para esto, vamos a hacer una limpieza inicial de cómo se enviaron esos datos. Finalmente, se uniran ambos dataframes por __DNI__.

    # ## 12.1) Dataset

    # Vamos a analizar y limpiar el dataset enviado para el enriquecimiento:
    df_pyp = pd.read_csv(path_enriquecimiento)

    # Creamos una columna que contenga IDMOROSO + DATOADIC según corresponda
    df_pyp.loc[(df_pyp['IDCLIENTE'].str.startswith('AMEX')) , 'NRO_DOC'] = df_pyp['DATOADIC']
    df_pyp.loc[(~df_pyp['IDCLIENTE'].str.startswith('AMEX')) , 'NRO_DOC'] = df_pyp['IDMOROSO']
    #df_pyp ['NRO_DOC'] = df_pyp ['IDMOROSO']
    #df_pyp = df_pyp [['IDMOROSO','NOMBRE','IDCLIENTE','NRO_DOC']]
    df_pyp = df_pyp [['IDMOROSO','IDCLIENTE','NRO_DOC']]
    df_pyp = df_pyp.dropna(subset=['NRO_DOC'])
    df_pyp.tail()


    # ## 12.2) DNI Cleaning

    # Limpiamos el dataframe __df_pyp__, ya que dentro de DATO_A_ENVIAR tenemos 9 tipos de casos diferentes de datos:
    #
    # - DNI 12345678
    # - DNI12345678
    # - DNIE1234678
    # - LC  1234567
    # - LE  1234567
    # - CUIT00123456780
    # - 00123456780 --> Es un CUIT.
    # - 00 12345678 0 --> Es un CUIT.
    # - DNI 00123456780 --> Dice DNI pero es un CUIT de una persona __jurídica__ (TO DO)

    # Para eso definimos 5 funciones:
    #
    # - __Tipo_DOC__: Se definen dos funciones para determinar el tipo de DNI. Una inicial, y otra para corregir sobre el dataframe resultante.
    # - __Standard DNI__: Estandariza los DNI para 7 de los casos. Esta función debe ejecutarse en primer lugar.
    # - __DNI Spaces__: Elimina los espacios.
    # - __DNI Zero__: Elimina los ceros iniciales de ciertos CUITs para que coincidan con los DNI.
    # - __Cuit__: Decisión sobre dejar un cuit o volverlo DNI

    def tipo_doc_1 (string_dni):

        if 'DNI' in string_dni:
            tipo = 'DNI'

        elif 'LC' in string_dni:
            tipo = 'LC'

        elif 'LE' in string_dni:
            tipo = 'LE'

        elif 'DNIE' in string_dni:
            tipo = 'DNIE'

        elif ((string_dni.startswith(('CUIT30','CUIT33','CUIT34'))) & (len(string_dni)>8)):
            tipo = 'CUIT'

        elif ((string_dni.startswith(('30','33','34'))) & (len(string_dni)>8)):
            tipo = 'CUIT'

        elif ((string_dni.startswith(('20','23','24','27'))) & (len(string_dni)>8)):
            tipo = 'DNI'

        elif ((string_dni.startswith(('CUIT20','CUIT23','CUIT24','CUIT27'))) & (len(string_dni)>8)):
            tipo = 'DNI'

        else:
            tipo = 'DNI'

        return tipo


    def tipo_doc_2 (string_dni, tipo_original):

        if (len(string_dni) > 8) & (tipo_original == 'DNI'):
            tipo = 'CUIT'
        else:
            tipo = tipo_original
        return tipo


    def standard_dni (string_dni):
        """DNI separation by each case.
        """

        if 'DNI ' in string_dni:
            dni = string_dni.split('DNI ')[1]

        elif 'DNIE' in string_dni:
            dni = string_dni.split('DNIE')[1]

        elif 'DNI' in string_dni:
            dni = string_dni.split('DNI')[1]

        elif 'LC  ' in string_dni:
            dni = string_dni.split('LC  ')[1]

        elif 'LE  ' in string_dni:
            dni = string_dni.split('LE  ')[1]

        elif 'CUIT' in string_dni:
            dni = string_dni.split('CUIT')[1]

        else:
            dni = string_dni

        return dni


    def dni_characters (string_dni):

        if ' ' in string_dni:
            dni = string_dni.replace(' ','')

        elif '-' in string_dni:
            dni = string_dni.replace('-','')

        else:
            dni = string_dni

        return dni


    def cuit (string_dni):

        if ((string_dni.startswith('20')) & (len(string_dni)>8)):
            cuit = string_dni [2:-1]

        elif ((string_dni.startswith('23')) & (len(string_dni)>8)):
            cuit = string_dni [2:-1]

        elif ((string_dni.startswith('24')) & (len(string_dni)>8)):
            cuit = string_dni [2:-1]

        elif ((string_dni.startswith('27')) & (len(string_dni)>8)):
            cuit = string_dni [2:-1]

        else:
            cuit = string_dni

        return cuit


    def dni_zero (string_dni):

        if string_dni.startswith('0'):
            dni = string_dni [1:]

        else:
            dni = string_dni

        return dni


    # En primer lugar, trimmeamos los espacios que sobran:
    df_pyp['NRO_DOC'] = [(str(df_pyp['NRO_DOC'][i]).strip()) for i in range (df_pyp['NRO_DOC'].count())]


    # Se implementan estas funciones:

    # Para determinar el __TIPO DE DOCUMENTO__ en primer lugar:
    df_pyp['TIPO_DOC'] = [tipo_doc_1(df_pyp['NRO_DOC'][i]) for i in range (df_pyp['NRO_DOC'].count())]


    # Para la limpieza de DNIs:
    df_pyp['NRO_DOC'] = [standard_dni(df_pyp['NRO_DOC'][i]) for i in range (df_pyp['NRO_DOC'].count())]
    df_pyp['NRO_DOC'] = [dni_characters(df_pyp['NRO_DOC'][i]) for i in range (df_pyp['NRO_DOC'].count())]
    df_pyp['NRO_DOC'] = [cuit(df_pyp['NRO_DOC'][i]) for i in range (df_pyp['NRO_DOC'].count())]
    df_pyp['NRO_DOC'] = [dni_zero(df_pyp['NRO_DOC'][i]) for i in range (df_pyp['NRO_DOC'].count())]

    # Para corregir el __TIPO DE DOCUMENTO__ sobre el dataframe resultante
    df_pyp['TIPO_DOC'] = [tipo_doc_2(df_pyp['NRO_DOC'][i],df_pyp['TIPO_DOC'][i]) for i in range (df_pyp['NRO_DOC'].count())]
    df_pyp.tail()


    # # 13) Union de Dataframes

    # Se unen ambos dataframes en la columna NRO_DOC

    # Convertimos la columna NRO_DOC de objeto a INT para hacer la unión
    if tipo == 1:
        df_clean = df_clean.astype({'NRO_DOC': 'str'})
        df_clean ['NRO_DOC'] = df_clean ['NRO_DOC'].str.split('.').str[0]
        df_union = pd.merge(df_clean,df_pyp, on='NRO_DOC', how='inner')
    else:
        df_clean = df_clean.astype({'ID': 'str'})
        df_clean ['ID'] = df_clean ['ID'].str.split('.').str[0]
        df_union = pd.merge(df_clean,df_pyp, left_on='ID',right_on='IDMOROSO', how='inner')

    # # 14) Export

    # Renombramos, dropeamos y reordenamos las columnas
    df_union = df_union.rename(columns = {'Dato':'DATO'})

    # Dropeamos columnas que no nos sirven
    df_export = df_union.drop(columns=['IDCLIENTE'], axis=0)
    df_export = df_export[['IDMOROSO', 'TIPO_DOC','NRO_DOC','CUIT_CUIL','TIPO','SUBTIPO','DATO','PROPIETARIO_DATO','PROVEEDOR_DATO','FECHA_INGRESO_DATO','VERIFICADO','FECHA_VERIFICACION','OPERADOR_VERIFICACION','SCORE_DATO']]

    #df_export['DATO'] = [(str(int(round(df_export['DATO'][i],0)))) for i in range (df_export['DATO'].count())]
    df_export ['DATO'] = df_export ['DATO'].astype(str)
    df_export ['DATO'] = df_export ['DATO'].str.strip()
    df_export ['DATO'] = df_export ['DATO'].str.split('.').str[0]
    df_export['CUIT_CUIL'] = [(str(int(round(df_export['CUIT_CUIL'][i],0)))) for i in range (df_export['CUIT_CUIL'].count())]

    df_export = df_export.drop_duplicates()
    df_duplicates = df_export[df_export.duplicated(keep=False)]


    # ## Dato a str + strip()
    df_export ['DATO'] = df_export['DATO'].str.strip()


    # # Revisión de duplicados
    # Se eliminan los mails duplicados para un mismo NRO_DOC pero le quitamos el CUIL porque nos pasan varios
    df_export.loc[(df_export.duplicated(subset=['IDMOROSO','DATO','PROVEEDOR_DATO'],keep=False)) , 'CUIT_CUIL'] = None
    df_export = df_export.drop_duplicates()

    df_export["CUIT_CUIL"]= df_export["CUIT_CUIL"].fillna(0.0)
    df_export1 = df_export.copy()
    df_export1["CUIT_CUIL"] = [int(i) for i in df_export1["CUIT_CUIL"].tolist()]
    df_export1.loc[(df_export1["CUIT_CUIL"]==0), 'CUIT_CUIL'] = None

    # Tenemos duplicados en la tríada IDMOROSO-DATO-PROVEEDOR_DATO. Exportamos e inicializamos el pipeline de duplicados:

    # ## Insert a tabla DAM_CONTACTABILIDAD:
    #df_export.to_excel('D:\MOPC\Enriquecimiento de Datos\Data Cleaning\PyP\Exports\DAM_CONTACTABILIDAD\PyP_DAM_CONTACTABILIDAD_telefonos_refuerzo.xlsx',index=False)
    #df_export.to_csv('D:\MOPC\Enriquecimiento de Datos\Data Cleaning\PyP\Exports\DAM_CONTACTABILIDAD\PyP_DAM_CONTACTABILIDAD_telefonos_refuerzo.csv',index=False)

    ## Filtrado de datos
    # Tenemos que filtrar los datos basura que nos insertan
    lista_filtrado = ["11111111",
                      "111111111",
                      "1111111111"]
    df_export1 = df_export1[~df_export1.DATO.isin(lista_filtrado)]
    df_export2 = df_export1.drop_duplicates(subset=["DATO"],keep=False)
    duplicates_subset = df_export2[df_export2.duplicated(subset=['DATO','PROVEEDOR_DATO'],keep=False)]

    return df_export2

df1 = procesar("sumario_fis.csv","Datos_Enriquecimiento.csv",1) # 1 para persona física
df2 = procesar("sumario_jur.csv","Datos_Enriquecimiento.csv",2)
df_export = pd.concat([df1,df2])

# # Export a Pipeline: Duplicados
path = "insiders/phone_"+str(date.today())+".csv"
df_export.to_csv(path,index=False)
print("Resultado exitoso.")
print("Guardado en:",path)
