import pandas as pd
import numpy as np
import sys
from datetime import datetime, date

sys.path.append("/home/tetotille/Escritorio/MOPC/enriquecimiento")


def iniciar_procesado(fecha_envio_log,fecha_insercion_log,fecha_estados_log):
    # #LOG
    #1. Datos enviados
    df_log = pd.read_csv("Datos_Enriquecimiento.csv")

    df_log = df_log.rename(columns={"ESTADO": "ESTADO_ANTERIOR"})
    df_log = df_log [['IDMOROSO','DATOADIC','IDCLIENTE','ESTADO_ANTERIOR']]

    df_log = df_log.assign(FECHA_ENVIO_ENRIQUECIMIENTO=fecha_envio_log)
    df_log = df_log.assign(FECHA_INSERCION_DATO=fecha_insercion_log)
    df_log = df_log.assign(FECHA_CAMBIO_ESTADO=fecha_estados_log)
    df_log = df_log.assign(PROVEEDOR='ZERT')
    df_log['ESTADO_ANTERIOR'] = [(str(df_log['ESTADO_ANTERIOR'][i]).strip()) for i in range (df_log['ESTADO_ANTERIOR'].count())]

    df_log.loc[(df_log['ESTADO_ANTERIOR'] == 'INTERESADO') |
            (df_log['ESTADO_ANTERIOR'] == 'NO VISTO') |
            (df_log['ESTADO_ANTERIOR'] == 'EN GESTION') |
            (df_log['ESTADO_ANTERIOR'] == 'ASIGNADO') |
            (df_log['ESTADO_ANTERIOR'] == 'ROTACION') |
            (df_log['ESTADO_ANTERIOR'] == 'ASIGNADO') |
            (df_log['ESTADO_ANTERIOR'] == 'PLAN V') |
            (df_log['ESTADO_ANTERIOR'] == 'NO INTERESADO') |
            (df_log['ESTADO_ANTERIOR'] == 'ACUERDO CAIDO') |
            (df_log['ESTADO_ANTERIOR'] == 'CANCELO_RECHAZADA')
            , 'CATEGORIA'] = 'NEUTRO'

    df_log.loc[(df_log['ESTADO_ANTERIOR'] == 'NO UBICADO') |
            (df_log['ESTADO_ANTERIOR'] == 'SKIP LEVEL') |
            (df_log['ESTADO_ANTERIOR'] == 'SKIP LEVEL 1') |
            (df_log['ESTADO_ANTERIOR'] == 'SKIP LEVEL 1 OK') |
            (df_log['ESTADO_ANTERIOR'] == 'SKIP LEVEL 2') |
            (df_log['ESTADO_ANTERIOR'] == 'SKIP LEVEL 3') |
            (df_log['ESTADO_ANTERIOR'] == 'SKIP LEVEL 3 OK') |
            (df_log['ESTADO_ANTERIOR'] == 'SKIP LEVEL3 OK') |
            (df_log['ESTADO_ANTERIOR'] == 'SKIP LEVEL1 OK') |
            (df_log['ESTADO_ANTERIOR'] == 'SKIP LEVEL1') |
            (df_log['ESTADO_ANTERIOR'] == 'NO_VISTO_CONTACTABLE') |
            (df_log['ESTADO_ANTERIOR'] == 'NO_VISTO_MASIVO')
            , 'CATEGORIA'] = 'NEGATIVO POR CONTACTABILIDAD'

    df_log.loc[(df_log['ESTADO_ANTERIOR'] == 'PAGO PARCIAL') |
            (df_log['ESTADO_ANTERIOR'] == 'PLAN DE PAGOS') |
            (df_log['ESTADO_ANTERIOR'] == 'POSIBLE REFI') |
            (df_log['ESTADO_ANTERIOR'] == 'REFINANCIACION') |
            (df_log['ESTADO_ANTERIOR'] == 'PROPUESTA') |
            (df_log['ESTADO_ANTERIOR'] == 'PROPUESTA OK') |
            (df_log['ESTADO_ANTERIOR'] == 'COMPROMISO') |
            (df_log['ESTADO_ANTERIOR'] == 'CANCELO') |
            (df_log['ESTADO_ANTERIOR'] == 'CANCELO_DEVUELTO') |
            (df_log['ESTADO_ANTERIOR'] == 'CANCELO DEVUELTO') |
            (df_log['ESTADO_ANTERIOR'] == 'CANCELO FULL') |
            (df_log['ESTADO_ANTERIOR'] == 'CANCELO SIN ACP')
            , 'CATEGORIA'] = 'POSITIVO CON CAJA'

    df_log.loc[(df_log['ESTADO_ANTERIOR'] == 'CONVENIO') |
            (df_log['ESTADO_ANTERIOR'] == 'CONVENIO QUITA') |
            (df_log['ESTADO_ANTERIOR'] == 'NUEVOS CARGOS') |
            (df_log['ESTADO_ANTERIOR'] == 'CONVENIO_QUITA')
            , 'CATEGORIA'] = 'POSITIVO CON CAJA SOBRE COUTA'

    df_log.loc[(df_log['ESTADO_ANTERIOR'] == 'PROPUESTA REFI') |
            (df_log['ESTADO_ANTERIOR'] == 'REFI ARMADA') |
            (df_log['ESTADO_ANTERIOR'] == 'REFI EN TRAMITE') |
            (df_log['ESTADO_ANTERIOR'] == 'REFI A LIQUIDAR')
            , 'CATEGORIA'] = 'POSITIVO CON CAJA CON REFI PENDIENTE DE LIQUIDACIO'

    df_log.loc[(df_log['ESTADO_ANTERIOR'] == 'REFI APROBADA') |
            (df_log['ESTADO_ANTERIOR'] == 'DEVUELTO/CANCELO CON COBRO DE HONORARIO PENDIENTE')
            , 'CATEGORIA'] = 'POSITIVO CON CAJA CON REFI/CANCELACIÓN LIQUIDADA'

    df_log.loc[(df_log['ESTADO_ANTERIOR'] == 'DEVUELTO') |
            (df_log['ESTADO_ANTERIOR'] == 'FALLECIDO') |
            (df_log['ESTADO_ANTERIOR'] == 'PROBLEMA') |
            (df_log['ESTADO_ANTERIOR'] == 'SALDO INSOLUTO') |
            (df_log['ESTADO_ANTERIOR'] == 'NO GESTIONAR')
            , 'CATEGORIA'] = 'INACTIVO'

    # 2. Devolución de PyP
    # Levantamos los DNIs desde el archivo que devuelve PyP para cruzar contra los IDs y ver cuáles están enriquecidos.

    #Esta fecha para el formato en el path
    fecha_inser = fecha_insercion_log[0:5]+fecha_insercion_log[8:10]+fecha_insercion_log[4:7]
    df_pyp1 = pd.read_csv('insiders/mail_fis_'+fecha_insercion_log+'.csv')
    df_pyp2 = df_pyp = pd.read_csv('insiders/phone_fis_'+fecha_insercion_log+'.csv')

    df_reduced1 = df_pyp1[['CUIT_CUIL','NRO_DOC']]
    df_reduced2 = df_pyp2[['CUIT_CUIL','NRO_DOC']]

    df_reduced = df_reduced1.merge(df_reduced2, on="NRO_DOC", how="outer")
    df_reduced = df_reduced.rename(columns = {'CUIT_CUIL_x':'CUIT_CUIL'})
    df_reduced = df_reduced.drop(columns = ['CUIT_CUIL_y'])

    df_reduced = df_reduced.drop_duplicates(subset=["NRO_DOC"])

    # # NRO_DOC para union de dataframes
    # Creamos una columna que contenga IDMOROSO + DATOADIC según corresponda
    df_log.loc[(df_log['IDCLIENTE'].str.startswith('AMEX')) , 'NRO_DOC'] = df_log['DATOADIC']
    df_log.loc[(~df_log['IDCLIENTE'].str.startswith('AMEX')) , 'NRO_DOC'] = df_log['IDMOROSO']
    df_log = df_log.dropna(subset=['NRO_DOC'])

    """
    12.2) DNI Cleaning
    Limpiamos el dataframe df_pyp, ya que dentro de DATO_A_ENVIAR tenemos 9 tipos de casos diferentes de datos:

    DNI 12345678
    DNI12345678
    DNIE1234678
    LC 1234567
    LE 1234567
    CUIT00123456780
    00123456780 --> Es un CUIT.
    00 12345678 0 --> Es un CUIT.
    DNI 00123456780 --> Dice DNI pero es un CUIT de una persona jurídica (TO DO)
    Para eso definimos 5 funciones:

    Tipo_DOC: Se definen dos funciones para determinar el tipo de DNI. Una inicial, y otra para corregir sobre el dataframe resultante.
    Standard DNI: Estandariza los DNI para 7 de los casos. Esta función debe ejecutarse en primer lugar.
    DNI Spaces: Elimina los espacios.
    DNI Zero: Elimina los ceros iniciales de ciertos CUITs para que coincidan con los DNI.
    Cuit: Decisión sobre dejar un cuit o volverlo DNI
    """

    def tipo_doc_1 (string_dni):

        if 'DNI' in string_dni:
            tipo = 'DNI'

        elif 'LC' in string_dni:
            tipo = 'LC'

        elif 'LE' in string_dni:
            tipo = 'LE'

        elif 'PAS' in string_dni:
            tipo = 'PAS'

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

        elif 'PAS' in string_dni:
            dni = string_dni.split('PAS')[1]

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
    df_log['NRO_DOC'] = df_log['NRO_DOC'].str.strip()

    #Se implementan estas funciones:

    #Para determinar el TIPO DE DOCUMENTO en primer lugar:

    df_log['TIPO_DOC'] = df_log['NRO_DOC'].map(tipo_doc_1)

    #Para la limpieza de DNIs:
    df_log['NRO_DOC'] = df_log['NRO_DOC'].map(standard_dni)
    df_log['NRO_DOC'] = df_log['NRO_DOC'].map(dni_characters)
    df_log['NRO_DOC'] = df_log['NRO_DOC'].map(cuit)
    df_log['NRO_DOC'] = df_log['NRO_DOC'].map(dni_zero)

    #Para corregir el TIPO DE DOCUMENTO sobre el dataframe resultante
    df_log['TIPO_DOC'] = df_log.apply(lambda x: tipo_doc_2(x.NRO_DOC, x.TIPO_DOC), axis=1)

    #13) Union de Dataframes

    #Se unen ambos dataframes en la columna NRO_DOC

    #Convertimos la columna NRO_DOC de objeto a INT para hacer la unión
    df_reduced = df_reduced.astype({'NRO_DOC': 'str'})
    df_reduced ['NRO_DOC'] = df_reduced ['NRO_DOC'].str.split('.').str[0]
    df_reduced['ENRIQUECIDO']=1
    df_union = pd.merge(df_reduced, df_log,  on='NRO_DOC', how='right')
    df_union['ENRIQUECIDO']=df_union["ENRIQUECIDO"].fillna(0)
    df_clean = df_union.drop_duplicates()

    duplicateRowsDF = df_union[df_union.duplicated(subset=['IDMOROSO'], keep=False)]

    ## Nuevo Estado
    #Como no se usaron nuevos estados, se deja en nulo
    df_clean = df_clean.assign(NUEVO_ESTADO=np.nan)


    ##Enriquecido
    #Nota: sólo estoy mirando los enriquecidos. Si no se enriqueció, no lo mando al LOG.
    df_clean = df_clean [['IDMOROSO','IDCLIENTE','ESTADO_ANTERIOR','CATEGORIA','NUEVO_ESTADO','ENRIQUECIDO','FECHA_ENVIO_ENRIQUECIMIENTO','FECHA_INSERCION_DATO','FECHA_CAMBIO_ESTADO','PROVEEDOR']]
    df_clean['ENRIQUECIDO']=df_clean['ENRIQUECIDO'].astype(int)
    df_clean['ENRIQUECIDO'].value_counts()

    df_clean.to_csv("DAM-LOG/dam_log_"+fecha_estados_log+".csv")
    print("El programa fue exitoso.")
    print("Se guardó en "+"dam_log_"+fecha_estados_log+".csv")


fecha_envio_log = input('Ingrese la fecha que se mandó a enriquecer. Ej:2021-08-26\n') # Fecha en la que se envió a enriquecer los datos
fecha_insercion_log = input('Ingrese la fecha en que se subió a la BD. Ej:2021-08-27\n') # Fecha en la que se insertaron los datos en la base de datos
fecha_estados_log = str(date.today()) # Fecha actual
iniciar_procesado(fecha_envio_log,fecha_insercion_log,fecha_estados_log)
