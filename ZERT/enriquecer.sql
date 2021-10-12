insert into  DAM_CONTACTABILIDAD
SELECT
      [IDMOROSO]
      ,[TIPO_DOC]
      ,[NRO_DOC]
      ,[CUIT_CUIL]
      ,[TIPO]
      ,[SUBTIPO]
      ,[DATO]
      ,[PROPIETARIO_DATO]
      ,[PROVEEDOR_DATO]
      ,getdate () as [FECHA_INGRESO_DATO]
      ,[VERIFICADO]
      ,convert ( datetime, ([FECHA_VERIFICACION]),102) as [FECHA_VERIFICACION]
      ,[OPERADOR_VERIFICACION]
      ,[SCORE_DATO]
 ,null as rechazado
 ,null as fecha_rechazo
 ,null as operador_rechazo
 ,null as razon_rechazo
 ,null as SCORE_DATO_100
  FROM at.dbo.DAM_CONTACTABILIDAD_TEMP with (nolock)
  where at.dbo.DAM_CONTACTABILIDAD_TEMP.idmoroso not  in
  (select  DAM_CONTACTABILIDAD.IDMOROSO from  DAM_CONTACTABILIDAD where DAM_CONTACTABILIDAD.IDMOROSO =at.dbo.DAM_CONTACTABILIDAD_TEMP.idmoroso and
  DAM_CONTACTABILIDAD.dato = at.dbo.DAM_CONTACTABILIDAD_TEMP.dato and DAM_CONTACTABILIDAD.[PROVEEDOR_DATO] =
  at.dbo.DAM_CONTACTABILIDAD_TEMP.[PROVEEDOR_DATO])
