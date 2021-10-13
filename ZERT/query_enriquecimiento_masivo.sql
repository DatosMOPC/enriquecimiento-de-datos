SELECT c.IDMOROSO, m.NOMBRE, c.IDCLIENTE, c.ESTADO,
c.FILTROA, m.DATOADIC, c.FASIGNACION
FROM Active.dbo.CARTERA c
LEFT JOIN Active.dbo.MOROSO m
ON c.IDMOROSO = m.IDMOROSO
WHERE c.FASIGNACION >= CAST('2021-01-01' AS DATE)
AND c.ESTADO NOT IN ( 'ACUERDO CAIDO', 'ASIGNADO', 'COMPROMISO_PORTAL', 'CONVENIO', 'CONVENIO CAIDO', 'CONVENIO_QUITA', 'DESAPARECIDO', 'N GESTION',
	'EN GESTION_PORTAL', 'FELLECIDO', 'INTERESADO', 'INTERESADO_PORTAL', 'NO GESTIONAR', 'NO INTERESADO',
	'NO UBICADO', 'NO VISTO', 'PAGO PARCIAL', 'PLAN DE PAGO', 'PLAN DE PAGOS_PORTAL', 'POSIBLE REFI',
	' PROBLEMA', 'PROPUESTA', 'PROPUESTA OK', 'PROPUESTA REFI', 'PROPUESTA_OK', 'RAUL', 'REFI APROBADA', 'REFI ARMADA', 'REFI EN TRAMITE', 'SALDO INSOLUTO',
	'SKIP LEVEL', 'SKIP LEVEL 1', 'SKIP LEVEL 2', 'SKIP LEVEL1', 'SKIP LEVEL1 OK', 'SKIP LEVEL3 OK')
AND IDCLIENTE IN ('FRANCES_2012', 'BSR_TARDIA', 'AMEX_ALHR','AMEX_ALLR','AMEX_PALM','AMEX_PARM',
	'AMEX_PBLM', 'AMEX_PBRM','AMEX_PELM','AMEX_PFLM','AMEX_PNCM','AMEX_QRNM','AMEX_QRPM','AMEX_TNCM',
	'AMEX_TNLM','AMEX_TNRM', 'HSBC_LEGALES14', 'HSBC_LEGALES_TEMP', 'BBVA_LEGALES_PREJU',
	'BBVA_LEGALES_PREJU_5', 'HSBC_FOGAR','PRENDARIOS_LEGALES','BBVA_LEGALES_PREJU_EMP')


