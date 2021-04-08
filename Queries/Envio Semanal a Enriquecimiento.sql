
-- NO VISTOS

SELECT CARTERA.IDMOROSO, MOROSO.NOMBRE, CARTERA.IDCLIENTE, CARTERA.ESTADO, CARTERA.FILTROA, MOROSO.DATOADIC
FROM CARTERA
LEFT JOIN MOROSO
ON CARTERA.IDMOROSO = MOROSO.IDMOROSO
WHERE ESTADO = 'NO VISTO'
AND IDCLIENTE IN ('FRANCES_2012', 'BSR_TARDIA')
AND CARTERA.IDMOROSO NOT IN (SELECT IDMOROSO FROM ACTIVE_TESTING.dbo.DAM_LOG)
UNION ALL
------------AMEX
SELECT CARTERA.IDMOROSO, MOROSO.NOMBRE, CARTERA.IDCLIENTE, CARTERA.ESTADO, CARTERA.FILTROA,MOROSO.DATOADIC
FROM CARTERA
LEFT JOIN MOROSO
ON CARTERA.IDMOROSO = MOROSO.IDMOROSO
WHERE ESTADO IN ('NO VISTO','NO UBICADO','SKIP LEVEL1','SKIP LEVEL1 OK')
AND IDCLIENTE IN ('AMEX_ALHR','AMEX_ALLR','AMEX_PALM','AMEX_PARM','AMEX_PBLM',
'AMEX_PBRM','AMEX_PELM','AMEX_PFLM','AMEX_PNCM','AMEX_QRNM','AMEX_QRPM','AMEX_TNCM','AMEX_TNLM','AMEX_TNRM' )
AND CARTERA.IDMOROSO NOT IN (SELECT IDMOROSO FROM ACTIVE_TESTING.dbo.DAM_LOG)


-------------AFM
SELECT CARTERA.IDMOROSO, MOROSO.NOMBRE, CARTERA.IDCLIENTE, CARTERA.ESTADO, CARTERA.FILTROA, MOROSO.DATOADIC
FROM CARTERA
LEFT JOIN MOROSO
ON CARTERA.IDMOROSO = MOROSO.IDMOROSO
WHERE ESTADO IN ('NO VISTO', 'EN GESTION','NO UBICADO','SKIP LEVEL1')
AND IDCLIENTE IN ('FRAVEGA_LEGALES548')
AND CARTERA.IDMOROSO NOT IN (SELECT IDMOROSO FROM ACTIVE_TESTING.dbo.DAM_LOG)

