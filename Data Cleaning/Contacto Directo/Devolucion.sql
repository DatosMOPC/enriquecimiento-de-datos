SELECT DOCUMENTO, IDMOROSO, TIPO, DATO, SCORE_DATO
FROM AT.dbo.CD_ENRIQUECIMIENTO
INNER JOIN ACTIVE.dbo.DAM_CONTACTABILIDAD_FILTRADA
ON ACTIVE.dbo.DAM_CONTACTABILIDAD_FILTRADA.NRO_DOC = AT.dbo.CD_ENRIQUECIMIENTO.DOCUMENTO
AND VERIFICADO = 1
ORDER BY DOCUMENTO ASC

SELECT * 
FROM ACTIVE.dbo.DAM_CONTACTABILIDAD
LEFT JOIN AT.dbo.CD_ENRIQUECIMIENTO 
ON ACTIVE.dbo.DAM_CONTACTABILIDAD.NRO_DOC = AT.dbo.CD_ENRIQUECIMIENTO.DOCUMENTO
AND VERIFICADO = 1

SELECT * FROM ACTIVE.dbo.DAM_CONTACTABILIDAD WHERE IDMOROSO = 'DNI 10010919            '
SELECT * FROM CARTERA WHERE IDMOROSO = 'DNI 10155103      '
SELECT * FROM AT.dbo.CD_ENRIQUECIMIENTO WHERE DOCUMENTO = '10155103            '