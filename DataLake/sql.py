query_bopos = '''SELECT
	LO_BSN_UN.RFID_BSN_UN   AS SUCURSAL,
	LO_BSN_UN.NM_BSN_UN     AS DESCRIPCION_SUCURSAL,
	OH_ITM_BU_ST.ID_ITM     AS ARTICULO,
	AS_ITM.DE_ITM           AS DESCRIPCION_ARTICULO,
	CO_UOM.LU_UOM           AS CODIGO_UDM,
	OH_ITM_BU_ST.ID_SBTP    AS ESTADO,
	OH_ITM_BU_ST.QU_CUR     AS SALDO
FROM
	INVENTARIO.AS_ITM_STK@SRC_BOPOS_PRD,
	INVENTARIO.AS_ITM@SRC_BOPOS_PRD,
	INVENTARIO.ITM_UN_MEA@SRC_BOPOS_PRD,
	INVENTARIO.CO_UOM@SRC_BOPOS_PRD,
	INVENTARIO.LO_LCN_INV@SRC_BOPOS_PRD,
	INVENTARIO.LO_LCN@SRC_BOPOS_PRD,
	INVENTARIO.LO_STE@SRC_BOPOS_PRD,
	INVENTARIO.CO_BSN_UN_STE@SRC_BOPOS_PRD,
	INVENTARIO.LO_BSN_UN@SRC_BOPOS_PRD,
	INVENTARIO.OH_ITM_BU_ST@SRC_BOPOS_PRD
WHERE
	AS_ITM.ID_ITM = AS_ITM_STK.ID_ITM
	AND OH_ITM_BU_ST.ID_ITM = AS_ITM_STK.ID_ITM
	AND ITM_UN_MEA.ID_ITM = AS_ITM.ID_ITM
	AND ITM_UN_MEA.IS_MIN_UNI = 'Y'
	AND CO_UOM.LU_UOM = ITM_UN_MEA.UN_MEA_ID
	AND LO_LCN.ID_LCN = LO_LCN_INV.ID_LCN
	AND LO_LCN.ID_STE = LO_STE.ID_STE
	AND CO_BSN_UN_STE.ID_STE = LO_STE.ID_STE
	AND LO_BSN_UN.ID_BSN_UN = CO_BSN_UN_STE.ID_BSN_UN
	--AND OH_ITM_BU_ST.QU_CUR > 0  --OPCIONAL
	AND LO_BSN_UN.ACTIVE = 'Y'
	AND LO_BSN_UN.TY_BSN_UN IN 'TR'
	AND OH_ITM_BU_ST.ID_BSN_UN = LO_BSN_UN.ID_BSN_UN
	AND OH_ITM_BU_ST.ID_BSN_UN = CO_BSN_UN_STE.ID_BSN_UN
GROUP BY
	LO_BSN_UN.RFID_BSN_UN,
	LO_BSN_UN.NM_BSN_UN,
	OH_ITM_BU_ST.ID_ITM,
	AS_ITM.DE_ITM,
	CO_UOM.LU_UOM,
	OH_ITM_BU_ST.ID_SBTP,
	OH_ITM_BU_ST.QU_CUR
'''

