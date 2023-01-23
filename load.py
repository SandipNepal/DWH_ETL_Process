from connection import cs
from connection import ctx
from datetime import datetime

current_time = datetime.now()


def insertInCountry():
    update = f"""
                UPDATE DW_TGT.D_BHATBHATENI_CNTRY_T tgt
                SET
                tgt.CNTRY_ID = tmp.CNTRY_ID,
                tgt.CNTRY_DESC = tmp.CNTRY_DESC,
                tgt.ROW_UPDT_TMS = '{current_time}'
                FROM DW_TMP.D_BHATBHATENI_CNTRY_T tmp
                WHERE tgt.CNTRY_KY = tmp.CNTRY_KY
                """
    cs.execute(update)

    insert = f"""
                INSERT INTO DW_TGT.D_BHATBHATENI_CNTRY_T(CNTRY_KY, CNTRY_ID, CNTRY_DESC, OPEN_CLOSE_CD, ROW_INSRT_TMS, ROW_UPDT_TMS)
                SELECT tmp.CNTRY_KY, tmp.CNTRY_ID, tmp.CNTRY_DESC, 'O', '{current_time}', '{current_time}'
                FROM DW_TMP.D_BHATBHATENI_CNTRY_T tmp
                WHERE tmp.CNTRY_KY NOT IN (SELECT CNTRY_KY FROM DW_TGT.D_BHATBHATENI_CNTRY_T);
                """
    cs.execute(insert)


def insertInRegion():
    update = f"""
                UPDATE DW_TGT.D_BHATBHATENI_RGN_T tgt
                SET
                tgt.RGN_ID = tmp.RGN_ID,
                tgt.RGN_DESC = tmp.RGN_DESC,
                tgt.ROW_UPDT_TMS = '{current_time}'
                FROM DW_TMP.D_BHATBHATENI_RGN_T tmp
                WHERE tgt.RGN_KY =tmp.RGN_KY
                """
    update = f"""
                UPDATE DW_TGT.D_BHATBHATENI_CNTRY_T tgt
                SET
                tgt.CNTRY_ID = tmp.CNTRY_ID,
                tgt.CNTRY_DESC = tmp.CNTRY_DESC,
                tgt.ROW_UPDT_TMS = '{current_time}'
                FROM DW_TMP.D_BHATBHATENI_CNTRY_T tmp
                WHERE tgt.CNTRY_KY = tmp.CNTRY_KY
                """
    cs.execute(update)

    cs.execute(update)

    insert = f"""
                INSERT INTO DW_TGT.D_BHATBHATENI_RGN_T(RGN_ID, RGN_KY, CNTRY_KY, RGN_DESC, OPEN_CLOSE_CD, ROW_INSRT_TMS, ROW_UPDT_TMS)
                SELECT tmp.RGN_ID, tmp.RGN_KY, tmp.CNTRY_KY, tmp.RGN_DESC,'O', '{current_time}', '{current_time}'
                FROM DW_TMP.D_BHATBHATENI_RGN_T tmp
                WHERE tmp.RGN_KY NOT IN (SELECT RGN_KY FROM DW_TGT.D_BHATBHATENI_RGN_T)
                """

    cs.execute(insert)

    insert = f"""
                INSERT INTO DW_TGT.D_BHATBHATENI_CNTRY_T(CNTRY_KY, CNTRY_ID, CNTRY_DESC, OPEN_CLOSE_CD, ROW_INSRT_TMS, ROW_UPDT_TMS)
                SELECT tmp.CNTRY_KY, tmp.CNTRY_ID, tmp.CNTRY_DESC, 'O', '{current_time}', '{current_time}'
                FROM DW_TMP.D_BHATBHATENI_CNTRY_T tmp
                WHERE tmp.CNTRY_KY NOT IN (SELECT CNTRY_KY FROM DW_TGT.D_BHATBHATENI_CNTRY_T);
                """

    cs.execute(insert)


def insertInStore():
    update = f"""
                UPDATE DW_TGT.D_BHATBHATENI_STORE_T tgt
                SET
                tgt.STORE_ID = tmp.STORE_ID,
                tgt.STORE_DESC = tmp.STORE_DESC,
                tgt. ROW_UPDT_TMS = '{current_time}'
                FROM DW_TMP.D_BHATBHATENI_STORE_T tmp
                WHERE tgt.STORE_KY = tmp.STORE_KY;
                """

    cs.execute(update)

    insert = f"""
                INSERT INTO DW_TGT.D_BHATBHATENI_STORE_T(STORE_KY,STORE_ID,RGN_KY,STORE_DESC,LAST_OPEN_TMS,ACTV_FLG,OPEN_CLOSE_CD,ROW_INSRT_TMS,ROW_UPDT_TMS)
                SELECT STORE_KY, STORE_ID, RGN_KY, STORE_DESC, '{current_time}','Y','O', '{current_time}', '{current_time}'
                FROM DW_TMP.D_BHATBHATENI_STORE_T
                WHERE STORE_KY NOT IN (SELECT STORE_KY FROM DW_TGT.D_BHATBHATENI_STORE_T)
                """

    cs.execute(insert)

    not_active_N = f"""
        UPDATE DW_TGT.D_BHATBHATENI_STORE_T
        SET
        ACTV_FLG = 'N',
        LAST_CLOSED_TMS = '{current_time}',
        ROW_UPDT_TMS = '{current_time}'
        WHERE STORE_KY NOT IN 
        (SELECT STORE_KY FROM DW_TMP.D_BHATBHATENI_STORE_T)
        AND ACTV_FLG = 'Y' ;
                """
    cs.execute(not_active_N)

    active_Y = f"""
        UPDATE DW_TGT.D_BHATBHATENI_STORE_T
        SET
        ACTV_FLG = 'Y',
        LAST_CLOSED_TMS = NULL,
        ROW_UPDT_TMS = '{current_time}'
        WHERE ACTV_FLG = 'N' and STORE_KY IN
        (SELECT STORE_KY FROM DW_TMP.D_BHATBHATENI_STORE_T);
        """
    cs.execute(active_Y)


def loadToTarget(tableNames):
    if "D_BHATBHATENI_CNTRY_T" in tableNames:
        insertInCountry()
    if "D_BHATBHATENI_RGN_T" in tableNames:
        insertInRegion()
    if "D_BHATBHATENI_STORE_T" in tableNames:
        insertInStore()


def main():
    tableNames = ["D_BHATBHATENI_CNTRY_T",
                  "D_BHATBHATENI_RGN_T", "D_BHATBHATENI_STORE_T"]
    loadToTarget(tableNames)


try:
    main()
finally:
    cs.close()
ctx.close()
