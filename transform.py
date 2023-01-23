from connection import cs
from connection import ctx


def insertToTemp(tableNames):
    stgTableWithColumns = []
    tmpTableWithColumns = []
    tableWithForeignKey = []
    i = 0
    for table in tableNames:
        cs.execute(
            "SHOW COLUMNS IN DW_STG.{}".format(table)
        )
        allColumnsInStgTable = cs.fetchall()
        allColumnsInStgTable = list(
            map(lambda x: (x[2]), allColumnsInStgTable))
        allColumnsInStgTable.insert(0, table)
        stgTableWithColumns.append(allColumnsInStgTable)

    for table in tableNames:
        cs.execute("SHOW COLUMNS IN DW_TMP.{}".format(table))
        allColumnsInTmpTable = cs.fetchall()
        allColumnsInTmpTable = list(
            map(lambda x: (x[2]), allColumnsInTmpTable))
        allColumnsInTmpTable.insert(0, table)
        tmpTableWithColumns.append(allColumnsInTmpTable)

    for table in tableNames:
        cs.execute("""SELECT DISTINCT
                   TABLE_NAME
                   FROM
                   INFORMATION_SCHEMA.TABLE_CONSTRAINTS
                   WHERE
                   CONSTRAINT_TYPE='FOREIGN KEY' AND TABLE_NAME='{}'"""
                   .format(table))
        result = cs.fetchall()
        tableWithForeignKey.append(result)

    for table in tableNames:
        referencedTables = ['D_BHATBHATENI_CNTRY_T', 'D_BHATBHATENI_RGN_T']
        referencedColumns = ['CNTRY_KY', 'RGN_KY']
        referencedIds = ['CNTRY_ID', 'RGN_ID']
        cs.execute("TRUNCATE TABLE DW_TMP.{}".format(table))
        if any(table == sublist[0][0] for sublist in tableWithForeignKey if sublist):
            temp = [
                sublist for sublist in tmpTableWithColumns if sublist[0] == table]
            stg = [
                sublist for sublist in stgTableWithColumns if sublist[0] == table]
            query = """INSERT INTO DW_TMP.{}({},{},{}) SELECT DW_STG.{}.{}, DW_TMP.{}.{}, DW_STG.{}.{} FROM DW_STG.{} JOIN DW_TMP.{} ON DW_STG.{}.{} = DW_TMP.{}.{}""".format(
                table, temp[0][2], temp[0][3], temp[0][4], table, stg[0][1], referencedTables[
                    i], referencedColumns[i], table, stg[0][3], table, referencedTables[i], table, stg[0][2], referencedTables[i],
                referencedIds[i])
            cs.execute(query)
            i += 1

        else:
            temp = [
                sublist for sublist in tmpTableWithColumns if sublist[0] == table]
            stg = [
                sublist for sublist in stgTableWithColumns if sublist[0] == table]
            query = """INSERT INTO DW_TMP.{}({},{}) SELECT DW_STG.{}.{}, DW_STG.{}.{} FROM DW_STG.{}""".format(
                table, temp[0][2], temp[0][3], table, stg[0][1], table, stg[0][2], table)
            cs.execute(query)


def main():
    tableNames = ["D_BHATBHATENI_CNTRY_T",
                  "D_BHATBHATENI_RGN_T", "D_BHATBHATENI_STORE_T"]
    insertToTemp(tableNames)


try:
    main()
finally:
    cs.close()
ctx.close()
