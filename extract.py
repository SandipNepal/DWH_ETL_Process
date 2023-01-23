from snowflake.connector.pandas_tools import write_pandas
from pathlib import Path
from connection import cs
from connection import ctx

import pandas as pd
import csv
import os


def convertToCsv(filepath, sqlQueries):
    tableList = ["COUNTRY", "REGION", "STORE"]
    for table in tableList:
        cs.execute(sqlQueries[table])
        result = cs.fetch_pandas_all()
        result.to_csv(filepath + table + ".csv", index=False)


def loadIntoStagingTable(ctx, filepath):
    cs.execute("USE SCHEMA DW_STG")

    fileList = os.listdir(filepath)
    tableList = ["D_BHATBHATENI_CNTRY_T",
                 "D_BHATBHATENI_RGN_T", "D_BHATBHATENI_STORE_T"]
    i = 0
    for table in tableList:
        cs.execute("truncate table {}".format(table))
        df = pd.read_csv(filepath + fileList[i], sep=",")
        write_pandas(ctx, df, table_name=table)
        i += 1


def main():
    filepath = os.getcwd()+'/csv_files/'
    sqlQueries = {
        "COUNTRY": " SELECT * FROM COUNTRY;",
        "REGION": " SELECT * FROM REGION;",
        "STORE": " SELECT * FROM STORE;",
    }
    convertToCsv(filepath, sqlQueries)
    loadIntoStagingTable(ctx, filepath)


try:
    main()
finally:
    cs.close()
ctx.close()
