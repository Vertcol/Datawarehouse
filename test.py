from tableutils import * 
import pandas as pd
import numpy as np


def mergeTest():
    df1 = pd.DataFrame({
        'CODE': ['A', 'B', 'C'],
        'Column1': [1, 2, 3],
        'Column2': ['X', 'Y', 'Z']
    })

    df2 = pd.DataFrame({
        'CODE': ['B', 'C', 'D'],
        'Column2': ['Y', 'Z', 'W'], 
        'Column3': [True, False, True]
    })

    df_expected = pd.DataFrame({
        'CODE': ['A', 'B', 'C', 'D'],
        'Column1': [1, 2, 3, np.nan],
        'Column2': ['X', 'Y', 'Z', 'W'],
        'Column3': [np.nan, True, False, True]
    }).set_index('CODE')

    df_result = mergeTables(df1,df2,'CODE')

    if not df_expected.equals(df_result):
        raise("Merge Failed")
    
    print("✅ Merge Test Sucess")

def mergeConflictTest():
    df1 = pd.DataFrame({
        'CODE': ['A', 'B', 'C'],
        'Column1': [1, 2, 3],
        'Column2': ['J', 'K', 'L']
    })

    df2 = pd.DataFrame({
        'CODE': ['B', 'C', 'D'],
        'Column2': ['Y', 'Z', 'W'],  # Note: 'Y' and 'Z' don't match df1's 'Column2'
        'Column3': [True, False, True]
    })

    df_expected = pd.DataFrame({
        'CODE': ['A', 'B', 'C', 'D'],
        'Column1': [1, 2, 3, np.nan],
        'Column2': ['X', 'Y', 'Z', 'W'],
        'Column3': [np.nan, True, False, True]
    }).set_index('CODE')
    
    try:
        mergeTables(df1,df2,'CODE')
    except ValueError:
        print("✅ Merge Conflict Test Sucess")
        return

    raise Exception("Merge Conflict Test Failed")
    
def sizeCheckTest():
    df1 = pd.DataFrame({
        'CODE': ['A', 'B', 'C'],
        'Column1': [1, 2, 3],
        'Column2': ['J', 'K', 'L']
    })

    sizeCheck(df1,3)
    try:
        sizeCheck(df1,5)
    except ValueError:
        print("✅ Size Check Test Sucess")
        return

    raise Exception("Size Check Test Failed")

def surrogateTest(cursor):

    updateSurrogate('Sales_Staff', 'Sales_Staff', 'MANAGER_id', 'SALES_STAFF_id', cursor)

    print("✅ Surogate Update Test Sucess")


def runTests(settings):

    sql_server_conn = pyodbc.connect(f"DRIVER={{SQL Server}};SERVER={settings.server};DATABASE={settings.database};Trusted_Connection=yes")
    cursor = sql_server_conn.cursor()

    mergeTest()
    mergeConflictTest()
    sizeCheckTest()
    surrogateTest(cursor)

if __name__ == '__main__':
    settings = Settings(
        server="DESKTOP-9F8A8PF\\MSSQLSERVER01",
        database="Datawarehouse",
        data_dir="data/",
        log_dir="logs/"
    )

    runTests(settings)
    