import pandas as pd
import json
import sqlite3
import pyodbc

"""
Method to merge two tables flexibly
- NaN values of one dataframe can be filled by the other dataframe
- Uses all available columns
- Errors when a row of the two dataframes doesn't match (df1 has 'A' and df2 has 'B' in row)
"""
def merge_tables(df1, df2, index_col):
    # Ensure 'CODE' is set as the index for both DataFrames
    if index_col not in df1.columns or index_col not in df2.columns:
        raise KeyError(f"{index_col} must be a column in both DataFrames.")
    
    df1 = df1.set_index(index_col)
    df2 = df2.set_index(index_col)

    # Identify common and exclusive columns
    common_columns = df1.columns.intersection(df2.columns)
    exclusive_df1 = df1.columns.difference(df2.columns)
    exclusive_df2 = df2.columns.difference(df1.columns)

    # Concatenate exclusive columns from each DataFrame onto the other
    df1_combined = pd.concat([df1, df2[exclusive_df2]], axis=1, sort=False)
    df2_combined = pd.concat([df2, df1[exclusive_df1]], axis=1, sort=False)

    # Resolve common columns with nulls and conflicts
    for col in common_columns:
        # Align the Series from both DataFrames for comparison
        series1, series2 = df1_combined[col].align(df2_combined[col])

        # Check for conflicts (non-null values that do not match)
        conflict_mask = (~series1.isnull() & ~series2.isnull() & (series1 != series2))
        if conflict_mask.any():
            raise ValueError(f"Merge failed due to conflict in column '{col}'")

        # Use values from df2 where df1 is null (prioritizing df1 values)
        df1_combined[col] = series1.combine_first(series2)

    return df1_combined

# Filters out all columns of dataframe that aren't typed
def filterColumns(dataframe):
    rename_mapping = {}

    with open('renames.json') as f_in:
        rename_mapping = json.load(f_in)

    # List of all vetted columns
    valid_columns = set(rename_mapping.values())

    valid_columns_set = set(valid_columns)
    actual_columns_set = set(dataframe.columns)
    intersection_columns = list(actual_columns_set.intersection(valid_columns_set))

    # Use the intersection result to filter columns from dataframe
    return dataframe[intersection_columns]

# Filters out all columns of dataframe that aren't typed
def excludeColumns(dataframe, column_names):
    return dataframe[dataframe.columns.difference(column_names)]

def sizeCheck(dataframe, expected_column_count):
    actual_column_count = len(dataframe.columns)
    if actual_column_count == expected_column_count:
        print(f'Table has {expected_column_count} columns')
    else:
        raise Exception(f'Table has {actual_column_count} columns, expected {expected_column_count}')

"""
Get the last slice of a string
"""
def getTypes():
    rename_mapping = {}
    with open('renames.json') as f_in:
        rename_mapping = json.load(f_in)

    types = {}
    for column in rename_mapping.values():
        column_type = column.rsplit('_', 1)[1]
        types[column_type] = ''
    return types

"""
Uses the column name to derive a SQL Server compatible type
- The type is derived from the column name (COLUMN_NAME_type)
- Column names without a type are invalid
"""
def columnType(column_name):
    column_types = {
        'name': 'NVARCHAR(80)',
        'image': 'NVARCHAR(60)',
        'id': 'INT',
        'description': 'NTEXT',
        'money': 'DECIMAL(19,4)',
        'percentage': 'DECIMAL(12,12)',
        'date': 'NVARCHAR(30)',
        'code': 'NVARCHAR(40)',
        'char': 'CHAR(1)',
        'number': 'INT',
        'phone': 'NVARCHAR(30)',
        'address': 'NVARCHAR(80)',
        'bool': 'BIT',
    }

    err = ''
    try:
        return column_types[column_name.rsplit('_', 1)[1]]
    except IndexError:
        err = "Column name doesn't contain a type"
    except KeyError:
        err = "Column type not found"
    raise Exception(err)

"""
Method to insert dataframe data into SQL server
"""
def createTable(tablename, dataframe, PK, cursor):
    SK = ''
    if PK == None:
        PK = dataframe.columns[0]
        SK = f'SK_{tablename}'
        columns = f'{PK} {columnType(PK)}'
    else:
        SK = f'SK_{PK}'
        columns = f'{PK} {columnType(PK)} NOT NULL'
    # Add Primary Key as third column
    
    # Add all the other columns
    for column in dataframe.columns:
        if column != PK: # PK is already added
            columns += f', {column} {columnType(column)}'

    surogate_columns = f"{SK} INT IDENTITY(1,1) NOT NULL PRIMARY KEY, Timestamp DATETIME NOT NULL DEFAULT(GETDATE())"

    # Create the command
    command = f"CREATE TABLE {tablename} ({surogate_columns}, {columns})"

    try:
        cursor.execute(command)
        cursor.commit()
    except pyodbc.Error as e:
        if 'There is already an object named' in str(e):
            print('Table already exists in database')
        else:
            raise(e)

"""
Method to insert dataframe data into SQL server
"""
def insertTable(tablename, dataframe, PK, cursor):
    # Add Primary Key as first column
    columns = ''
    if PK == None:
        PK = dataframe.columns[0]
        
    columns = PK
        
    # Add all the other columns
    for column in dataframe.columns:
        if column != PK: # PK is already added
            columns += f', {column}'
    
    # Execute inserts
    for i, row in dataframe.iterrows():
        values = ''
        values += str(row[PK])

        for column in dataframe.columns:
            if column != PK: # PK is already added
                try:
                    val = str(row[column]).replace("'","''")
                    if val != 'None':
                        values += f", '{val}'"
                    else:
                        values += f", NULL"
                except AttributeError:
                    values += f", NULL"

        command = f"INSERT INTO {tablename} ({columns}) VALUES ({values});\n"
        
        cursor.execute(command)
    
    try:
        cursor.commit()
    except pyodbc.Error as e:
        if 'There is already an object named' in str(e):
            print('Table already exists in database')
        else:
            print(command)
            print(e)