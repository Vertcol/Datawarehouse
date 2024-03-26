import pandas as pd
import numpy as np
import json
import sqlite3
import pyodbc
import os
from settings import Settings
from tableutils import * 
#mergeTables, getSqlite, createTable, insertTable, filterColumns, excludeColumns, sizeCheck

def run(settings: Settings):
    select_tables = "SELECT name FROM sqlite_master WHERE type='table'"

    sales_con = getSqlite(settings, "go_sales.sqlite") 

    sales_tables = pd.read_sql_query(select_tables, sales_con)

    sales_country       = pd.read_sql_query("SELECT * FROM country;", sales_con)
    order_details       = pd.read_sql_query("SELECT * FROM order_details;", sales_con)
    order_header        = pd.read_sql_query("SELECT * FROM order_header;", sales_con)
    order_method        = pd.read_sql_query("SELECT * FROM order_method;", sales_con)
    product             = pd.read_sql_query("SELECT * FROM product;", sales_con)
    product_line        = pd.read_sql_query("SELECT * FROM product_line;", sales_con)
    product_type        = pd.read_sql_query("SELECT * FROM product_type;", sales_con)
    sales_retailer_site = pd.read_sql_query("SELECT * FROM retailer_site;", sales_con)
    return_reason       = pd.read_sql_query("SELECT * FROM return_reason;", sales_con)
    returned_item       = pd.read_sql_query("SELECT * FROM returned_item;", sales_con)
    sales_branch        = pd.read_sql_query("SELECT * FROM sales_branch;", sales_con)
    sales_staff         = pd.read_sql_query("SELECT * FROM sales_staff;", sales_con)
    SALES_TARGETData    = pd.read_sql_query("SELECT * FROM SALES_TARGETData;", sales_con)
    sqlite_sequence     = pd.read_sql_query("SELECT * FROM sqlite_sequence;", sales_con)
    print("Imported sales tables")

    staff_con = getSqlite(settings, "go_staff.sqlite")  # sqlite3.connect()
    staff_tables = pd.read_sql_query(select_tables, staff_con)

    course            = pd.read_sql_query("SELECT * FROM course;", staff_con)
    sales_branch      = pd.read_sql_query("SELECT * FROM sales_branch;", staff_con)
    sales_staff       = pd.read_sql_query("SELECT * FROM sales_staff;", staff_con)
    satisfaction      = pd.read_sql_query("SELECT * FROM satisfaction;", staff_con)
    satisfaction_type = pd.read_sql_query("SELECT * FROM satisfaction_type;", staff_con)
    training          = pd.read_sql_query("SELECT * FROM training;", staff_con)
    print("Imported staff tables")

    crm_con = getSqlite(settings, "go_crm.sqlite") 
    crm_tables = pd.read_sql_query(select_tables, crm_con)
                            
    age_group             = pd.read_sql_query("SELECT * FROM age_group;", crm_con)
    crm_country           = pd.read_sql_query("SELECT * FROM country;", crm_con)
    retailer              = pd.read_sql_query("SELECT * FROM retailer;", crm_con)
    retailer_contact      = pd.read_sql_query("SELECT * FROM retailer_contact;", crm_con)
    retailer_headquarters = pd.read_sql_query("SELECT * FROM retailer_headquarters;", crm_con)
    retailer_segment      = pd.read_sql_query("SELECT * FROM retailer_segment;", crm_con)
    crm_retailer_site     = pd.read_sql_query("SELECT * FROM retailer_site;", crm_con)
    retailer_type         = pd.read_sql_query("SELECT * FROM retailer_type;", crm_con)
    sales_demographic     = pd.read_sql_query("SELECT * FROM sales_demographic;", crm_con)
    sales_territory       = pd.read_sql_query("SELECT * FROM sales_territory;", crm_con)
    print("Imported crm tables")

    inventory_level = getCSV(settings, "GO_SALES_INVENTORY_LEVELSData.csv") 
    print("Imported inventory table")

    sales_forecast = getCSV(settings, "GO_SALES_PRODUCT_FORECASTData.csv") 
    print("Imported sales product forecast table")

    sql_server_conn = pyodbc.connect(f"DRIVER={{SQL Server}};SERVER={settings.server};DATABASE={settings.database};Trusted_Connection=yes")
    cursor = sql_server_conn.cursor()

    """
    Dicionary to rename all original columns to their Data Warehouse equivalent
    -  Types are encoded in the column name (COLUMN_NAME_type)
    """
    rename_mapping = {}

    with open('renames.json') as f_in:
        rename_mapping = json.load(f_in)

    # List of all vetted columns
    valid_columns = set(rename_mapping.values())

    # Merge duplicate tables into single table
    retailer_site = mergeTables(sales_retailer_site, crm_retailer_site, 'RETAILER_SITE_CODE')
    # Column name mismatch
    sales_country = sales_country.rename(columns={'COUNTRY': 'COUNTRY_EN'})
    country = mergeTables(sales_country, crm_country, 'COUNTRY_CODE')

    # Tables and surrogates to create at end         
    etl_tables = []
    surrogates = []

    # Product ETL

    # Merge
    product_etl = pd.merge(product, product_type, on="PRODUCT_TYPE_CODE")
    product_etl = pd.merge(product_etl, product_line, on="PRODUCT_LINE_CODE")

    # Rename
    product_etl = product_etl.rename(columns=rename_mapping)

    # Exclude
    product_etl = filterColumns(product_etl)

    # Assert
    sizeCheck(product_etl,10)
    product_etl

    # Create
    etl_tables.append({
        'table_name': 'Product',
        'dataframe': product_etl,
        'PK': 'PRODUCT_id',
        'SK_columns': []
    })



    # Sales Staff ETL

    # Merge
    sales_staff_etl = pd.merge(sales_staff, sales_branch, on='SALES_BRANCH_CODE')
    sales_staff_etl = pd.merge(sales_staff_etl, country, on='COUNTRY_CODE')
    sales_staff_etl = pd.merge(sales_staff_etl, sales_territory, on='SALES_TERRITORY_CODE')

    # Add
    sales_staff_etl['FULL_NAME'] = sales_staff_etl['FIRST_NAME'] + ' ' + sales_staff_etl['LAST_NAME']

    # Rename
    sales_staff_etl = sales_staff_etl.rename(columns=rename_mapping)

    # Exclude
    sales_staff_etl = filterColumns(sales_staff_etl)

    # Assert
    sizeCheck(sales_staff_etl,24)
    sales_staff_etl

    # Create
    etl_tables.append({
        'table_name': 'Sales_Staff',
        'dataframe': sales_staff_etl,
        'PK': 'SALES_STAFF_id',
        'SK_columns': ['MANAGER_id']
    })

    # Surrogates
    surrogates.append({
        'table': 'Sales_Staff',
        'column': 'MANAGER_id',
        'foreign_column': 'SALES_STAFF_id'
    }) 

    # Satisfaction type ETL

    # Rename
    satisfaction_type_etl = satisfaction_type.rename(columns=rename_mapping)

    # Exclude
    satisfaction_type_etl = filterColumns(satisfaction_type_etl)

    # Assert
    sizeCheck(satisfaction_type_etl,2)
    satisfaction_type_etl

    # Create
    etl_tables.append({
        'table_name': 'Satisfaction_Type',
        'dataframe': satisfaction_type_etl,
        'PK': 'SATISFACTION_TYPE_id',
        'SK_columns': []
    })

    # Course ETL

    # Rename
    course_etl = course.rename(columns=rename_mapping)

    # Exclude
    course_etl = filterColumns(course_etl)

    # Assert
    sizeCheck(course_etl,2)
    course_etl

    # Create
    etl_tables.append({
        'table_name': 'Course',
        'dataframe': course_etl,
        'PK': 'COURSE_id',
        'SK_columns': []
    })

    # Sales Forecast ETL

    # Rename
    sales_forecast_etl = sales_forecast.rename(columns=rename_mapping)

    # Exclude
    sales_forecast_etl = filterColumns(sales_forecast_etl)

    # Assert
    sizeCheck(sales_forecast_etl,4)
    sales_forecast_etl

    # Create
    etl_tables.append({
        'table_name': 'Sales_Forecast',
        'dataframe': sales_forecast_etl,
        'PK': 'PRODUCT_id',
        # Broken
        'SK_columns': [] #['PRODUCT_id']
    })

    # Surrogates (Broken)
    # surrogates.append({
    #     'table': 'Sales_Forecast',
    #     'foreign_table': 'Product',
    #     'column': 'PRODUCT_id',
    # }) 

    # Inventory Level ETL

    inventory_level_etl = inventory_level.rename(columns=rename_mapping)

    # Exclude
    inventory_level_etl = filterColumns(inventory_level_etl)

    # Assert
    sizeCheck(inventory_level_etl,4)
    inventory_level_etl

    # Create (BROKEN)
    #etl_tables.append(('Inventory_Level', inventory_level_etl, 'PRODUCT_id'))

    # Retailer Contact ETL

    # Merge
    retailer_contact_etl = pd.merge(retailer_contact, retailer_site, on='RETAILER_SITE_CODE')
    retailer_contact_etl = pd.merge(retailer_contact_etl, country, on='COUNTRY_CODE')
    retailer_contact_etl = pd.merge(retailer_contact_etl, sales_territory, on='SALES_TERRITORY_CODE')

    # Add
    retailer_contact_etl['FULL_NAME'] = retailer_contact_etl['FIRST_NAME'] + ' ' + retailer_contact_etl['LAST_NAME']

    # Rename 
    retailer_contact_etl = retailer_contact_etl.rename(columns=rename_mapping)

    # Exclude
    retailer_contact_etl = filterColumns(retailer_contact_etl)

    # Assert
    sizeCheck(retailer_contact_etl,24)
    retailer_contact_etl

    # Create
    etl_tables.append({
        'table_name': 'Retailer_Contact',
        'dataframe': retailer_contact_etl,
        'PK': 'RETAILER_CONTACT_id',
        'SK_columns': ['RETAILER_id']
    })

    # Surrogates
    surrogates.append({
        'table': 'Retailer_Contact',
        'foreign_table': 'Retailer',
        'column': 'RETAILER_id'
    })


    # Retailer ETL

    # Merge
    retailer_etl = pd.merge(retailer, retailer_headquarters, on='RETAILER_CODEMR')
    retailer_etl = pd.merge(retailer_etl, retailer_type, on='RETAILER_TYPE_CODE')

    # Merge and rename language columns for clarity
    retailer_etl = pd.merge(retailer_etl, retailer_segment, on='SEGMENT_CODE').rename(columns={'LANGUAGE':'SEGMENT_LANGUAGE_code'})
    retailer_etl = pd.merge(retailer_etl, country, on='COUNTRY_CODE').rename(columns={'LANGUAGE':'COUNTRY_LANGUAGE_code'})

    # Exclude columns early due to merge naming conflicts
    retailer_etl = excludeColumns(retailer_etl, ['TRIAL219','TRIAL222_x','TRIAL222_y','TRIAL222'])

    # Rename
    retailer_etl = pd.merge(retailer_etl, sales_territory, on='SALES_TERRITORY_CODE')\
        .rename(columns=rename_mapping)

    # Exclude
    retailer_etl = filterColumns(retailer_etl)

    # Assert
    sizeCheck(retailer_etl,22)
    retailer_etl

    # Create
    etl_tables.append({
        'table_name': 'Retailer',
        'dataframe': retailer_etl,
        'PK': 'RETAILER_id',
        'SK_columns': []
    })

    # Order ETL

    # Merge
    order_etl = pd.merge(order_header, order_method, on='ORDER_METHOD_CODE').rename(columns=rename_mapping)

    # Exclude redundant foreign key columns
    # RETAILER_SITE_code can be derived from RETAILER_CONTACT_id
    # SALES_BRANCH_code can be derived from SALES_STAFF_id
    order_etl = excludeColumns(order_etl, ['RETAILER_SITE_id', 'SALES_BRANCH_id'])

    # Exclude
    order_etl = filterColumns(order_etl)

    # Assert
    sizeCheck(order_etl,7)
    order_etl

    # Create
    etl_tables.append({
        'table_name': 'Orders',
        'dataframe': order_etl,
        'PK': 'ORDER_TABLE_id',
        'SK_columns': ['SALES_STAFF_id', 'RETAILER_CONTACT_id']
    })

    # Surrogates
    surrogates.append({
        'table': 'Orders',
        'foreign_table': 'Sales_Staff',
        'column': 'SALES_STAFF_id',
    }) 
    surrogates.append({
        'table': 'Orders',
        'foreign_table': 'Retailer_Contact',
        'column': 'RETAILER_CONTACT_id',
    }) 

    # Return reason ETL

    # Rename
    return_reason_etl = return_reason.rename(columns=rename_mapping)

    # Exclude
    return_reason_etl = filterColumns(return_reason_etl)

    # Assert
    sizeCheck(return_reason_etl,2)
    return_reason_etl

    # Create
    etl_tables.append({
        'table_name': 'Return_Reason',
        'dataframe': return_reason_etl,
        'PK': 'RETURN_REASON_id',
        'SK_columns': []
    })

    # Training ETL


    # Rename
    training_etl = training.rename(columns=rename_mapping)

    # Exclude
    training_etl = filterColumns(training_etl)

    # Assert
    sizeCheck(training_etl,3)
    training_etl

    # Create
    etl_tables.append({
        'table_name': 'Training',
        'dataframe': training_etl,
        'PK': None,
        'SK_columns': []
    })

    # Satisfaction ETL

    # Rename
    satisfaction_etl = satisfaction.rename(columns=rename_mapping)

    # Exclude
    satisfaction_etl = filterColumns(training_etl)

    # Assert
    sizeCheck(satisfaction_etl,3)
    satisfaction_etl

    # Create
    etl_tables.append({
        'table_name': 'Satisfaction',
        'dataframe': satisfaction_etl,
        'PK': None,
        'SK_columns': []
    })

    # Returned Item ETL

    # Rename 
    returned_item_etl = returned_item.rename(columns=rename_mapping)

    # Exclude 
    returned_item_etl = filterColumns(returned_item_etl)

    # Assert
    sizeCheck(returned_item_etl,5)
    returned_item_etl

    # Create
    etl_tables.append({
        'table_name': 'Returns',
        'dataframe': returned_item_etl,
        'PK': 'RETURNS_id',
        'SK_columns': []
    })

    # Order Details ETL

    # Rename
    order_detail_etl = order_details.rename(columns=rename_mapping)

    # Exclude
    order_detail_etl = filterColumns(order_detail_etl)

    # Assert
    sizeCheck(order_detail_etl,7)
    order_detail_etl

    # Create
    etl_tables.append({
        'table_name': 'Order_Details',
        'dataframe': order_detail_etl,
        'PK': 'ORDER_DETAIL_id',
        'SK_columns': ['PRODUCT_id']
    })

    # Surrogates
    surrogates.append({
        'table': 'Order_Details',
        'foreign_table': 'Product',
        'column': 'PRODUCT_id',
    }) 

    # Sales Target ETL

    # Drop
    sales_target_etl = SALES_TARGETData.drop('RETAILER_NAME', axis=1)

    # Rename
    sales_target_etl = sales_target_etl.rename(columns=rename_mapping)
    sales_target_etl = sales_target_etl.rename(columns={'Id':'TARGET_id'})

    # Exclude
    sales_target_etl = filterColumns(sales_target_etl)

    # Assert
    sizeCheck(sales_target_etl,7)
    sales_target_etl  

    # Create
    etl_tables.append({
        'table_name': 'Sales_Target',
        'dataframe': sales_target_etl,
        'PK': 'TARGET_id',
        'SK_columns': ['SALES_STAFF_id', 'PRODUCT_id']
    })

    # Surrogates
    surrogates.append({
        'table': 'Sales_Target',
        'foreign_table': 'Product',
        'column': 'PRODUCT_id',
    }) 

    # Surrogates
    surrogates.append({
        'table': 'Sales_Target',
        'foreign_table': 'Sales_Staff',
        'column': 'SALES_STAFF_id',
    }) 


    # Create tables

    # Drop old
    dropTables(etl_tables, cursor)

    # Create
    for table in etl_tables:
        print(f"Creating {table['table_name']}")
        createTable(table['table_name'], table['dataframe'], table['PK'], table['SK_columns'], cursor)
        insertTable(table['table_name'], table['dataframe'], table['PK'], table['SK_columns'], cursor)
        print(f"Inserted {table['table_name']}")
    
    # Update surrogates
    updateSurrogates(surrogates, cursor)

    cursor.close()


