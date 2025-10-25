
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os 
import sqlite3
from ingestion_db import ingest_db 

import time 

# For Logging 
import logging

logging.basicConfig(
    filename = "/Logs/get_vendor_summary.log" , 
    level = logging.DEBUG , 
    format = "%(asctime)s - %(levelname)s - %(message)s" , 
    filemode = "a"
) 

def vendor_sales_summary(conn) :  
    """
    This function helps us in creating vendor_sales_summary using other tables
    stored at vendor_inventory.db  
    """
    vendor_sales_summary = pd.read_sql_query("""
    with purchase_summary as (
        select p.VendorNumber, p.VendorName, p.Brand,p.Description , p.PurchasePrice, pp.Volume,
            pp.Price as ActualPrice,
            sum(p.Quantity) as TotalPurchaseQuantity,
            sum(p.Dollars) as TotalPurchaseDollars
        from purchases p
        join purchase_prices pp on p.Brand = pp.Brand
        where p.PurchasePrice > 0
        group by 1, 2, 3
    ),
    freight_summary as (
        select VendorNumber, sum(freight) as FreightCost
        from vendor_invoice
        group by 1
    ),
    sales_summary as (
        select VendorNo, Brand,
            sum(SalesDollars) as TotalSalesDollars,
            sum(SalesQuantity) as TotalSalesQuantity,
            sum(SalesPrice) as TotalSalesPrice , 
            sum(ExciseTax) as TotalExciseTax
        from sales
        group by VendorNo, Brand
    )
    SELECT
    ps.VendorNumber, ps.VendorName, ps.Brand,
    ps.Description, ps.PurchasePrice, ps.ActualPrice,
    ps.Volume, ps.TotalPurchaseQuantity,
    ps.TotalPurchaseDollars, ss.TotalSalesQuantity, 
    ss.TotalSalesDollars, ss.TotalSalesPrice, 
    ss.TotalExciseTax, fs.FreightCost
    FROM purchase_summary ps
    LEFT JOIN sales_summary ss
    ON ps.VendorNumber = ss. VendorNo
    AND ps.Brand = ss.Brand
    LEFT JOIN freight_summary fs
    ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    """, conn)

    return vendor_sales_summary


def clean_data(df) : 
    """
    This function helps us in cleaning the vendor_sales_summary dataframe
    """

    # Cleaning all the categorical columns and replacing null rows with 0.
    df['Volume']= df['Volume'].astype('float')
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    df.fillna(0 , inplace = True) 

    # Creating new columns for better analysis 
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = df['GrossProfit'] / df['TotalSalesDollars'] * 100 
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalestoPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']

    return df 
    

if __name__ == "__main__" : 

    conn = sqlite3.connect("vendor_inventory.db")

    logging.info("Creating Vendor Summary Table....")
    vendor_summary_creation_start_time = time.time()
    summary_df = vendor_sales_summary(conn) 
    logging.info(summary_df.info()) 
    vendor_summary_creation_end_time = time.time() 
    total_time_taken_creating_vendor_summary = (vendor_summary_creation_end_time-vendor_summary_creation_start_time)/60
    logging.info(f"Total time taken for creating vendor summary: {total_time_taken_creating_vendor_summary} mins")


    logging.info("Cleaning Data......")
    cleaning_data_start_time = time.time()
    clean_df = clean_data(summary_df) 
    logging.info(clean_df.head())  
    cleaning_data_end_time = time.time()
    total_time_cleaning_data = (cleaning_data_end_time - cleaning_data_start_time)/60
    logging.info(f"Total time taken for cleaning data: {total_time_cleaning_data} mins")

    logging.info("Ingesting Data.......")
    ingest_data_start_time = time.time() 
    ingest_db(clean_df,'vendor_sales_summary' , conn) 
    logging.info("Completed !....")
    ingest_data_end_time = time.time()
    total_time_ingesting_data = (ingest_data_end_time - ingest_data_start_time)/60
    logging.info(f"Total time taken for ingesting data: {total_time_ingesting_data} mins")