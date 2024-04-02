import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# import task
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

import pandas as pd
import numpy as np

import psycopg2

# define a funciton to read data.csv file

def read_data_csv():
    # from https://raw.githubusercontent.com/osbm/ain3009-hw1/main/data.csv
    data = pd.read_csv('https://raw.githubusercontent.com/osbm/ain3009-hw1/main/data.csv')
    return data

def feed_sample_data_to_db():
    # connect to the database
    # data = pd.read_csv('https://raw.githubusercontent.com/osbm/ain3009-hw1/main/sample-data-postgres.csv')
    conn = psycopg2.connect(
        host='postgres',
        database='airflow',
        user='airflow',
        password='airflow'
    )

    # create a cursor
    cursor = conn.cursor()

    # drop online_sales table if it exists
    cursor.execute("DROP TABLE IF EXISTS online_sales;")

    # create a table
    cursor.execute("CREATE TABLE online_sales (sale_id SERIAL PRIMARY KEY, product_id INT, quantity INT, sale_amount DECIMAL(10, 2), sale_date DATE);")


    # loop through the data and insert each row
    cursor.execute(
        "INSERT INTO online_sales (product_id, quantity, sale_amount, sale_date) VALUES (%s, %s, %s, %s)",
        (101, 2, 40.00, '2024-03-01')
    )

    cursor.execute(
        "INSERT INTO online_sales (product_id, quantity, sale_amount, sale_date) VALUES (%s, %s, %s, %s)",
        (102, 1, 20.00, '2024-03-01')
    )

    cursor.execute(
        "INSERT INTO online_sales (product_id, quantity, sale_amount, sale_date) VALUES (%s, %s, %s, %s)",
        (103, 3, 60.00, '2024-03-02')
    )

    cursor.execute(
        "INSERT INTO online_sales (product_id, quantity, sale_amount, sale_date) VALUES (%s, %s, %s, %s)",
        (101, 1, 20.00, '2024-03-02')
    )
    # commit the transaction
    conn.commit()

    # close the connection
    conn.close()

    return None

def read_data_db():
    # connect to the database
    conn = psycopg2.connect(
        host='postgres',
        database='airflow',
        user='airflow',
        password='airflow'
    )

    # define the query
    query = "SELECT * FROM online_sales"

    # execute the query
    data = pd.read_sql(query, conn)

    # close the connection
    conn.close()

    return data

def merge_data(data1, data2):
    # merge the two dataframes
    # print(type(data1))
    # print(data2)

    data = pd.concat([data1, data2])
    # print(data)
    return data.reset_index(drop=True).to_json()

def print_data(data):
    data = pd.read_json(data)
    print(data)
    return None

def remove_nan_rows(data):
    # print the rows with NaN values and drop them
    data = pd.read_json(data)

    print(data[data.isnull().any(axis=1)])
    data = data.dropna()
    return data.to_json()

def aggregate_data(data):
    data = pd.read_json(data)
    # aggregate the data
    data = data.groupby('product_id').agg(
        total_quantity=('quantity', 'sum'),
        total_sale_amount=('sale_amount', 'sum')
    ).reset_index()

    return data.to_json()

def save_data_db(data):
    # connect to the database
    data = pd.read_json(data)
    data = data.convert_dtypes()
    conn = psycopg2.connect(
        host='postgres',
        database='airflow',
        user='airflow',
        password='airflow'
    )

    # save everything to the result table
    cursor = conn.cursor()

    # drop the table if it exists
    cursor.execute("DROP TABLE IF EXISTS result;")

    # create the table
    cursor.execute("CREATE TABLE result (product_id INT, total_quantity INT, total_sale_amount DECIMAL(10, 2));")

    # loop through the data and insert each row
    for i, row in data.iterrows():
        cursor.execute(
            "INSERT INTO result (product_id, total_quantity, total_sale_amount) VALUES (%s, %s, %s)",
            (row['product_id'], row['total_quantity'], row['total_sale_amount'])
        )


    # close the connection
    conn.close()

    return None




with DAG(
    dag_id="ain3009-hw",
    start_date=datetime.datetime(2024, 1, 1),
    schedule="@daily",
):
    # Define the tasks
    # read data.csv file
    # read a table from the database
    # Aggregate the data to calculate the total `quantity` and total `sale_amount` for each `product_id`.
    #  Load the aggregated data into a table in the data warehouse (a simple MySQL table will be sufficient). The table should have columns for `product_id`, `total_quantity`, and `total_sale_amount`.

    read_data_csv_task = PythonOperator(
        task_id="read_data_csv",
        python_callable=read_data_csv,
    )
    place_data_db_task = PythonOperator(
        task_id="place_data_db",
        python_callable=feed_sample_data_to_db,
    )

    # read a table from the database
    read_data_db_task = PythonOperator(
        task_id="read_data_db",
        python_callable=read_data_db,
    )

    # merge the two dataframes
    merge_data_task = PythonOperator(
        task_id="merge_data",
        python_callable=merge_data,
        op_args=[read_data_csv_task.output, read_data_db_task.output],
        
    )

    remove_nan_rows_task = PythonOperator(
        task_id="remove_nan_rows",
        python_callable=remove_nan_rows,
        op_args=[merge_data_task.output],
    )



    # # Aggregate the data to calculate the total `quantity` and total `sale_amount` for each `product_id`.
    aggregate_data_task = PythonOperator(
        task_id="aggregate_data",
        python_callable=aggregate_data,
        op_args=[remove_nan_rows_task.output],
    )

    print_data_task = PythonOperator(
        task_id="print_data",
        python_callable=print_data,
        op_args=[aggregate_data_task.output],
    )


    # # Load the aggregated data into a table in the data warehouse (a simple MySQL table will be sufficient). The table should have columns for `product_id`, `total_quantity`, and `total_sale_amount`.
    save_data_db = PythonOperator(
        task_id="save_data_db",
        python_callable=save_data_db,
        op_args=[aggregate_data_task.output],
    )
    place_data_db_task >> read_data_db_task
    # Define the order of the tasks
    [read_data_csv_task, read_data_db_task] >> merge_data_task
    merge_data_task >> remove_nan_rows_task
    remove_nan_rows_task >> aggregate_data_task

    aggregate_data_task >> print_data_task

    aggregate_data_task >> save_data_db

    # merge_data >> aggregate_data
    # aggregate_data >> save_data_db
