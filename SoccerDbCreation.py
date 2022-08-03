import csv
import os
import pandas as pd
import sqlite3
import numpy as np


def data_extraction(folder_path):
    """
    This function extracts information like table_name, column names and their types as column_name_type
    from each csv file in the folder_path
    :param folder_path: This is the path where the csv files are located.
    :return: df: This dataframe contains the table_name as csv file names and column names and their types
    as column_name_type.
    """
    file_name_list = []
    list_of_column_name_type_list = []
    for file in os.listdir(folder_path):
        if file.endswith('.csv'):
            file_name = os.path.splitext(file)[0]
            file_name_list.append(file_name)
            csv_df = pd.read_csv(os.path.join(folder_path, file))
            col_names = csv_df.columns
            column_name_type_list = []
            for column in col_names:
                col = csv_df.iloc[0][column]
                if isinstance(col, str):
                    col_type = 'text'
                    column_name_type = column+" "+col_type
                    column_name_type_list.append(column_name_type)

                elif isinstance(col, np.int64):
                    col_type = 'int'
                    column_name_type = column+" "+col_type
                    column_name_type_list.append(column_name_type)

                elif isinstance(col, float):
                    col_type = 'int'
                    column_name_type = column+" "+col_type
                    column_name_type_list.append(column_name_type)
            list_of_column_name_type_list.append(column_name_type_list)

    df = pd.DataFrame()
    df['table_name'] = file_name_list
    df['column_name_type'] = list_of_column_name_type_list
    df['column_name_type'] = df['column_name_type'].apply(", ".join)
    return df


def create_table_insert_data_to_table(database_path, folder_path, df):
    """
    This function establishes a connection with sqlite3 database and creates table with table_name as the table
     and column_name_type as the columns with their data types. It also loads data from the dataframe csv_df to the
     database tables.
    :param folder_path: This is the path where the csv files are located.
    :param database_path: This is the path of the database.
    :param df: df is the dataframe containing the table details like table_name, and column_name_type.
    :return: None
    """
    
    conn = sqlite3.connect(database_path)
    cur = conn.cursor()
    for row in range(0, len(df)):
        cur.execute("Create table {} ({})".format(df['table_name'][row], df['column_name_type'][row]))

    for file in os.listdir(folder_path):
        if file.endswith('.csv'):
            file_name = os.path.splitext(file)[0]
            csv_df = pd.read_csv(os.path.join(folder_path, file))
            csv_df.to_sql(file_name, conn, if_exists='replace', index = False)


if __name__ == '__main__':

    ROOT_FOLDER = r'C:/FILES/DOCUMENTS/SQL/W3School/Soccer/'

    DATABASE_FOLDER = os.path.join(ROOT_FOLDER, "Databases")
    DATABASE_NAME = 'SoccerDatabase.db'
    DATABASE_PATH = os.path.join(DATABASE_FOLDER, DATABASE_NAME)
    
    FILE_FOLDER = os.path.join(ROOT_FOLDER, "Data_Files")
    table_df = data_extraction(FILE_FOLDER)
    create_table_insert_data_to_table(DATABASE_PATH, FILE_FOLDER, table_df)