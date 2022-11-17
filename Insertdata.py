import pandas as pd
from Projcode import insert_df_rows_to_table
import re


if __name__ == "__main__":
    df = pd.read_csv("Cities_Info.csv", header=0)
    df1 = pd.read_csv("Lines_Info.csv", header=0)
    df2 = pd.read_csv("Stations_Info.csv", header=0)
    df3 = pd.read_csv("Station_Lines_Info.csv", header=0)
    df4 = pd.read_csv("Transit_Systems_Info.csv", header=0)
    df5 = pd.read_csv("Track_Info.csv", header=0)
    df6 = pd.read_csv("Track_Lines_Info.csv", header=0)
    

    
    insert_df_rows_to_table(df, 'Cities_Info')
    insert_df_rows_to_table(df1, 'Lines_Info')
    insert_df_rows_to_table(df2, 'Stations_Info')
    insert_df_rows_to_table(df3, 'Station_Lines_Info')
    insert_df_rows_to_table(df4, 'Transit_Systems_Info')
    insert_df_rows_to_table(df5, 'Track_Info')
    insert_df_rows_to_table(df6, 'Track_Lines_Info')

