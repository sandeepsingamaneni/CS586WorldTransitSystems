import psycopg2
from sqlalchemy import create_engine
import os


def create_connection():
    connection = psycopg2.connect(
        host=os.environ['CLASS_DB_HOST'],
        database=os.environ['CLASS_DB_USERNAME'],
        user=os.environ['CLASS_DB_USERNAME'],
        password=os.environ['CLASS_DB_PASSWORD']
    )
    connection.autocommit = True
    return connection

def create_Cities_Info_table(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE Cities_Info (" \
                  "  City_ID  UUID NOT NULL," \
                  "  Name VARCHAR(30)," \
                  "  Longitude DECIMAL(9,6) NOT NULL," \
		  "  Latitude DECIMAL(9,6) NOT NULL,"\
                  "  Start_Year SMALLINT NOT NULL,"\
		  "  Country VARCHAR(30),"\
		  "  PRIMARY KEY (City_ID));"
    cur.execute(create_stmt)

def create_Lines_Info_table(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE Lines_Info (" \
                  "  Line_ID  UUID NOT NULL," \
                  "  Line_Name VARCHAR(50)," \
                  "  City_ID  UUID NOT NULL," \
		  "  System_ID UUID NOT NULL,"\
		  "  PRIMARY KEY (Line_ID),"\
		  "  FOREIGN KEY (City_ID) REFERENCES Cities_Info(City_ID),"\
		  "  FOREIGN KEY (System_ID) REFERENCES Transit_Systems_Info(System_ID));"
    cur.execute(create_stmt)

def create_Stations_Info_table(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE Stations_Info (" \
                  "  Station_ID  UUID NOT NULL," \
                  "  Station_Name VARCHAR(50)," \
                  "  BuildStart_Year SMALLINT NOT NULL," \
		  "  Opened_Year SMALLINT NOT NULL,"\
		  "  Closed_Year SMALLINT NOT NULL,"\
		  "  City_ID  UUID NOT NULL,"\
		  "  PRIMARY KEY (Station_ID),"\
		  "  FOREIGN KEY (City_ID) REFERENCES Cities_Info(City_ID));"
    cur.execute(create_stmt)

def create_Station_Lines_Info_table(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE Station_Lines_Info (" \
                  "  StationLine_ID  UUID NOT NULL," \
                  "  Station_ID  UUID NOT NULL," \
                  "  Line_ID  UUID NOT NULL," \
		  "  City_ID  UUID NOT NULL,"\
		  "  Date_Created DATE NOT NULL,"\
		  "  PRIMARY KEY (StationLine_ID),"\
		  "  FOREIGN KEY (City_ID) REFERENCES Cities_Info(City_ID)
		  "  FOREIGN KEY (Line_ID) REFERENCES Lines_Info(Line_ID)
		  "  FOREIGN KEY (Station_ID) REFERENCES Stations_Info(Station_ID));"
    cur.execute(create_stmt)

def create_Transit_Systems_Info_table(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE Transit_Systems_Info (" \
                  "  System_ID  UUID NOT NULL," \
                  "  System_Name VARCHAR(50)," \
		  "  City_ID  UUID NOT NULL,"\  
		  "  PRIMARY KEY (System_ID),"\
		  "  FOREIGN KEY (City_ID) REFERENCES Cities_Info(City_ID));"
    cur.execute(create_stmt)

def create_Track_Lines_Info_table(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE Track_Lines_Info (" \
                  "  TrackLine_ID  UUID NOT NULL," \
                  "  Section_ID  UUID NOT NULL," \
                  "  Line_ID  UUID NOT NULL," \
		  "  City_ID  UUID NOT NULL,"\
		  "  Date_Of_Creation DATE NOT NULL,"\
		  "  PRIMARY KEY (TrackLine_ID),"\
		  "  FOREIGN KEY (City_ID) REFERENCES Cities_Info(City_ID)
		  "  FOREIGN KEY (Line_ID) REFERENCES Lines_Info(Line_ID));"
    cur.execute(create_stmt)

def create_Track_Info_table(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE Track_Info (" \
                  "  Track_ID  UUID NOT NULL," \
                  "  Track_Build_Start_Year SMALLINT NOT NULL," \
		  "  Track_Opened_Year SMALLINT NOT NULL,"\  
		  "  Track_Closed_Year SMALLINT NOT NULL,"\
		  "  Track_Length SMALLINT NOT NULL,"\
		  "  City_ID  UUID NOT NULL,"\
		  "  PRIMARY KEY (Track_ID),"\
		  "  FOREIGN KEY (City_ID) REFERENCES Cities_Info(City_ID));"
    cur.execute(create_stmt)


def insert_df_rows_to_table(df, table_name):
    engine = create_engine(
        f"postgresql://{os.environ['CLASS_DB_USERNAME']}:"
        f"{os.environ['CLASS_DB_PASSWORD']}@"
        f"{os.environ['CLASS_DB_HOST']}:"
        f"5432/{os.environ['CLASS_DB_USERNAME']}"
    )
    df.to_sql(table_name, engine, if_exists="append", index=False)


if __name__ == "__main__":
    conn = create_connection()

   
    
    create_Cities_Info_table(conn)
    create_Transit_Systems_Info_table(conn)
    create_Lines_Info_table(conn)
    create_Stations_Info_table(conn)
    create_Station_Lines_Info_table(conn)
    create_Track_Info_table(conn)
    create_Track_Lines_Info_table(conn)

   