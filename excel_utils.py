import pandas as pd
import pyodbc
import argparse 
from lib.cptcode import migrate_cpt_codes


from enum import Enum, auto

from lib.healthplan import  healthplan

class MigrationType(Enum):
    cptcode = auto()
    healthplan = auto()

    def __str__(self):
        return self.name.lower()

def main(migration_type):
    # Database connection setup
    server = 'SDCDIDB9084,8414'
    database = 'clinical_ref_data'
    # username = r'DEERFIELD\\RagSingh'
    username = 'RSingh'
    password = 'Password1$'
    # driver = '{ODBC Driver 17 for SQL Server}'
    driver = '{SQL Server}'

    conn_str = (
        r'DRIVER={SQL Server};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password}'
        f'Trusted_Connection=yes'
        )
    # Establishing the connection
    # print(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}')
    conn = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Trusted_Connection=yes')
    # conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    # healthplan(cursor)
    # conn.commit()
    # return
    
    migrationType = MigrationType.__members__.get(migration_type, lambda:  None)
    if(migrationType == MigrationType.cptcode):
        migrate_cpt_codes(cursor=cursor)
    elif(migrationType == MigrationType.healthplan):
        healthplan(cursor=cursor)
    else:
        print("OOPS!!")

    conn.commit()




    # Don't forget to close the cursor and connection when done
    cursor.close()
    conn.close()

if __name__ == "__main__":
    migration_type_keys = [member.name for member in MigrationType]
    parser = argparse.ArgumentParser(description="Migrate Excel data based on the specified type.")
    parser.add_argument("migration_type")
    args = parser.parse_args()
    
    if(migration_type_keys.__contains__(args.migration_type)):
        # Call the main function with the migration type argument
        main(args.migration_type)
    else:
        print("Invalid Args!!")
        raise Exception("Invalid Args!!")
