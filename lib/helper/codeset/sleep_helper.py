from sqlite3 import Cursor

from lib.healthplan import create_table_if_not_exists_query
from lib.helper.code_set_query import insert_modality
from lib.helper.common import get_health_plan_by_id, get_yes_no_properties
from lib.helper.csv_helper import read_excel_file


sleep_code_set_table_name = "CodeSetSleep"



sleep_code_set_table_ddl =  f"""
        CREATE TABLE {sleep_code_set_table_name} (
            ID int IDENTITY(1,1) NOT NULL,
            SolutionId int NOT NULL,
            CPTCode varchar(20)  NOT NULL,
            
            
            Grouper varchar(MAX) NOT NULL,
            Quantity Int,
            Qualifier varchar(MAX) NOT NULL,
            AuthTimeFrame varchar(MAX) NOT NULL,
            TypeOfService varchar(MAX) NOT NULL,
            PlaceOfService varchar(MAX) NOT NULL,

            Plans NVARCHAR(MAX) NOT NULL,

            YEAR varchar(10) NOT NULL,
            HealthPlanId int NOT NULL,
            CONSTRAINT PK_{sleep_code_set_table_name} PRIMARY KEY (ID),
            CONSTRAINT FK_{sleep_code_set_table_name}_Solution FOREIGN KEY (SolutionId) REFERENCES Solution(ID),
            CONSTRAINT FK_{sleep_code_set_table_name}_CPTCode FOREIGN KEY (CPTCode) REFERENCES CPTCodes(CPTCode),
            CONSTRAINT FK_{sleep_code_set_table_name}Rad_HealthPlan FOREIGN KEY (HealthPlanId) REFERENCES HealthPlan(ID),
        );
        """

def insert_sleep_code_set(cursor: Cursor, solution_id: int,  cpt_code: str, grouper: str, quantity, qualifier, auth_time_frame, type_of_service, place_of_service, plans: dict, year, health_plan_id: int):
    cpt_code = str(cpt_code)
    plans = plans.__str__()
    # Ensure year is a string
    year = str(year)
     # Convert quantity to int if it's not None
    quantity = int(quantity) if quantity is not None else None
    
    insert_sql = f"""
        INSERT INTO 
        {sleep_code_set_table_name} (
            SolutionId,
            CPTCode,
            Grouper,
            Quantity,
            Qualifier,
            AuthTimeFrame,
            TypeOfService,
            PlaceOfService,
            Plans,
            [YEAR],
            HealthPlanId
        )
        SELECT
            ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1
            FROM {sleep_code_set_table_name}
            WHERE 
                SolutionId = ? AND
                CPTCode = ? AND
                Grouper = ? AND
                Quantity = ? AND
                Qualifier = ? AND
                AuthTimeFrame = ? AND
                TypeOfService = ? AND
                PlaceOfService = ? AND
                Plans = ? AND
                [YEAR] = ? AND
                HealthPlanId = ?
        );
    """
    params = (
        solution_id, cpt_code, grouper, quantity, qualifier, auth_time_frame, type_of_service, place_of_service, plans, year, health_plan_id,
        solution_id, cpt_code, grouper, quantity, qualifier, auth_time_frame, type_of_service, place_of_service, plans, year, health_plan_id
    )
    cursor.execute(insert_sql, params)
    



def process_sleep_excel_and_insert_data(cursor: Cursor, current_year: str, health_plan_id: int, excel_file, solution_id):

    cursor.execute(create_table_if_not_exists_query(sleep_code_set_table_ddl, sleep_code_set_table_name))
    
    check_if_health_plan_exists = get_health_plan_by_id(health_plan_id, cursor=cursor)
    if check_if_health_plan_exists is not None:
        excel_data = read_excel_file(excel_file, solution="SLEEP")
        for row in excel_data:
            cpt_code = row.get('included_cpt_codes')
            cpt_description: str = row.get('description')
            insert_cpt_code(cursor=cursor, cpt_code=cpt_code, description= cpt_description)
            grouper = next((value for key, value in row.items() if 'grouper_name' in key), 'N/A')
            quantity = row.get('quantity')
            qualifier= next((value for key, value in row.items() if 'qualifier' in key), 'N/A')
            auth_time_frame = next((value for key, value in row.items() if 'auth_time_frame' in key), 'N/A')
            type_of_service = next((value for key, value in row.items() if 'type_of_service' in key), 'N/A')
            place_of_service = next((value for key, value in row.items() if 'place_of_service' in key), 'N/A')
            
            plans = get_yes_no_properties(row)
            
            insert_sleep_code_set(
                cursor=cursor,
                solution_id=solution_id,
                cpt_code=cpt_code,
                grouper=grouper,
                quantity=quantity,
                qualifier=qualifier,
                auth_time_frame=auth_time_frame,
                type_of_service=type_of_service,
                place_of_service=place_of_service,
                plans=plans,
                year=current_year,
                health_plan_id=health_plan_id
            )


code_set_table_name = "CodeSet"
cpt_table_name = "CPTCodes"

codeset_set = set() 

code_set_table_name = "CodeSet"
cpt_table_name = "CPTCodes"
    
def insert_cpt_code(cursor: Cursor, cpt_code: str, description: str):
    if cpt_code not in codeset_set:
        query = f"""
            IF NOT EXISTS (SELECT 1 FROM {cpt_table_name} WHERE CPTCode = ?)
                INSERT INTO {cpt_table_name} (CPTCode, Description) VALUES (?, ?)
        """
        try:
            # Ensure cpt_code is a string and trim it to 20 characters
            cpt_code = str(cpt_code)[:20]
            
            # If description is None or empty, use a space
            description = description if description and description.strip() else ' '
            
            cursor.execute(query, (cpt_code, cpt_code, description))
            codeset_set.add(cpt_code)
        except Exception as e:
            print(f"Error inserting CPT code: {e}")
            print(f"CPT Code: {cpt_code}, Description: {description}")
            # You might want to log this error or handle it in some way
            raise  # Re-raise the exception if you want the program to stop on error
        