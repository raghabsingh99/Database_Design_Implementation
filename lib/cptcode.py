from sqlite3 import Cursor
import datetime
import os
import re
from lib.healthplan import create_table_if_not_exists_query
from lib.helper.codeset.codeset_switch import codeset_switch
from lib.helper.common import get_health_plan_by_id, get_solution_id_by_name
from lib.helper.code_set_query import *
from lib.helper.health_plan_quries import *
from lib.helper.location_helper import extract_state

code_set_table_name = "CodeSet"
cpt_table_name = "CPTCodes"

processed_file_log_ddl = """

CREATE TABLE ProcessedFiles (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    FilePath NVARCHAR(255) NOT NULL,
    Processed BIT NOT NULL DEFAULT 0,
    ProcessedDate DATETIME DEFAULT GETDATE(),
    Remarks NVARCHAR(255) NULL
);

"""

def get_codeset_table_ddl(table_name: str):
    return   f"""
        CREATE TABLE {table_name} (
            ID int IDENTITY(1,1) NOT NULL,
            SolutionId int NOT NULL,
            HealthPlanId int NOT NULL,
            CPTCode varchar(10) NOT NULL,
            PlanId1 bit NOT NULL,
            PlanId2 bit NOT NULL,
            PlanId3 bit,
            YEAR varchar NOT NULL,
            CONSTRAINT PK_{table_name} PRIMARY KEY (CodeSetID),
            CONSTRAINT FK_{table_name}_HealthPlan FOREIGN KEY (HealthPlanId) REFERENCES HealthPlan(ID),
            CONSTRAINT FK_{table_name}_Solution FOREIGN KEY (SolutionId) REFERENCES Solution(ID),
            CONSTRAINT FK_{table_name}_CPTCode FOREIGN KEY (CPTCode) REFERENCES {cpt_table_name}(CPTCode)
        );
        """

cpt_table_ddl = f"""
CREATE TABLE {cpt_table_name} (
	CPTCode varchar(20) NOT NULL,
	Description varchar(MAX),
	CONSTRAINT PK__CPTCodes__44DA3F2EFCDF3DDB PRIMARY KEY (CPTCode)
);

"""

solution_map: set[str] = set([])


def get_all_solutions(cursor: Cursor):
    query = """
            SELECT NAME from Solution;
        """
    cursor.execute(query)
    solutions: list[tuple[str]] = cursor.fetchall()
    for solution in solutions:
        solution_map.add(solution[0].upper())
    return solution_map


def check_if_processed(cursor : Cursor, file_path: str):
    query = """
    SELECT Processed
    FROM ProcessedFiles
    WHERE FilePath = ?
    """
    cursor.execute(query, (file_path,))
    result = cursor.fetchone()
    
    if result:
        return result[0] == 1
    else:
        return False
    


def migrate_cpt_codes(cursor: Cursor, folder_path = './temp'):
    cursor.execute(create_table_if_not_exists_query(cpt_table_ddl,cpt_table_name))
    cursor.execute(create_table_if_not_exists_query(modality_table_ddl, modality_table_name))
    cursor.execute(create_table_if_not_exists_query(processed_file_log_ddl, 'ProcessedFiles'))

    current_year = datetime.datetime.now().year
    current_year_str = str(current_year)
    get_all_solutions(cursor=cursor)
    
    
    for root, dirs, files in os.walk(folder_path):
        if current_year_str in os.path.basename(root):
            for file in files:
                if (file.endswith('.xlsx') or file.endswith('.xls')) and not file.startswith('~$'):
                    path = os.path.join(root, file)
                    print(f"Processing file: {file}")
                    solution = get_matching_element(solution_map=solution_map, filename=file)

                    if solution is not None:
                        is_processed = check_if_processed(cursor=cursor, file_path=path)
                        if is_processed is False:
                            process_excel_and_insert_data(path, cursor, file, current_year_str, solution)
                        else:
                            print(f"File {path} was already processed")



def get_matching_element(solution_map: set[str], filename: str) -> str:
    # Convert the filename to lowercase and split into words for case-insensitive comparison
    filename_words = re.split(r'[ .]', filename.lower())
    
    filename_words = ['radiology' if word == 'rbm' else word for word in filename_words]
    for element in solution_map:
        # Convert the element to lowercase for case-insensitive comparison
        element_lower = element.lower()
        # Check if the element matches any word in the filename exactly
        if element_lower in filename_words:
            return element.upper()
    
    # Return None if no match is found
    return None


    
    
codeset_set = set() 
    
def insert_cpt_code(cursor, cpt_code, description):
    if codeset_set.__contains__(cpt_code) is False:
        query =  f"""
                IF NOT EXISTS (SELECT 1 FROM {cpt_table_name} WHERE CPTCode = ?)
                        INSERT INTO {cpt_table_name} (CPTCode, Description) VALUES (?, ?)
        """
        cursor.execute(query, (cpt_code, cpt_code, description))
        codeset_set.add(cpt_code)


def insert_code_set(cursor: Cursor, solution_id, health_plan_id, cpt_code, plan1, plan2, plan3, year):
    insert_sql = f"""
        INSERT INTO 
        {code_set_table_name} (
            SolutionId,
            HealthPlanId,
            CPTCode,
            PlanId1,
            PlanId2,
            PlanId3,
            [YEAR]
        )
        SELECT
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?
        WHERE NOT EXISTS (
            SELECT 1
            FROM CodeSet
            WHERE 
                SolutionId = ? AND
                HealthPlanId = ? AND
                CPTCode = ? AND
                PlanId1 = ? AND
                PlanId2 = ? AND
                PlanId3 = ? AND
                [YEAR] = ?
        );
    """
    cursor.execute(insert_sql, (solution_id, health_plan_id,cpt_code, plan1, plan2, plan3, year,solution_id, health_plan_id,cpt_code, plan1, plan2, plan3, year))



def process_excel_and_insert_data(excel_file, cursor, file, current_year_str: str, solution: str):
        
        health_plan_id = get_first_number_from_filename(filename=file)
        
        if health_plan_id is not None:
            check_if_health_plan_exists = get_health_plan_by_id(health_plan_id, cursor=cursor)
            if check_if_health_plan_exists is not None:
                solution_id = get_solution_id_by_name(solution, cursor=cursor)
                location = extract_state(filename=file)
                codeset_switch(cursor=cursor, solution_id=solution_id, solution=solution, current_year=current_year_str, health_plan_id=health_plan_id, excel_file=excel_file, location=location)



def get_first_number_from_filename(filename: str) -> int:
    # Use regular expression to find the first number in the filename
    match = re.search(r'\d+', filename)
    if match:
        # Convert the matched string to an integer
        return int(match.group())
    
    # Return None if no number is found
    return None