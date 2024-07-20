import logging

from sqlite3 import Cursor

from lib.healthplan import create_table_if_not_exists_query
from lib.helper.code_set_query import insert_modality
from lib.helper.common import get_health_plan_by_id, get_yes_no_properties
from lib.helper.csv_helper import read_excel_file

codeset_set = set() 

code_set_table_name = "CodeSet"
cpt_table_name = "CPTCodes"
rbm_code_set_table_name = 'CodeSetRBM'
rbm_code_set_table_ddl = f"""
        CREATE TABLE {rbm_code_set_table_name} (
            ID int IDENTITY(1,1) NOT NULL,
            SolutionId int NOT NULL,
            ModalityId int,
            HealthPlanId int NOT NULL,
            CPTCode varchar(20) NOT NULL,
            
            "ProcedureNumber" varchar(50),
            "Procedure" varchar(MAX),
            Plans NVARCHAR(MAX) NOT NULL,

            YEAR varchar(10) NOT NULL,
            Location CHAR(2),
            CONSTRAINT FK_{rbm_code_set_table_name}_Location FOREIGN KEY (Location) REFERENCES States(ID),
            CONSTRAINT PK_{rbm_code_set_table_name} PRIMARY KEY (ID),
            CONSTRAINT FK_{rbm_code_set_table_name}_HealthPlan FOREIGN KEY (HealthPlanId) REFERENCES HealthPlan(ID),
            CONSTRAINT FK_{rbm_code_set_table_name}_Modality_asldfkj FOREIGN KEY (ModalityId) REFERENCES Modality(ID),
            CONSTRAINT FK_{rbm_code_set_table_name}_Solution FOREIGN KEY (SolutionId) REFERENCES Solution(ID),
            CONSTRAINT FK_{rbm_code_set_table_name}_CPTCode FOREIGN KEY (CPTCode) REFERENCES CPTCodes(CPTCode)
        );
        """
        
        
def insert_cardiology_code_set(
    cursor: Cursor, 
    solution_id, 
    modality_id, 
    health_plan_id, 
    cpt_code, 
    procedure_number,
    procedure, 
    plans : dict,
    year,
    location: str
    ):
    plans = plans.__str__()
    cpt_code = str(cpt_code)
    # Ensure year is a string
    year = str(year)
    insert_sql = f"""
        INSERT INTO 
        {rbm_code_set_table_name} (
            SolutionId,
            ModalityId,
            HealthPlanId,
            CPTCode,
            "Procedure",
            "ProcedureNumber",
            Plans,
            [YEAR],
            Location
        )
        SELECT
            ?, ?, ?, ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1
            FROM {rbm_code_set_table_name}
            WHERE 
                SolutionId = ? AND
                ModalityId = ? AND
                HealthPlanId = ? AND
                CPTCode = ? AND
                "Procedure" = ? AND
                "ProcedureNumber" = ? AND
                Plans = ? AND
                [YEAR] = ? AND
                Location = ?
        );
    """
    params = (
        solution_id, modality_id, health_plan_id, cpt_code, procedure, procedure_number, plans, year, location,
        solution_id, modality_id, health_plan_id, cpt_code, procedure, procedure_number, plans, year, location
    )
    try:
        cursor.execute(insert_sql, params)
    except Exception as e:
        logging.info(f"error {e}occurted on cpt code{cpt_code}")
    
    
def process_rbm_excel_and_insert_data(cursor: Cursor, current_year: str, health_plan_id: int, excel_file, solution_id, location: str):
    
    cursor.execute(create_table_if_not_exists_query(rbm_code_set_table_ddl, rbm_code_set_table_name))
    check_if_health_plan_exists = get_health_plan_by_id(health_plan_id, cursor=cursor)
    if check_if_health_plan_exists is not None:
        # RBM and CARDIOLOGY has same format
        excel_data = read_excel_file(excel_file, 'RADIOLOGY')
        for row in excel_data:
            cpt_code = row.get('included_cpt_codes')
            
            cpt_description: str = row.get('description')
            insert_cpt_code(cursor=cursor, cpt_code=cpt_code, description= cpt_description)
            modality = insert_modality(cursor=cursor, solution_id=solution_id, modality=row.get('modality'))

            procedure  = row.get('procedure_name')
            procedure_number = row.get('procedure')
            plans = get_yes_no_properties(row)
            
            insert_cardiology_code_set(
                cursor=cursor,
                solution_id=solution_id,
                modality_id=modality, 
                health_plan_id=health_plan_id,
                cpt_code=cpt_code,
                procedure=procedure,
                procedure_number=procedure_number,
                plans = plans,
                year=current_year,
                location=location
            )

    
def insert_cpt_code(cursor: Cursor, cpt_code, description):
    description = description if description and description.strip() else ' '
    cpt_code = str(cpt_code)[:20]
    if codeset_set.__contains__(cpt_code) is False:
        query =  f"""
                IF NOT EXISTS (SELECT 1 FROM {cpt_table_name} WHERE CPTCode = ?)
                        INSERT INTO {cpt_table_name} (CPTCode, Description) VALUES (?, ?)
        """
        cursor.execute(query, (cpt_code, cpt_code, description))
        codeset_set.add(cpt_code)

       