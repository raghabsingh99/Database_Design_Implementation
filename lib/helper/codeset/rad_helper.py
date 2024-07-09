
from sqlite3 import Cursor
import re

from lib.healthplan import create_table_if_not_exists_query

from lib.helper.code_set_query import insert_modality, rad_code_set_table_ddl, rad_code_set_table_name
from lib.helper.common import get_health_plan_by_id, get_yes_no_properties
from lib.helper.csv_helper import read_excel_file

def insert_rad_code_set(cursor: Cursor, solution_id, modality_id, health_plan_id, cpt_code, procedure, grouper, grouper_included, quantity, default_enabled, grouper_default, plans: dict, year):
    
    plans = plans.__str__()
    cpt_code = clean_string(cpt_code)
    grouper = clean_string(grouper)
    quantity = check_number(quantity)
    grouper_included = grouper_included or 'N/A'
    procedure = procedure or 'N/A'
    grouper_default = grouper_default or 'N/A'
    year = str(year)
    insert_sql = """
        INSERT INTO 
        CodeSetRad (
            SolutionId,
            ModalityId,
            HealthPlanId,
            CPTCode,
            "Procedure",
            Grouper,
            GrouperIncluded,
            Quantity,
            DefaultEnabled,
            GrouperDefault,
            Plans,
            [YEAR]
        )
        SELECT
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1
            FROM CodeSetRad
            WHERE 
                SolutionId = ? AND
                ModalityId = ? AND
                HealthPlanId = ? AND
                CPTCode = ? AND
                "Procedure" = ? AND
                Grouper = ? AND
                GrouperIncluded = ? AND
                Quantity = ? AND
                DefaultEnabled = ? AND
                GrouperDefault = ? AND
                Plans = ? AND
                [YEAR] = ?
        );
    """
    params = (
        solution_id, modality_id, health_plan_id, cpt_code, procedure, grouper, grouper_included,
        quantity, default_enabled, grouper_default, plans, year,
        solution_id, modality_id, health_plan_id, cpt_code, procedure, grouper, grouper_included,
        quantity, default_enabled, grouper_default, plans, year
    )
    cursor.execute(insert_sql, params)



def process_rad_excel_and_insert_data(cursor: Cursor, current_year: str, health_plan_id: int, excel_file, solution_id):
    cursor.execute(create_table_if_not_exists_query(rad_code_set_table_ddl, rad_code_set_table_name))
    check_if_health_plan_exists = get_health_plan_by_id(health_plan_id, cursor=cursor)
    if check_if_health_plan_exists is not None:
        excel_data = read_excel_file(excel_file, solution="RAD")
        for row in excel_data:
            
            try:
                print(row)
                cpt_code = row.get('included_cpt_codes')
                cpt_description: str = row.get('description')
                insert_cpt_code(cursor=cursor, cpt_code=cpt_code, description= cpt_description)
                modality = insert_modality(cursor=cursor, solution_id=solution_id, modality=row.get('modality'))
                grouper = next((value for key, value in row.items() if 'grouper_id' in key), None)
                grouper_included = next((value for key, value in row.items() if 'grouper_included' in key), 'N/A')
                quantity = row.get('quantity')
                default_enabled= next((value for key, value in row.items() if 'default_entered' in key), 'N/A')
                grouper_default = next((value for key, value in row.items() if 'grouper_default' in key), [])
                plans = get_yes_no_properties(row)
                procedure = next((value for key, value in row.items() if 'procedure' in key), '')

                insert_rad_code_set(cursor=cursor, solution_id=solution_id, cpt_code=cpt_code, modality_id=modality, default_enabled=default_enabled, grouper=grouper, grouper_default=grouper_default, grouper_included=grouper_included, health_plan_id=health_plan_id, plans=plans, procedure=procedure, quantity=quantity, year=current_year)
            except:
                print(f"Error while persisting {row.items()}")


code_set_table_name = "CodeSet"
cpt_table_name = "CPTCodes"

codeset_set = set() 

code_set_table_name = "CodeSet"
cpt_table_name = "CPTCodes"


def clean_string(s):
    if s is None:
        return None
    # Convert to string if it's not already
    s = str(s)
    # Remove non-breaking space and other whitespace
    s = s.replace('\xa0', '').strip()
    # Remove all non-printable characters
    return re.sub(r'[^\x20-\x7E]', '', s)

def insert_cpt_code(cursor: Cursor, cpt_code, description):
   
    if codeset_set.__contains__(cpt_code) is False:
        description = description if description and description.strip() else ' '
        cpt_code = str(cpt_code)[:20]
        
        query =  f"""
                IF NOT EXISTS (SELECT 1 FROM {cpt_table_name} WHERE CPTCode = ?)
                        INSERT INTO {cpt_table_name} (CPTCode, Description) VALUES (?, ?)
        """
        cursor.execute(query, (cpt_code, cpt_code, description))
        codeset_set.add(cpt_code)
        cursor.execute("COMMIT")
        
        
def check_number(value):
    try:
        # Try to convert the input to an int
        number = int(float(value))
        return number
    except (ValueError, TypeError):
        # If conversion fails, return 0
        return 0