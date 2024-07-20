from sqlite3 import Cursor
from lib.helper.codeset.cadiology_helper import process_cardiology_excel_and_insert_data
from lib.helper.codeset.msk_helper import process_msk_excel_and_insert_data
from lib.helper.codeset.rad_helper import  process_rad_excel_and_insert_data
from lib.helper.codeset.rbm_helper import process_rbm_excel_and_insert_data
from lib.helper.codeset.sleep_helper import process_sleep_excel_and_insert_data
from lib.helper.codeset.rehab_helper import process_rehab_excel_and_insert_data

def insert_file_log(cursor: Cursor, file_path: str):
    query = """
    INSERT INTO ProcessedFiles (FilePath, Processed, ProcessedDate, Remarks)
    VALUES (?, 1, GETDATE(), 'Initial record')
    """
    cursor.execute(query, (file_path,))

def codeset_switch(cursor: Cursor, current_year: str, health_plan_id: int, excel_file, solution_id, solution: str, location: str):
    solution = solution.upper()
    if solution == "RAD":
        process_rad_excel_and_insert_data(current_year=current_year, cursor=cursor, excel_file=excel_file, health_plan_id=health_plan_id,solution_id=solution_id, location = location) 
        insert_file_log(cursor=cursor, file_path=excel_file)
    elif solution == "SLEEP":
        process_sleep_excel_and_insert_data(current_year=current_year, cursor=cursor, excel_file=excel_file, health_plan_id=health_plan_id,solution_id=solution_id, location = location) 
        insert_file_log(cursor=cursor, file_path=excel_file)
    elif solution == "REHAB":
        process_rehab_excel_and_insert_data(current_year=current_year, cursor=cursor, excel_file=excel_file, health_plan_id=health_plan_id,solution_id=solution_id, location = location) 
        insert_file_log(cursor=cursor, file_path=excel_file)
    elif solution == "MSK":
        process_msk_excel_and_insert_data(current_year=current_year, cursor=cursor, excel_file=excel_file, health_plan_id=health_plan_id,solution_id=solution_id, location = location) 
        insert_file_log(cursor=cursor, file_path=excel_file)
    elif solution == "CARDIOLOGY":
        process_cardiology_excel_and_insert_data(current_year=current_year, cursor=cursor, excel_file=excel_file, health_plan_id=health_plan_id,solution_id=solution_id, location = location) 
        insert_file_log(cursor=cursor, file_path=excel_file)
    elif solution == "RBM" or solution == "RADIOLOGY":
        process_rbm_excel_and_insert_data(current_year=current_year, cursor=cursor, excel_file=excel_file, health_plan_id=health_plan_id,solution_id=solution_id, location = location) 
        insert_file_log(cursor=cursor, file_path=excel_file)
    else:
        print(f"Solution {solution} not implimented !!!")
    cursor.execute("COMMIT")



