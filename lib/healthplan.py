
from sqlite3 import Cursor
import pyodbc
from typing import List, Dict, Any
import csv

from lib.helper.health_plan_quries import HEALTH_PLAN_QUERIES, SOLUTION_DDL


group_name_table_name = "GroupName"
health_plan_table_name = "HealthPlan"
solution_table_name = "Solution"
solution_health_plan_mapper_table_name = "SolutionHealthPlanMapper"

get_group_data_from_db = """

SELECT
	AIMDW.dbo.DIM_HEALTHPLAN.HEALTHPLAN_ID,
	AIMDW.dbo.DIM_HEALTHPLAN.HEALTHPLAN_NAME,
	AIMDW.dbo.DIM_HEALTHPLAN.GROUP_NAME,
	AIMDW.DBO.DIM_DATE.FIRST_OF_MONTH,
	Count(AIMDW.dbo.FACT_EXAM.DIM_PRECERT_PK),
	'Radiology' solution
FROM
	AIMDW.dbo.DIM_HEALTHPLAN
INNER JOIN AIMDW.dbo.FACT_PRECERT ON
	(AIMDW.dbo.FACT_PRECERT.PRECERT_CLOSED_CLEAN_FLAG = 'Yes'
		AND AIMDW.dbo.DIM_HEALTHPLAN.DIM_HEALTHPLAN_PK = AIMDW.dbo.FACT_PRECERT.DIM_HEALTHPLAN_PK)
INNER JOIN AIMDW.DBO.DIM_DATE ON
	(AIMDW.DBO.DIM_DATE.DIM_DATE_PK = AIMDW.dbo.FACT_PRECERT.DIM_PRECERT_ORG_CLOSE_DATE_PK)
INNER JOIN AIMDW.dbo.FACT_EXAM ON
	(AIMDW.dbo.FACT_EXAM.CLOSED_CLEAN_FLAG = 'Yes'
		AND AIMDW.dbo.FACT_PRECERT.DIM_PRECERT_PK = AIMDW.dbo.FACT_EXAM.DIM_PRECERT_PK)
WHERE
	( (AIMDW.DBO.DIM_DATE.CALENDAR_DATE) >= dateadd(mm,
	datediff(mm,
	0,
	getdate())-12,
	0)
		AND 
(AIMDW.DBO.DIM_DATE.CALENDAR_DATE) < dateadd(mm,
		datediff(mm,
		0,
		getdate()),
		0) )
GROUP BY
	AIMDW.dbo.DIM_HEALTHPLAN.HEALTHPLAN_ID,
	AIMDW.dbo.DIM_HEALTHPLAN.HEALTHPLAN_NAME,
	AIMDW.dbo.DIM_HEALTHPLAN.GROUP_NAME,
	AIMDW.DBO.DIM_DATE.FIRST_OF_MONTH
	
"""

group_name_table_ddl = """

 CREATE TABLE GroupName (
    ID INT IDENTITY(1,1) PRIMARY KEY ,
    Name VARCHAR(255) NOT NULL
)
"""

health_plan_table_ddl = """
CREATE TABLE HealthPlan (
	ID INT PRIMARY KEY,
    Name VARCHAR(255) NOT NULL,
    GroupId INT NOT NULL,
    CONSTRAINT FK_HealthPlanGroupName FOREIGN KEY (GroupId) REFERENCES GroupName(ID)
);
"""

solution_health_plan_mapper_ddl = """

CREATE TABLE SolutionHealthPlanMapper (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    HEALTH_PLAN_ID INT NOT NULL,
    SOLUTION_ID INT NOT NULL,
    YEAR INT NOT NULL,
    MONTH INT NOT NULL,
    CONSTRAINT FK_SolutionHealthPlanMapper_HealthPlan FOREIGN KEY (HEALTH_PLAN_ID) REFERENCES HealthPlan(ID),
    CONSTRAINT FK_SolutionHealthPlanMapper_Solution FOREIGN KEY (SOLUTION_ID) REFERENCES Solution(ID)
)

"""




def create_table_if_not_exists_query(query: str, table_name: str):
    return f"if not exists (select * from sysobjects where name='{table_name}' and xtype='U'){query}"



def healthplan(cursor: Cursor):

    cursor.execute(create_table_if_not_exists_query(group_name_table_ddl, group_name_table_name))
    cursor.execute(create_table_if_not_exists_query(health_plan_table_ddl, health_plan_table_name))
    cursor.execute(create_table_if_not_exists_query(SOLUTION_DDL, solution_table_name))
    cursor.execute(create_table_if_not_exists_query(solution_health_plan_mapper_ddl, solution_health_plan_mapper_table_name))
    solution_map = {}
    for key, value in HEALTH_PLAN_QUERIES.items():
        if key not in solution_map:
            solution_id = insert_solution(cursor=cursor, solution_name=key)
            solution_map[key] = solution_id

        health_plans = get_health_plan_data(value)
        for health_plan in health_plans:
            group_id = insert_group_name(cursor, health_plan.get('GROUP_NAME'))
            insert_health_plan(cursor=cursor, health_plan_id=health_plan.get('HEALTHPLAN_ID'), group_id=group_id, health_plan_name=health_plan.get('HEALTHPLAN_NAME'))
            insert_solution_health_plan_mapper(cursor=cursor, health_plan_id=health_plan.get('HEALTHPLAN_ID'), solution_id=solution_map.get(key), year=health_plan.get('FIRST_OF_MONTH').year, month=health_plan.get('FIRST_OF_MONTH').month)






def get_health_plan_data(query: str) -> List[Dict[str, Any]]:
    if True:
        return read_file_from_csv()
        # Database connection setup
    server = '127.0.0.1'
    database = 'MaxView_1.9.0.750'
    username = 'sa'
    password = 'Password1$'
    driver = '{ODBC Driver 17 for SQL Server}'

    # Establishing the connection
    conn = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}')
    cursor = conn.cursor()

    # Execute your query
    cursor.execute(query)

    # Fetch all rows from the query
    rows = cursor.fetchall()

    # Get headers from the cursor's description attribute
    headers = [column[0] for column in cursor.description]

    # Construct a list of dictionaries
    results = [dict(zip(headers, row)) for row in rows]
    print(results)
    cursor.close()
    conn.close()
    return results




def read_file_from_csv():
    # Replace 'your_file.csv' with the path to your actual CSV file
    csv_file_path = 'query-result.csv'

    # Initialize an empty list to store the dictionaries
    list_of_dicts = []

    # Open the CSV file for reading
    with open(csv_file_path, mode='r', encoding='utf-8-sig') as csv_file:
        # Use the csv.DictReader to read the CSV file into a dictionary for each row
        csv_reader = csv.DictReader(csv_file)
        
        # Loop through the rows in the csv_reader
        for row in csv_reader:
            # Each row is already a dictionary, so you can directly append it to your list
            list_of_dicts.append(row)
    
    return list_of_dicts


def insert_group_name(cursor, group_name):
    # First, attempt to fetch the ID if the group name already exists
    cursor.execute("SELECT ID FROM GroupName WHERE Name = ?", (group_name,))
    result = cursor.fetchone()
    if result:
        return result[0]  # Return the existing ID if found

    # If the group name does not exist, insert it
    cursor.execute("INSERT INTO GroupName (Name) OUTPUT INSERTED.ID VALUES (?)", (group_name,))
    result = cursor.fetchone()
    if result:
        return result[0]  # Return the new ID
    
def insert_solution(cursor, solution_name):
    cursor.execute("SELECT ID FROM Solution WHERE Name = ? ", (solution_name))
    result = cursor.fetchone()
    if result:
        return result[0]
    # If the group name does not exist, insert it
    cursor.execute("INSERT INTO Solution (Name) OUTPUT INSERTED.ID VALUES (?)", (solution_name,))
    result = cursor.fetchone()
    if result:
        return result[0]  # Return the new ID


def insert_health_plan(cursor, health_plan_id, health_plan_name, group_id):
    merge_sql = """
    MERGE INTO HealthPlan AS target
    USING (SELECT ? AS ID, ? AS Name, ? AS GroupId) AS source
    ON (target.ID = source.ID)
    WHEN MATCHED THEN
        UPDATE SET Name = source.Name, GroupId = source.GroupId
    WHEN NOT MATCHED THEN
        INSERT (ID, Name, GroupId) VALUES (source.ID, source.Name, source.GroupId);
    """
    # Adjusted for SQLite syntax; assuming ID is unique, replace into to handle potential conflicts
    cursor.execute(merge_sql, (health_plan_id, health_plan_name, group_id))

def insert_solution_health_plan_mapper(cursor, health_plan_id, solution_id, year, month):
    insert_sql = """
        INSERT INTO SolutionHealthPlanMapper (HEALTH_PLAN_ID, SOLUTION_ID, YEAR, MONTH)
        SELECT ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1
            FROM SolutionHealthPlanMapper
            WHERE HEALTH_PLAN_ID = ?
            AND SOLUTION_ID = ?
            AND YEAR = ?
            AND MONTH = ?
        );
    """
    cursor.execute(insert_sql, (health_plan_id, solution_id, year, month, health_plan_id, solution_id, year, month,))
    