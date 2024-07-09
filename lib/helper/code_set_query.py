
from sqlite3 import Cursor
from .health_plan_quries import modality_table_name

rad_code_set_table_name = "CodeSetRad"

rad_code_set_table_ddl = f"""
        CREATE TABLE {rad_code_set_table_name} (
            ID int IDENTITY(1,1) NOT NULL,
            SolutionId int NOT NULL,
            ModalityId int NOT NULL,
            HealthPlanId int NOT NULL,
            CPTCode varchar(20) NOT NULL,
            
            "Procedure" varchar(MAX),
            Grouper varchar(MAX) NOT NULL,
            GrouperIncluded varchar(MAX) NOT NULL,
            Quantity Int,
            DefaultEnabled VARCHAR(MAX) NOT NULL,
            GrouperDefault VARCHAR(MAX),
            Plans NVARCHAR(MAX) NOT NULL,

            YEAR varchar(10) NOT NULL,
            CONSTRAINT PK_{rad_code_set_table_name} PRIMARY KEY (ID),
            CONSTRAINT FK_{rad_code_set_table_name}_HealthPlan FOREIGN KEY (HealthPlanId) REFERENCES HealthPlan(ID),
            CONSTRAINT FK_{rad_code_set_table_name}_Modality_asldfkj FOREIGN KEY (ModalityId) REFERENCES Modality(ID),
            CONSTRAINT FK_{rad_code_set_table_name}_Solution FOREIGN KEY (SolutionId) REFERENCES Solution(ID),
            CONSTRAINT FK_{rad_code_set_table_name}_CPTCode FOREIGN KEY (CPTCode) REFERENCES CPTCodes(CPTCode)
        );
        """







def insert_modality(cursor: Cursor, solution_id,  modality):
    if modality is None:
        return None
    cursor.execute(f"SELECT ID FROM {modality_table_name} WHERE Name = ? and SolutionId = ?", (modality, solution_id))
    result = cursor.fetchone()
    if result:
        return result[0]
    # If the group name does not exist, insert it
    cursor.execute(f"INSERT INTO {modality_table_name} (Name, SolutionId) OUTPUT INSERTED.ID VALUES (?, ?)", (modality,solution_id))
    result = cursor.fetchone()
    if result:
        return result[0]  # Return the new ID