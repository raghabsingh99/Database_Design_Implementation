def get_health_plan_by_id(id, cursor) -> str:
    cursor.execute("SELECT ID FROM HealthPlan WHERE ID = ? ", (id))
    result = cursor.fetchone()
    if result:
        return result[0]
    


def get_solution_id_by_name(name: str, cursor) -> str:
    cursor.execute("SELECT ID from Solution where Name = ?", (name))
    result = cursor.fetchone()
    if result:
        return result[0]
    
def get_yes_no_properties(input_dict: dict[str, str]):
    yes_no_properties = {}
    for key, value in input_dict.items():
        if "yes_no" in key:
            if value is not None:
                yes_no_properties[key] = value.lower() == "yes"
            else: 
                yes_no_properties[key] = None
    return yes_no_properties