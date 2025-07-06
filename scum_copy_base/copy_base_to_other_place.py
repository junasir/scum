import sqlite3
from sqlite_class import QueueManagerSqliteOperate

# 修改下面内容
flag_id = "14725"
steam_id = "76561198317370930"
new_x = "-7803.029"
new_y = "-124538.578"
new_z = "41642.902"
new_rotation_roll = "0"
db_file_path = "SCUM.db"


sqlite_operate = QueueManagerSqliteOperate(db_file_path)
# 获取领地旗的基地ID
sql_statements = [
    """SELECT * FROM base_element WHERE element_id = ?""",
]
parameters = [
    (flag_id,),
]
results = sqlite_operate.run_bat(sql_statements, parameter=parameters, return_res=True)
results_id = results[0]
base_id_new = results_id["base_id"]
base_x = results_id["location_x"]
base_y = results_id["location_y"]
base_z = results_id["location_z"]
base_z_z = results_id["rotation_roll"]

delta_x = float(new_x) - float(base_x)
delta_y = float(new_y) - float(base_y)
delta_z = float(new_z) - float(base_z)


# 获取玩家的ID
sql_statements = [
    """SELECT * FROM user_profile WHERE user_id = ?""",
]
parameters = [
    (steam_id,),
]
results_steam_id = sqlite_operate.run_bat(sql_statements, parameter=parameters, return_res=True)
results_steam_id = results_steam_id[0]
user_id = results_steam_id["id"]

# 获取玩家对应的囚犯ID
sql_statements = [
    """SELECT * FROM prisoner WHERE user_profile_id = ?""",
]
parameters = [
    (user_id,),
]
results_prisoner_id = sqlite_operate.run_bat(sql_statements, parameter=parameters, return_res=True)
results_prisoner_id = results_prisoner_id[0]
prisoner_id = results_steam_id["id"]

# 获取base表中的最后的ID
sql_statements = [
    """SELECT * FROM base ORDER BY id DESC LIMIT 1;""",
]
parameters = [
    (),
]
results_base_id = sqlite_operate.run_bat(sql_statements, parameter=parameters, return_res=True)
if len(results_base_id) != 0:
    results_base_id = results_base_id[0]
    results_base_id = results_base_id["id"]
else:
    results_base_id = 1

# 获取base_element表中的最后的ID
sql_statements = [
    """SELECT * FROM base_element ORDER BY element_id DESC LIMIT 1;""",
]
parameters = [
    (),
]
results_base_element_id = sqlite_operate.run_bat(sql_statements, parameter=parameters, return_res=True)
if len(results_base_element_id) != 0:
    results_base_element_id = results_base_element_id[0]
    results_base_element_id = results_base_element_id["element_id"] + 2
else:
    results_base_element_id = 1

# 创建新的基地
last_new_1 = results_base_id + 2

sql_query = """INSERT INTO base (id,location_x,location_y,size_x,size_y,name,map_id,owner_user_profile_id,is_owned_by_player, bounds_min_x, bounds_min_y, bounds_max_x, bounds_max_y) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"""
sqlite_operate.run_bat(sql_query, parameter=(last_new_1, new_x, new_y, 0, 0, f"Base #{last_new_1}", 1, user_id, 0, -10000, -10000, 10000, 10000),
                            return_res=False)

# 元素迁移【包括喷漆】

sql_statements = [
    """SELECT * FROM base_element WHERE base_id = ?""",
]
parameters = [
    (base_id_new,),
]
results_ori_base = sqlite_operate.run_bat(sql_statements, parameter=parameters, return_res=True)
for item in results_ori_base:
    ori_element_id = item["element_id"]
    element = item["asset"]
    location_x = item["location_x"]
    location_y = item["location_y"]
    location_z = item["location_z"]
    rotation_pitch = item["rotation_pitch"]
    rotation_yaw = item["rotation_yaw"]
    rotation_roll = item["rotation_roll"]
    rotation_roll = rotation_roll + float(new_rotation_roll)
    new_location_x = location_x + delta_x
    new_location_y = location_y + delta_y
    new_location_z = location_z + delta_z
    if element == "/Game/ConZ_Files/BaseBuilding/BaseElements/BP_Base_Flag.BP_Base_Flag_C":
        new_location_x = new_x
        new_location_y = new_y
        new_location_z = new_z
        continue
    sql_statements = """INSERT INTO base_element (element_id,base_id,location_x,location_y,location_z,rotation_pitch,rotation_yaw,rotation_roll,scale_x,scale_y,
        scale_z, asset, element_health, owner_profile_id,quality,creator_prisoner_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    parameters = (results_base_element_id, last_new_1, new_location_x, new_location_y, new_location_z, rotation_pitch, rotation_yaw , rotation_roll, 1, 1, 1,
         element, 1, user_id, 1, prisoner_id)
    sqlite_operate.run_bat(sql_statements, parameter=parameters, return_res=False)
    sql_statements_color = [
        """SELECT * FROM base_element_coloring WHERE element_id = ?""",
    ]
    parameters = [
        (ori_element_id,),
    ]
    results_ori_color = sqlite_operate.run_bat(sql_statements_color, parameter=parameters, return_res=True)
    if len(results_ori_color) == 1:
        results_ori_color = results_ori_color[0]
        results_ori_color_element_part_index = results_ori_color["element_part_index"]
        results_ori_color_element_color_index = results_ori_color["element_color_index"]
        sql_statements_insert_color = """INSERT INTO base_element_coloring (element_id,element_part_index,element_color_index) VALUES (?,?,?)"""
        parameters = (
            results_base_element_id, results_ori_color_element_part_index, results_ori_color_element_color_index)
        sqlite_operate.run_bat(sql_statements_insert_color, parameter=parameters, return_res=False)
    results_base_element_id += 1

sqlite_operate.close_sql()