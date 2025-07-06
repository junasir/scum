from queue import Queue, Empty
from threading import Thread
from time import sleep
from sqlite3 import connect as sqlite3_connect
# from jun_log import JunLog
from loguru import logger


class QueueManagerSqliteOperate:
    def __init__(self, db_path="SCUM.db"):
        self.logger = logger
        self.conn = sqlite3_connect(db_path, check_same_thread=False)  # SQLite 数据库文件
        self.cursor = self.conn.cursor()
        self.task_queue = Queue(maxsize=30)
        self.consumer_thread = Thread(target=self.consumer, daemon=True)
        self.consumer_thread.start()

    # 消费队列函数
    def consumer(self):
        while True:
            try:
                task_data, result_queue = self.task_queue.get(timeout=1)
                # logger.info(f"Task received: {task_data}")
            except Empty:
                continue

            # 执行任务
            try:
                result = self.run_batch_sql(task_data)
            except Exception as e:
                self.logger.error(f"Error executing SQL batch: {e}")
                result = None

            # 将结果返回到队列
            if result_queue:
                result_queue.put(result)

            # 标记任务完成
            self.task_queue.task_done()
            sleep(0.1)

    def run_batch_sql(self, data):
        """执行一组SQL语句"""
        sql_statements = data.get("sql_statements", [])
        parameter_list = data.get("parameters", [])
        return_res = data.get("return_res", False)

        if not isinstance(sql_statements, list):
            sql_statements = [sql_statements]
        if not isinstance(parameter_list, list):
            parameter_list = [parameter_list]

        results_list = []  # 存储所有查询结果
        try:
            # 启动事务
            self.conn.execute("BEGIN")

            for idx, sql_query in enumerate(sql_statements):
                params = parameter_list[idx] if idx < len(parameter_list) else ()
                self.logger.info(f"Executing SQL: {sql_query}, Parameters: {params}")
                self.cursor.execute(sql_query, params)

                # 如果需要返回结果，获取当前语句的查询结果
                if return_res:
                    query_results = self.cursor.fetchall()
                    columns = [description[0] for description in self.cursor.description]
                    results_dict = [dict(zip(columns, row)) for row in query_results]
                    results_list.append(results_dict)

            # 提交事务
            self.conn.commit()
            if len(sql_statements) == 1 and return_res is True:
                results_list = results_list[0]
            return results_list if return_res else None
        except Exception as e:
            self.conn.rollback()  # 回滚事务
            self.logger.error(f"Transaction failed: {e}")
            raise

    def run_bat(self, sql_query, parameter=None, return_res=False):
        """提交任务并等待结果"""
        if parameter is None:
            parameter = ()
        task_data = {
            "sql_statements": sql_query,
            "parameters": parameter,
            "return_res": return_res,
        }
        result_queue = Queue()  # 用于获取结果
        self.task_queue.put((task_data, result_queue))
        return result_queue.get()  # 阻塞等待结果

    def close_sql(self):
        try:
            self.cursor.close()
            self.conn.close()
        except:
            pass


# 使用方式
if __name__ == '__main__':
    sqlite_operate = QueueManagerSqliteOperate()
    sqlite_operate2 = QueueManagerSqliteOperate("scum_back.db")
    sql_statements = [
        """SELECT * FROM base_element WHERE base_id = ?""",
    ]
    parameters = [
        (3,),
    ]
    results = sqlite_operate.run_bat(sql_statements, parameter=parameters,
                                          return_res=True)
    for item in results:
        base_element_id = item["element_id"]
        sql_query = """SELECT * FROM base_element_back WHERE element_id = ?"""
        element = sqlite_operate2.run_bat(sql_query, parameter=(base_element_id, ),
                                               return_res=True)
        if len(element) == 0:
            sql_statements = [
                """INSERT INTO base_element_back (element_id,asset_1) VALUES (?,?)"""
            ]
            parameters = [
                (base_element_id, item["asset"])

            ]
            sqlite_operate2.run_bat(sql_statements, parameter=parameters, return_res=False)

        else:
            element_dic = element[0]
            asset_1 = element_dic["asset_1"]
            asset_2 = element_dic["asset_2"]
            asset_3 = element_dic["asset_3"]
            asset_4 = element_dic["asset_4"]
            asset_5 = element_dic["asset_5"]
            if asset_2 is None:
                sql_statements = [
                    """INSERT INTO base_element_back (element_id,asset_2) VALUES (?,?)"""
                ]
                parameters = [
                    (base_element_id, item["asset"])

                ]
                sqlite_operate2.run_bat(sql_statements, parameter=parameters, return_res=False)
                continue
            if asset_3 is None:
                sql_statements = [
                    """INSERT INTO base_element_back (element_id,asset_3) VALUES (?,?)"""
                ]
                parameters = [
                    (base_element_id, item["asset"])

                ]
                sqlite_operate2.run_bat(sql_statements, parameter=parameters, return_res=False)
                continue
            if asset_4 is None:
                sql_statements = [
                    """INSERT INTO base_element_back (element_id,asset_4) VALUES (?,?)"""
                ]
                parameters = [
                    (base_element_id, item["asset"])

                ]
                sqlite_operate2.run_bat(sql_statements, parameter=parameters, return_res=False)
                continue
            if asset_5 is None:
                sql_statements = [
                    """INSERT INTO base_element_back (element_id,asset_5) VALUES (?,?)"""
                ]
                parameters = [
                    (base_element_id, item["asset"])

                ]
                sqlite_operate2.run_bat(sql_statements, parameter=parameters, return_res=False)
                continue
    sqlite_operate.close_sql()
    sqlite_operate2.close_sql()