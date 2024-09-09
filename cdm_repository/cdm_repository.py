from datetime import datetime
import vertica_python
import pandas
import psycopg2
import logging
import os

class CdmRepository_global_metrics:

    def __init__(self, vert_conn:dict, logger: logging) -> None:
        self.vert_conn = vert_conn
        self.logger = logger
        self.project_root_dir = os.path.dirname(os.path.abspath(__file__))

    def PG_conn(self):
        pass

    def Load_from_STG_to_CDM_Vertica(self, uploud_date:str):

        with vertica_python.connect(**self.vert_conn) as connection:

            cur = connection.cursor()

            # Формируем данные для витрины
            with open(os.path.join(self.project_root_dir, 'cdm_read.sql'), 'r') as sql:
                cur.execute(sql.read(), [uploud_date, uploud_date, uploud_date])
                val = cur.fetchall()
                self.logger.info(f"{datetime.utcnow()}: Data for CDM READED succesfully. Date_update = {uploud_date}")
           
            #Формируем загрузочные данные из прочитанные в PG
            df = pandas.DataFrame(val)
            df_csv = df.to_csv(sep='|',index=False)
            self.logger.info(f"{datetime.utcnow()}: Data preparation for loading into CDM Vertica COMPLITED Successfully. Upload_day = {uploud_date}.")

            # Удаляем данные за день загрузки для избежания дублей
            with open(os.path.join(self.project_root_dir, 'cdm_delete.sql'), 'r') as sql:
                cur.execute(sql.read(),[uploud_date])
                self.logger.info(f"{datetime.utcnow()}: Old data in global_metrics deleted. Date_update = {uploud_date}")

            # Вставляем подготовленные данные за день загрузки
            with open(os.path.join(self.project_root_dir, 'cdm_copy.sql'), 'r') as sql:
                cur.copy(sql.read(),df_csv)
                self.logger.info(f"{datetime.utcnow()}: Copy data into global_metrics COMPLITED Successfully. Date_update = {uploud_date}")
