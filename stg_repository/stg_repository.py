from datetime import datetime
import vertica_python
import pandas
import psycopg2
import logging
import os


class StgRepository_currencies:

    def __init__(self, pg_conn:dict, vertica_conn:dict, logger: logging) -> None:
        self.pg_conn = pg_conn
        self.vertica_conn = vertica_conn
        self.logger = logger
        self.project_root_dir = os.path.dirname(os.path.abspath(__file__))

    def PG_conn(self):
        pass

    def Load_from_PG_to_Vertica_stg_currencies(self, uploud_date:str):

        #Вычитываем данные за день из PG
        with psycopg2.connect(**self.pg_conn).cursor() as cur:

            self.logger.info(f"{datetime.utcnow()}: START reading currencies data from PG. Upload_day = {uploud_date}")
            with open(os.path.join(self.project_root_dir, 'stg_currencies_read_from_pg.sql'), 'r') as sql:
                cur.execute(sql.read(), [uploud_date])
            val = cur.fetchall()
            rows_readed = len(val)
            self.logger.info(f"{datetime.utcnow()}: Reading currencies data from PG. Upload_day = {uploud_date}. COMPLITED Successfully. Rows readed {rows_readed}")
    
            #Формируем загрузочные данные из прочитанные в PG
        df = pandas.DataFrame(val)
        df_csv = df.to_csv(sep='|',index=False)
        self.logger.info(f"{datetime.utcnow()}: Data preparation for loading to Vertica COMPLITED Successfully. Data = currencies. Upload_day = {uploud_date}.")

        # Загружем сформированные данные в Vertica
        with vertica_python.connect(**self.vertica_conn) as connection:
            cur = connection.cursor()

            # Удаляем данные за день загрузки для избежания дублей
            with open(os.path.join(self.project_root_dir, 'stg_currencies_delete_old.sql'), 'r') as sql:
                cur.execute(sql.read(), [uploud_date])
            self.logger.info(f"{datetime.utcnow()}: Old data deleted. Date_update = {uploud_date}")

            # Вставляем подготовленные данные за день загрузки
            with open(os.path.join(self.project_root_dir, 'stg_currencies_copy.sql'), 'r') as sql:
                cur.copy(sql.read(),df_csv)
            self.logger.info(f"{datetime.utcnow()}: Copy data completed. Date_update = {uploud_date}")

            # Проверяем количество вставленных строк
            with open(os.path.join(self.project_root_dir, 'stg_currencies_check_count.sql'), 'r') as sql:
                cur.execute(sql.read(), [uploud_date])
            rows_added = cur.fetchone()[0]

            connection.commit()

            #Сверяем количество прочитанных и количество вставленных строк
            if (rows_readed == rows_added):
                self.logger.info(f"{datetime.utcnow()}: Loading data to Vertica COMPLITED Successfully. Data = currencies. Upload_day = {uploud_date}. Rows added {rows_added}")
            else:
                self.logger.info(f"{datetime.utcnow()}: Loading data to Vertica COMPLITED with ERROR. Data = currencies. Upload_day = {uploud_date}. Rows readed {rows_readed}. Rows added {rows_added}")



class StgRepository_transactions:

    def __init__(self, pg_conn:dict, vertica_conn:dict, logger: logging) -> None:
        self.pg_conn = pg_conn
        self.vertica_conn = vertica_conn
        self.logger = logger
        self.project_root_dir = os.path.dirname(os.path.abspath(__file__))

    def PG_conn(self):
        pass

    def Load_from_PG_to_Vertica_stg_transactions(self, transaction_dt:str):

        #Вычитываем данные за день из PG
        with psycopg2.connect(**self.pg_conn).cursor() as cur:

            self.logger.info(f"{datetime.utcnow()}: START reading transactions data from PG. Transaction_dt = {transaction_dt}")
            with open(os.path.join(self.project_root_dir, 'stg_transactions_read_from_pg.sql'), 'r') as sql:
                cur.execute(sql.read(), [transaction_dt])
            val = cur.fetchall()
            rows_readed = len(val)
            self.logger.info(f"{datetime.utcnow()}: Reading transactions data from PG. Transaction_dt = {transaction_dt}. COMPLITED Successfully. Rows readed {rows_readed}")

        #Формируем загрузочные данные из прочитанные в PG
        df = pandas.DataFrame(val)
        df_csv = df.to_csv(sep='|',index=False)
        self.logger.info(f"{datetime.utcnow()}: Data preparation for loading to Vertica COMPLITED Successfully. Data = transactions. Transaction_dt = {transaction_dt}.")

        # Загружем сформированные данные в Vertica
        with vertica_python.connect(**self.vertica_conn) as connection:

            cur = connection.cursor()

            # Удаляем данные за день загруги для избежания дублей
            with open(os.path.join(self.project_root_dir, 'stg_transactions_delete_old.sql'), 'r') as sql:
                cur.execute(sql.read(), [transaction_dt])
            self.logger.info(f"{datetime.utcnow()}: Old data deleted. Date_update = {transaction_dt}")
                
            # Вставляем подготовленные данные за день загрузки
            with open(os.path.join(self.project_root_dir, 'stg_transactions_copy.sql'), 'r') as sql:
                cur.copy(sql.read(),df_csv)
            self.logger.info(f"{datetime.utcnow()}: Copy data completed. Transaction_dt = {transaction_dt}.")
                
            # Проверяем количество вставленных строк
            with open(os.path.join(self.project_root_dir, 'stg_transactions_check_count.sql'), 'r') as sql:
                cur.execute(sql.read(), [transaction_dt])
            rows_added = cur.fetchone()[0]
                
            connection.commit()
                
            if (rows_readed == rows_added):
                self.logger.info(f"{datetime.utcnow()}: Loading data to Vertica COMPLITED Successfully. Data = transactions. Upload_day = {transaction_dt}. Rows added {rows_added}")
            else:
                self.logger.info(f"{datetime.utcnow()}: Loading data to Vertica COMPLITED with ERROR. Data = transactions. Upload_day = {transaction_dt}. Rows readed {rows_readed}. Rows added {rows_added}")