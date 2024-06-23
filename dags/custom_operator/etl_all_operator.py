from airflow.hooks.base_hook import BaseHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python import PythonOperator
import pandas as pd
import os
import numpy as np
import logging
import firebase_admin
from firebase_admin import credentials
from firebase_admin import storage
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import bigquery
from io import StringIO
from sqlalchemy import create_engine

class ExtractData(PythonOperator):
    def __init__(self, mysql_conn_id, database, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.database = database
    
    def extract_data_from_external_db(self):
        mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id, 
                                database=self.database)
        
        tables = mysql_hook.get_records("SHOW TABLES")
        dataframes = []
        table_names = []
        for table, in tables:
            column_names = [col[0] for col in mysql_hook.get_records(f"SHOW COLUMNS FROM {table}")]
            
            data = mysql_hook.get_records(f"SELECT * FROM {table}")
            df = pd.DataFrame(data, columns=column_names)
            
            dataframes.append(df)
            table_names.append(table)
            
        print(dataframes)
        print(table_names)
        return dataframes, table_names

    def execute(self, context):
        dataframes, table_names = self.extract_data_from_external_db()
        
        context['ti'].xcom_push(key='dataframes', value=dataframes)
        context['ti'].xcom_push(key='table_names', value=table_names)


class CleaningData(PythonOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def check_for_duplicates(self, df, name_df):
        duplicates = df[df.duplicated(subset=df.columns, keep=False)]
        if not duplicates.empty:
            print(f"Terdapat duplikat pada {name_df}")
            print(duplicates)
            df = df.drop_duplicates()
        else:
            print(f"Tidak ada data duplikat pada {name_df}")
        return df
    
    def convert_to_datetime(self, df):
        columns_datetime = ['created_at', 'updated_at', 'deleted_at']
        for col in columns_datetime:
            if df[col].dtype != 'datetime64[ns]':
                df[col] = pd.to_datetime(df[col], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
                df[col] = df[col].dt.strftime('%Y-%m-%d')
        return df
    
    def handle_missing_values(self, df, name_df):
        missing_columns = [col for col in df.columns if col not in ['updated_at', 'deleted_at']]
        df[missing_columns] = df[missing_columns].replace('', None)
        if df[missing_columns].isnull().any().any():
            print(f"Terdapat missing value dalam {name_df}")
            missing_sum = df[missing_columns].isnull().sum()
            print(missing_sum)
            
            columns_numeric = ['total_likes', 'amount', 'goal_amount', 'current_progress', 'is_verified', 'registered_volunteer']
            for col in missing_columns:
                if df[col].dtype != 'object' and df[col].dtype != 'datetime64[ns]' and col not in columns_numeric:
                    df[col] = df[col].fillna(np.nan)
                elif col in columns_numeric :
                    df[col] = df[col].fillna(0)
                elif df[col].dtype == 'datetime64[ns]':
                    df[col] = df[col].fillna(pd.NaT)
                else:
                    df[col] = df[col].fillna('Unknown')
        else :
            print(f"Tidak ada missing value dalam {name_df}")
            missing_sum = df[missing_columns].isnull().sum()
            print(missing_sum)
        return df
    
    def execute(self, context):
        tables = ['admins', 'applications', 'articles', 'comments', 
                    'donation_comments', 'donation_manual_comments', 
                    'donation_manuals', 'donations', 'fundraising_categories', 
                    'fundraisings', 'like_donation_comments', 'like_donation_manual_comments', 
                    'likes_comments', 'organizations', 'testimoni_volunteers', 'transactions',
                    'user_bookmark_articles', 'user_bookmark_fundraisings', 
                    'user_bookmark_volunteer_vacancies', 'users', 'volunteers']
        
        dataframes=context['ti'].xcom_pull(task_ids='extract_data', key='dataframes')
        dfs_raw_data = [dataframes[7], dataframes[10], dataframes[2], dataframes[21], dataframes[15], dataframes[18], dataframes[19], dataframes[3], dataframes[9], dataframes[20], dataframes[14], dataframes[17], dataframes[4], dataframes[13]]
        name_df = ["Donation", "Fundraising", "Application", "Volunteer_Vacancies", "Testimoni_Volunteer", "Bookmark_Fundraising", "Bookmark_Volunteer", "Article", "Fundraising_Categories", "User", "Organization", "Bookmark_Articles", "Comments", "Like Comments"]
        
        dfs_csv = []
        for x, df in enumerate(dfs_raw_data):
            if df is not None:
                file_path = f"{name_df[x]}.csv"
                df.to_csv(file_path, index=False)
                dfs_csv.append(file_path)
        
        dfs = []
        for df_csv in dfs_csv:
            df = pd.read_csv(df_csv)
            dfs.append(df)
        
        
        clean_dfs = []
        for i in range(len(dfs)):
            print(len(dfs[i]))
            df = self.check_for_duplicates(dfs[i], name_df[i])
            df = self.convert_to_datetime(dfs[i])
            df = self.handle_missing_values(dfs[i], name_df[i])
            
            clean_dfs.append(df)
        
        context['ti'].xcom_push(key='cleaned_dfs', value=clean_dfs)
        return clean_dfs


class TransformationDataWarehouseSchema(PythonOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def fact_donation_transaction(self, df_donations_manual, df_fundraisings):
        columns = ['id', 'donation_id', 'fundraising_id', 'user_id', 'amount', 'goal_amount', 'fundraising_category_id', 'organization_id', 'created_at']
        df_fact_donation = pd.DataFrame(columns=columns)
        
        # mengambil data yang sukses di df_donation 
        df_donation_success = df_donations_manual.loc[df_donations_manual['status'] == 'sukses']
        df_donation_success = df_donation_success.reset_index(drop=True)
        
        # mengisi data pada dari kolom df_donation
        df_fact_donation['id'] = range(1, len(df_donation_success) + 1)
        df_fact_donation['donation_id'] = df_donation_success['id']
        df_fact_donation['fundraising_id'] = df_donation_success['fundraising_id']
        df_fact_donation['user_id'] = df_donation_success['user_id']
        df_fact_donation['amount'] = df_donation_success['amount']
        df_fact_donation['created_at'] = df_donation_success['created_at']
        
        # merge df_fundraising
        df_merge_fact_fundraising = pd.merge(df_fact_donation, df_fundraisings, left_on='fundraising_id', right_on='id', how='left')
        df_fact_donation['goal_amount'] = df_merge_fact_fundraising['goal_amount_y']
        df_fact_donation['fundraising_category_id'] = df_merge_fact_fundraising['fundraising_category_id_y']
        df_fact_donation['organization_id'] = df_merge_fact_fundraising['organization_id_y']
        
        return df_fact_donation
    
    def fact_volunteer_applications(self, df_applications, df_volunteer_vacancies):
        # Buat Struktur kolom df_fact_volunteer_applications
        columns = ['id', 'application_id', 'vacancy_id', 'user_id', 'organization_id', 'created_at']
        df_fact_applications = pd.DataFrame(columns=columns)

        # mengisi data pada dari kolom df_application
        df_fact_applications['id'] = range(1, len(df_applications) + 1)
        df_fact_applications['application_id'] = df_applications['id']
        df_fact_applications['vacancy_id'] = df_applications['vacancy_id']
        df_fact_applications['user_id'] = df_applications['user_id']
        df_fact_applications['created_at'] = df_applications['created_at']

        # merge df_fundraising
        df_merge_fact_volunteer = pd.merge(df_fact_applications, df_volunteer_vacancies, left_on='vacancy_id', right_on='id', how='left')
        df_fact_applications['organization_id'] = df_merge_fact_volunteer['organization_id_y']
        
        return df_fact_applications
    
    def fact_volunteer_testimoni(self, df_testimoni_volunteers):
        # Buat Struktur kolom df_fact_volunteer_testimoni
        columns = ['id', 'user_id', 'vacancy_id', 'testimoni_volunteer_id', 'rating', 'created_at']
        df_fact_volunteer_testimoni = pd.DataFrame(columns=columns)

        # mengisi data pada dari kolom df_application
        df_fact_volunteer_testimoni['id'] = range(1, len(df_testimoni_volunteers) + 1)
        df_fact_volunteer_testimoni['user_id'] = df_testimoni_volunteers['user_id']
        df_fact_volunteer_testimoni['vacancy_id'] = df_testimoni_volunteers['vacancy_id']
        df_fact_volunteer_testimoni['testimoni_volunteer_id'] = df_testimoni_volunteers['id']
        df_fact_volunteer_testimoni['rating'] = df_testimoni_volunteers['rating']
        df_fact_volunteer_testimoni['created_at'] = df_testimoni_volunteers['created_at']
        
        return df_fact_volunteer_testimoni
    
    def fact_article_popular(self, df_user_bookmark_articles, df_comments, df_like_comments):
        # Buat Struktur kolom df_fact_articel_popular
        columns = ['id', 'article_id', 'bookmark_id', 'user_id', 'comment_id', 'like_comment_id', 'created_at']
        df_fact_article_popular = pd.DataFrame(columns=columns)

        # mengisi data pada dari kolom df_comment
        df_fact_article_popular['id'] = range(1, len(df_user_bookmark_articles) + 1)
        df_fact_article_popular['article_id'] = df_user_bookmark_articles['article_id']
        df_fact_article_popular['bookmark_id'] = df_user_bookmark_articles['id']
        df_fact_article_popular['user_id'] = df_user_bookmark_articles['user_id']
        df_fact_article_popular['comment_id'] = df_comments['id']
        df_fact_article_popular['like_comment_id'] = df_like_comments['id']
        df_fact_article_popular['created_at'] = df_user_bookmark_articles['created_at']
        
        return df_fact_article_popular
    
    def fact_bookmark_fundraising(self, df_bookmark_fundraising):
        df_fact_bookmark_fundraising = df_bookmark_fundraising.drop(['deleted_at', 'updated_at'], axis=1)
        df_fact_bookmark_fundraising = df_fact_bookmark_fundraising.rename(columns={'id':'bookmark_id'})
        df_fact_bookmark_fundraising['id'] = range(1, len(df_fact_bookmark_fundraising) + 1)
        df_fact_bookmark_fundraising.insert(0, 'id', df_fact_bookmark_fundraising.pop('id'))
        
        return df_fact_bookmark_fundraising
    
    def fact_bookmark_volunteer_vacancies(self, df_bookmark_volunteer):
        df_fact_bookmark_volunteer_vacancies = df_bookmark_volunteer.drop(['volunteer_vacancy_id', 'deleted_at', 'updated_at'], axis=1)
        df_fact_bookmark_volunteer_vacancies = df_fact_bookmark_volunteer_vacancies.rename(columns={'id':'bookmark_id'})
        df_fact_bookmark_volunteer_vacancies['id'] = range(1, len(df_fact_bookmark_volunteer_vacancies) + 1)
        df_fact_bookmark_volunteer_vacancies.insert(0, 'id', df_fact_bookmark_volunteer_vacancies.pop('id'))
        
        return df_fact_bookmark_volunteer_vacancies
    
    
    def dimension_table(self, df_fundraisings, df_fundraising_categories, df_donation_manuals, df_organizations, df_users, df_applications, df_volunteer_vacancies, df_testimoni_volunteers, df_articles, df_bookmark_fundraising, df_bookmark_volunteer, df_bookmark_article, df_comments, df_like_comments):
        dim_fundraisings = df_fundraisings.drop(['fundraising_category_id', 'organization_id', 'updated_at'], axis=1)
        dim_fundraising_categories = df_fundraising_categories[['id', 'name', 'created_at']]
        dim_donation_manual = df_donation_manuals.drop(['fundraising_id', 'user_id', 'updated_at'], axis=1)
        dim_organization = df_organizations.drop(['updated_at','contact'], axis=1)
        dim_user = df_users.drop(['updated_at'], axis=1)
        dim_volunteer_application = df_applications.drop(['user_id', 'vacancy_id', 'updated_at'], axis=1)
        dim_volunteer_vacancies = df_volunteer_vacancies.drop(['organization_id', 'updated_at'], axis=1)
        dim_testimoni_volunteer = df_testimoni_volunteers.drop(['user_id', 'vacancy_id', 'updated_at'], axis=1)
        dim_article = df_articles.drop(['updated_at'], axis=1)
        dim_bookmark_fundraising = df_bookmark_fundraising.drop(['fundraising_id', 'user_id', 'updated_at'], axis=1)
        dim_bookmark_volunter_vacancies = df_bookmark_volunteer.drop(['volunteer_vacancy_id', 'volunteer_vacancies_id', 'user_id', 'updated_at'], axis=1)
        dim_bookmark_article = df_bookmark_article.drop(['updated_at', 'user_id', 'article_id'], axis=1)
        
        dim_comments = df_comments.drop(['user_id', 'article_id', 'updated_at'], axis=1)
        dim_like_comments = df_like_comments.drop(['user_id', 'comment_id', 'updated_at'], axis=1)
        
        all_dimension_table = [dim_fundraisings, dim_fundraising_categories, dim_donation_manual, dim_organization, dim_user, dim_volunteer_application, dim_volunteer_vacancies, dim_testimoni_volunteer, dim_article, dim_bookmark_fundraising, dim_bookmark_volunter_vacancies, dim_bookmark_article, dim_comments, dim_like_comments]
        return all_dimension_table
    
    
    def execute(self, context):
        # ambil list clean_dfs
        clean_dfs=context['ti'].xcom_pull(task_ids='cleaning_raw_data', key='cleaned_dfs')
        
        # fact table
        fact_donation_transaction = self.fact_donation_transaction(clean_dfs[0], clean_dfs[1])
        fact_volunteer_applications = self.fact_volunteer_applications(clean_dfs[2], clean_dfs[3])
        fact_volunteer_testimoni = self.fact_volunteer_testimoni(clean_dfs[4])
        fact_article_popular = self.fact_article_popular(clean_dfs[11], clean_dfs[12], clean_dfs[13])
        fact_bookmark_fundraising = self.fact_bookmark_fundraising(clean_dfs[5])
        fact_bookmark_volunteer_vacancies = self.fact_bookmark_volunteer_vacancies(clean_dfs[6])
        
        list_fact_tables = [fact_donation_transaction, fact_volunteer_applications, fact_volunteer_testimoni, fact_article_popular, fact_bookmark_fundraising, fact_bookmark_volunteer_vacancies]
        
        # dim table
        list_dim_tables = self.dimension_table(clean_dfs[1], clean_dfs[8], clean_dfs[0], clean_dfs[10], clean_dfs[9], clean_dfs[2], clean_dfs[3], clean_dfs[4], clean_dfs[7], clean_dfs[5], clean_dfs[6], clean_dfs[11], clean_dfs[12], clean_dfs[13])
        
        context['ti'].xcom_push(key='list_fact_table', value=list_fact_tables)
        context['ti'].xcom_push(key='list_dim_tables', value=list_dim_tables)
        print(list_dim_tables)
        print(list_fact_tables)
        print("Transform berhasil!!")
        return list_fact_tables, list_dim_tables


class LoadFirebase(PythonOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def upload_files_to_firebase(self, env_dir, data_frames, tables, ds):
        # Load environment variables from .env file
        env_path = env_dir
        load_dotenv(dotenv_path=env_path)
        
        credentials_path = os.getenv('FIREBASE_CREDENTIALS_PATH')
        cred = credentials.Certificate(credentials_path)
        bucket_name = os.getenv('BUCKET_NAME')
        firebase_admin.initialize_app(cred, {
            'storageBucket': bucket_name
        })
        
        bucket = storage.bucket()
        current_date = ds.replace("-", "")
        folder_blob = bucket.blob(f"{current_date}/")
        folder_blob.upload_from_string('')
        print(f"Folder '{current_date}' created successfully.")
        
        for df, table_name in zip(data_frames, tables):
            # Convert dataframe to CSV string
            csv_str = df.to_csv(index=False)

            # Create the blob reference with folder name
            file_name_with_date = f"{current_date}_{table_name}.csv"
            file_path_in_bucket = f"{current_date}/{file_name_with_date}"
            file_ref = bucket.blob(file_path_in_bucket)
            
            # Upload CSV string to Firebase
            file_ref.upload_from_string(csv_str, content_type='text/csv')
            print(f"Dataframe {table_name} uploaded successfully as {file_name_with_date}!")
    
    def execute(self, context):
        ds = context['ds']
        dataframes=context['ti'].xcom_pull(task_ids='extract_data', key='dataframes')
        tables_name=context['ti'].xcom_pull(task_ids='extract_data', key='table_names')
        self.upload_files_to_firebase("/opt/airflow/.env", dataframes, tables_name, ds)
        return "Data success upload to Firebase"

class LoadDatabaseLocal(PythonOperator):
    def __init__(self, mysql_conn_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mysql_conn_id = mysql_conn_id

    def test_mysql_connection(self):
        try:
            mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
            conn = mysql_hook.get_conn()
            cursor = conn.cursor()
            cursor.execute("USE peduli_pintar")
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            if result:
                logging.info(f"Connection to MySQL with connection ID '{self.mysql_conn_id}' is successful.")
                return True
            else:
                logging.error(f"Connection to MySQL with connection ID '{self.mysql_conn_id}' failed.")
                return False
        except Exception as e:
            logging.error(f"Error connecting to MySQL with connection ID '{self.mysql_conn_id}': {e}")
            return False

    def create_and_insert_data(self, dfs, table_names):
        try:
            # Get SQLAlchemy connection string from MySqlHook
            mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
            connection_uri = mysql_hook.get_uri()

            # Print connection URI to debug
            logging.info(f"Original connection URI: {connection_uri}")

            # Remove any __extra__ parameters if present
            if '__extra__' in connection_uri:
                connection_uri = connection_uri.split('?')[0]
                logging.info(f"Cleaned connection URI: {connection_uri}")

            engine = create_engine(connection_uri)

            with engine.connect() as conn:
                for df, table_name in zip(dfs, table_names):
                    df.to_sql(table_name, conn, index=False, if_exists='replace')
                    logging.info(f"Table '{table_name}' created and data inserted successfully.")

            logging.info("All tables created and data inserted successfully.")
            return True

        except Exception as e:
            logging.error(f"Error creating tables and inserting data: {e}")
            return False

    def execute(self, context):
        # ds = context['ds']

        if self.test_mysql_connection():
            logging.info("Connection DB sukses!!!")

            df_dim_table = context['ti'].xcom_pull(task_ids='transform_dw_schema', key='list_dim_tables')
            df_fact_table = context['ti'].xcom_pull(task_ids='transform_dw_schema', key='list_fact_table')
            dfs = df_dim_table + df_fact_table

            logging.info(f"DataFrames retrieved: {dfs}")
            table_names = ["dim_fundraisings", "dim_fundraising_categories", "dim_donation_manual", "dim_organization", "dim_user", "dim_volunteer_application", "dim_volunteer_vacancies", "dim_testimoni_volunteer", "dim_article", "dim_bookmark_fundraising", "dim_bookmark_volunter_vacancies", "dim_bookmark_article", "dim_comments", "dim_like_comments", "fact_donation_transaction", "fact_volunteer_applications", "fact_volunteer_testimoni", "fact_article_popular", "fact_bookmark_fundraising", "fact_bookmark_volunteer_vacancies"]

            if self.create_and_insert_data(dfs, table_names):
                logging.info("Data insertion completed successfully.")
            else:
                raise Exception("Failed to create tables and insert data.")
        else:
            raise Exception("Failed to connect to MySQL database.")
        
        
# class LoadDatabaseLocal(PythonOperator):
#     def __init__(self, mysql_conn_id, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.mysql_conn_id = mysql_conn_id
    
#     def test_mysql_connection(self):
#         try:
#             mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
#             conn = mysql_hook.get_conn()
#             cursor = conn.cursor()
#             cursor.execute("USE peduli_pintar")
#             cursor.execute("SELECT 1")
#             result = cursor.fetchone()
#             cursor.close()
#             conn.close()
#             if result:
#                 logging.info(f"Connection to MySQL with connection ID '{self.mysql_conn_id}' is successful.")
#                 return True
#             else:
#                 logging.error(f"Connection to MySQL with connection ID '{self.mysql_conn_id}' failed.")
#                 return False
#         except Exception as e:
#             logging.error(f"Error connecting to MySQL with connection ID '{self.mysql_conn_id}': {e}")
#             return False
    
#     def load_db_local(self, create_table_file_path, dfs, table_names):
#         mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
#         conn = mysql_hook.get_conn()
        
#         # Create tables
#         try:
#             with open(create_table_file_path, 'r') as file:
#                 table_queries = file.read().split(';')  # Split queries by semicolon
                
#             for query in table_queries:
#                 if query.strip():  # Ignore empty lines
#                     with conn.cursor() as cursor:
#                         cursor.execute(query)
#                     conn.commit()
#         except Exception as e:
#             logging.error(f"Error creating tables: {e}")
#             return False
        
#         # Insert data
#         for table_name, df in zip(table_names, dfs):
#             try:
#                 with conn.cursor() as cursor:
#                     # Build INSERT INTO query
#                     column_names = ', '.join(df.columns)
#                     values_placeholders = ', '.join(['%s'] * len(df.columns))
#                     insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({values_placeholders})"
                    
#                     # Execute the INSERT INTO query
#                     for row in df.itertuples(index=False):
#                         cursor.execute(insert_query, row)
#                     conn.commit()
#             except Exception as e:
#                 logging.error(f"Error inserting data into table '{table_name}': {e}")
#                 return False
#             finally:
#                 conn.close()  # Ensure connection is closed after operations

#         print("Tabel berhasil dibuat dan data berhasil dimasukkan.")
#         return True
    
#     def execute(self, context):
#         if self.test_mysql_connection():
#             df_dim_table=context['ti'].xcom_pull(task_ids='transform_dw_schema', key='list_dim_tables')
#             df_fact_table=context['ti'].xcom_pull(task_ids='transform_dw_schema', key='list_fact_table')
#             dfs = df_dim_table + df_fact_table
#             print(dfs)
#             table_names = ["dim_fundraisings", "dim_fundraising_categories", "dim_donation_manual", "dim_organization", "dim_user", "dim_volunteer_application", "dim_volunteer_vacancies", "dim_testimoni_volunteer", "dim_article", "dim_bookmark_fundraising", "dim_bookmark_volunter_vacancies", "dim_bookmark_article", "fact_donation_transaction", "fact_volunteer_applications", "fact_volunteer_testimoni", "fact_article_popular", "fact_bookmark_fundraising", "fact_bookmark_volunteer_vacancies"]
            
#             self.load_db_local("dags/sql/create_tables.sql", dfs, table_names)
#             print("Connection BERHASIL!!")
#         else:
#             raise Exception("Failed to connect to MySQL database.")
        
        
# class LoadDatabaseLocal(PythonOperator):
#     def __init__(self, mysql_conn_id, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.mysql_conn_id = mysql_conn_id
    
#     def test_mysql_connection(self):
#         try:
#             mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
#             conn = mysql_hook.get_conn()
#             cursor = conn.cursor()
#             cursor.execute("USE peduli_pintar")
#             cursor.execute("SELECT 1")
#             result = cursor.fetchone()
#             cursor.close()
#             conn.close()
#             if result:
#                 logging.info(f"Connection to MySQL with connection ID '{self.mysql_conn_id}' is successful.")
#                 return True
#             else:
#                 logging.error(f"Connection to MySQL with connection ID '{self.mysql_conn_id}' failed.")
#                 return False
#         except Exception as e:
#             logging.error(f"Error connecting to MySQL with connection ID '{self.mysql_conn_id}': {e}")
#             return False
        
#     def execute(self, context):
#         # Test MySQL connection before proceeding
#         if not self.test_mysql_connection():
#             raise Exception(f"Failed to establish connection to MySQL with connection ID '{self.mysql_conn_id}'")

#         mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
#         connection = mysql_hook.get_conn()
        
#         try:
#             # Retrieve DataFrames from XCom
#             df_dim_table = context['ti'].xcom_pull(task_ids='transform_dw_schema', key='list_dim_tables')
#             df_fact_table = context['ti'].xcom_pull(task_ids='transform_dw_schema', key='list_fact_table')
            
#             # Combine DataFrames if needed
#             dfs = df_dim_table + df_fact_table
            
#             # Iterate through each DataFrame and save to corresponding table
#             with connection.cursor() as cursor:
#                 # Create database if it doesn't exist
#                 cursor.execute('CREATE DATABASE IF NOT EXISTS peduli_pintar')
#                 cursor.execute('USE peduli_pintar')
                
#                 for df in dfs:
#                     table_name = df.name  # Assuming df.name contains the table name
#                     columns = ", ".join([f"`{col}` TEXT" for col in df.columns])
#                     sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({columns}) ENGINE=InnoDB"
#                     cursor.execute(sql)
                    
#                     for i, row in df.iterrows():
#                         placeholders = ", ".join(["%s"] * len(row))
#                         sql = f"INSERT INTO `{table_name}` VALUES ({placeholders})"
#                         cursor.execute(sql, tuple(row))
                    
#                     connection.commit()
                
#                 self.log.info("Data berhasil disimpan ke tabel-tabel dalam database 'peduli_pintar'.")
        
#         except Exception as e:
#             self.log.error(f"Terjadi kesalahan saat menyimpan data ke MySQL: {str(e)}")
#             raise
        
#         finally:
#             connection.close()


class LoadGoogleBigQuery(PythonOperator):
    def __init__(self, gcp_conn, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.gcp_conn = gcp_conn
    
    def upload_df_to_gbq(self, dataset_id, table_name, df):
        gcp_conn = BaseHook.get_connection(self.gcp_conn)

        # Create a BigQuery client
        client = bigquery.Client()
        
        # Convert DataFrame to CSV
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        table_id = f"{dataset_id}.{table_name}"
        
        partition_by = bigquery.TimePartitioning(field="created_at")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            time_partitioning=partition_by
        )

        # Load CSV data from StringIO buffer
        job = client.load_table_from_file(csv_buffer, table_id, job_config=job_config)
        
        # Wait for the load job to complete
        job.result()

        # Get table information
        table = client.get_table(table_id)
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    
    def execute(self, context):
        env_path = "/opt/airflow/.env"
        load_dotenv(dotenv_path=env_path)
        dataset_id_fact_all = os.getenv("dataset_id_fact_all")
        dataset_id_dim_all = os.getenv("dataset_id_dim_all")
        
        df_fact_table=context['ti'].xcom_pull(task_ids='transform_dw_schema', key='list_fact_table')
        df_dim_table=context['ti'].xcom_pull(task_ids='transform_dw_schema', key='list_dim_tables')
        
        table_fact_table = ["fact_donation_transaction", "fact_volunteer_applications", "fact_volunteer_testimoni", "fact_article_popular", "fact_bookmark_fundraising", "fact_bookmark_volunteer_vacancies"]
        table_dim_table = ["dim_fundraisings", "dim_fundraising_categories", "dim_donation_manual", "dim_organization", "dim_user", "dim_volunteer_applictaion", "dim_volunteer_vacancies", "dim_testimoni_volunteer", "dim_article", "dim_bookmark_fundraising", "dim_bookmark_volunter_vacancies", "dim_bookmark_article", "dim_comments", "dim_like_comments"]
        
        for x in range(len(df_fact_table)):
            self.upload_df_to_gbq(dataset_id_fact_all, table_fact_table[x], df_fact_table[x])
            
        for y in range(len(df_dim_table)):
            self.upload_df_to_gbq(dataset_id_dim_all, table_dim_table[y], df_dim_table[y])
        
        return "Data success upload to Google BigQuery!"