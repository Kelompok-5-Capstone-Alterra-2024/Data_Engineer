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
from google.cloud.exceptions import NotFound


class ExtractData(PythonOperator):
    def __init__(self, mysql_conn_id, database, query_select, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.database = database
        self.query_select = query_select
    
    def extract_data_from_external_db(self, query, text_columns):
        mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id, 
                                database=self.database)
        
        tables = mysql_hook.get_records("SHOW TABLES")
        
        dataframes = []
        table_names = []
        for table, in tables:
            column_names = [col[0] for col in mysql_hook.get_records(f"SHOW COLUMNS FROM {table}")]
            
            formatted_query = query.format(table=table)
            data = mysql_hook.get_records(formatted_query)
            
            df = pd.DataFrame(data, columns=column_names)
            
            # menambahkan kolom tertentu dengan ""
            if text_columns:
                for col in text_columns:
                    if col in df.columns:
                        df[col] = df[col].apply(lambda x: f'"{x}"' if isinstance(x, str) and not x.startswith('"') else x)
            
            dataframes.append(df)
            table_names.append(table)
            
        print(dataframes)
        print(table_names)
        return dataframes, table_names

    def execute(self, context):
        ds = context['ds']
        text_columns = ['message', 'content', 'content_activity', 'description']
        
        formatted_query = self.query_select.replace('{{ ds }}', ds)
        dataframes, table_names = self.extract_data_from_external_db(formatted_query, text_columns)
        
        context['ti'].xcom_push(key='dataframes', value=dataframes)
        context['ti'].xcom_push(key='table_names', value=table_names)


class CleaningData(PythonOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def check_for_duplicates(self, df, name_df):
        if df.empty:
            print(f"{name_df} is empty. Skipping duplicates check.")
            return df
        
        duplicates = df[df.duplicated(subset=df.columns, keep=False)]
        if not duplicates.empty:
            print(f"Terdapat duplikat pada {name_df}")
            print(duplicates)
            df = df.drop_duplicates()
        else:
            print(f"Tidak ada data duplikat pada {name_df}")
        return df
    
    def convert_to_datetime(self, df):
        if df.empty:
            print("DataFrame is empty. Skipping datetime conversion.")
            return df
        
        columns_datetime = ['created_at', 'updated_at', 'deleted_at']
        for col in columns_datetime:
            if df[col].dtype != 'datetime64[ns]':
                df[col] = pd.to_datetime(df[col], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
                df[col] = df[col].dt.strftime('%Y-%m-%d')
        return df
    
    def handle_missing_values(self, df, name_df):
        if df.empty:
            print(f"{name_df} is empty. Skipping missing values handling.")
            return df
        
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
        dataframes=context['ti'].xcom_pull(task_ids='extract_data', key='dataframes')
        if dataframes is None:
            raise ValueError("No dataframes found in XCom.")
        
        dfs_raw_data = [dataframes[7], dataframes[10], dataframes[2], dataframes[21], dataframes[15], dataframes[18], dataframes[19], dataframes[3], dataframes[9], dataframes[20], dataframes[14], dataframes[17], dataframes[4], dataframes[13]]
        name_df = ["Donation", "Fundraising", "Application", "Volunteer_Vacancies", "Testimoni_Volunteer", "Bookmark_Fundraising", "Bookmark_Volunteer", "Article", "Fundraising_Categories", "User", "Organization", "Bookmark_Articles", "Comments", "Like Comments"]
        
        clean_dfs = []
        for i, df in enumerate(dfs_raw_data):
            if df is not None and not df.empty:
                file_path = f"{name_df[i]}.csv"
                df.to_csv(file_path, index=False)
                
                # Baca kembali dari CSV untuk memastikan konsistensi
                df = pd.read_csv(file_path)
                
                df = self.check_for_duplicates(df, name_df[i])
                df = self.convert_to_datetime(df)
                df = self.handle_missing_values(df, name_df[i])
            else:
                df = df
                print(f"{name_df[i]} is None or empty. Skipping.")
            
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
        dim_fundraisings = df_fundraisings.drop(['fundraising_category_id', 'organization_id', 'updated_at'], axis=1) if not df_fundraisings.empty else pd.DataFrame()
        dim_fundraising_categories = df_fundraising_categories[['id', 'name', 'created_at']] if not df_fundraising_categories.empty else pd.DataFrame()
        dim_donation_manual = df_donation_manuals.drop(['fundraising_id', 'user_id', 'updated_at'], axis=1) if not df_donation_manuals.empty else pd.DataFrame()
        dim_organization = df_organizations.drop(['updated_at','contact'], axis=1) if not df_organizations.empty else pd.DataFrame()
        dim_user = df_users.drop(['updated_at'], axis=1) if not df_users.empty else pd.DataFrame()
        dim_volunteer_application = df_applications.drop(['user_id', 'vacancy_id', 'updated_at'], axis=1) if not df_applications.empty else pd.DataFrame()
        dim_volunteer_vacancies = df_volunteer_vacancies.drop(['organization_id', 'updated_at'], axis=1) if not df_volunteer_vacancies.empty else pd.DataFrame()
        dim_testimoni_volunteer = df_testimoni_volunteers.drop(['user_id', 'vacancy_id', 'updated_at'], axis=1) if not df_testimoni_volunteers.empty else pd.DataFrame()
        dim_article = df_articles.drop(['updated_at'], axis=1) if not df_articles.empty else pd.DataFrame()
        dim_bookmark_fundraising = df_bookmark_fundraising.drop(['fundraising_id', 'user_id', 'updated_at'], axis=1) if not df_bookmark_fundraising.empty else pd.DataFrame()
        dim_bookmark_volunter_vacancies = df_bookmark_volunteer.drop(['volunteer_vacancy_id', 'volunteer_vacancies_id', 'user_id', 'updated_at'], axis=1) if not df_bookmark_volunteer.empty else pd.DataFrame()
        dim_bookmark_article = df_bookmark_article.drop(['updated_at', 'user_id', 'article_id'], axis=1) if not df_bookmark_article.empty else pd.DataFrame()
        
        dim_comments = df_comments.drop(['user_id', 'article_id', 'updated_at'], axis=1) if not df_comments.empty else pd.DataFrame()
        dim_like_comments = df_like_comments.drop(['user_id', 'comment_id', 'updated_at'], axis=1) if not df_like_comments.empty else pd.DataFrame()
        
        all_dimension_table = [dim_fundraisings, dim_fundraising_categories, dim_donation_manual, dim_organization, dim_user, dim_volunteer_application, dim_volunteer_vacancies, dim_testimoni_volunteer, dim_article, dim_bookmark_fundraising, dim_bookmark_volunter_vacancies, dim_bookmark_article, dim_comments, dim_like_comments]
        return all_dimension_table
    
    def execute(self, context):
        clean_dfs=context['ti'].xcom_pull(task_ids='cleaning_raw_data', key='cleaned_dfs')
        if not clean_dfs:
            raise ValueError("No cleaned dataframes found in XCom.")
        
        # Fungsi untuk transformasi dan menambahkan ke list
        def transform_and_append(clean_df_indices, transform_func, list_to_append):
            if len(clean_df_indices) > 0 and all(i < len(clean_dfs) for i in clean_df_indices):
                dfs_to_transform = [clean_dfs[i] for i in clean_df_indices]
                if all(df.empty for df in dfs_to_transform):
                    print(f"DataFrames at indices {clean_df_indices} are empty. Skipping transformation.")
                    list_to_append.append(pd.DataFrame())
                else:
                    transformed_df = transform_func(*dfs_to_transform)
                    list_to_append.append(transformed_df)
            else:
                print(f"Error: clean_df_indices {clean_df_indices} is out of range for clean_dfs list.")
        
        list_fact_tables = []
        transform_and_append([0, 1], self.fact_donation_transaction, list_fact_tables)
        transform_and_append([2, 3], self.fact_volunteer_applications, list_fact_tables)
        transform_and_append([4], self.fact_volunteer_testimoni, list_fact_tables)
        transform_and_append([11, 12, 13], self.fact_article_popular, list_fact_tables)
        transform_and_append([5], self.fact_bookmark_fundraising, list_fact_tables)
        transform_and_append([6], self.fact_bookmark_volunteer_vacancies, list_fact_tables)
        
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
            if df is not None and not df.empty:
                # Convert dataframe to CSV string
                csv_str = df.to_csv(index=False)

                # Create the blob reference with folder name
                file_name_with_date = f"{current_date}_{table_name}.csv"
                file_path_in_bucket = f"{current_date}/{file_name_with_date}"
                file_ref = bucket.blob(file_path_in_bucket)
                
                # Upload CSV string to Firebase
                file_ref.upload_from_string(csv_str, content_type='text/csv')
                print(f"Dataframe {table_name} uploaded successfully as {file_name_with_date}!")
            else:
                print(f"Dataframe {table_name} is empty or None. Skipping upload.")
    
    def execute(self, context):
        dataframes=context['ti'].xcom_pull(task_ids='extract_data', key='dataframes')
        tables_name=context['ti'].xcom_pull(task_ids='extract_data', key='table_names')
        if dataframes is None or tables_name is None:
            raise ValueError("No dataframes or table names found in XCom.")
        
        ds = context['ds']
        self.upload_files_to_firebase("/opt/airflow/.env", dataframes, tables_name, ds)
        return "Data success upload to Firebase"


class LoadFileLocal(PythonOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def save_to_csv(self, dfs, base_dir, sub_dir, file_names, ds):
        # for df, filename in zip(dfs, file_names):
        #     if df.empty:
        #         print(f"DataFrame for {filename} is empty, skipping file save.")
        #         continue
            
        #     file_path = f"{base_dir}/{sub_dir}/{ds}_{filename}.csv"
        #     os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
        #     df.to_csv(file_path, index=False)
        #     print(f"Data dari tabel telah disimpan ke file '{file_path}'.")
        if dfs is not None and not dfs.empty:
            folder_path = f"{base_dir}/{sub_dir}/{ds}"
            os.makedirs(folder_path, exist_ok=True)
            file_path = f"{folder_path}/{file_names}.csv"
            dfs.to_csv(file_path, index=False)
            print(f"Data dari tabel telah disimpan ke file '{file_path}'.")
        else:
            print(f"DataFrame '{file_names}' kosong. Tidak disimpan ke file CSV.")
    
    def execute(self, context):
        df_fact_table=context['ti'].xcom_pull(task_ids='transform_dw_schema', key='list_fact_table')
        df_dim_table=context['ti'].xcom_pull(task_ids='transform_dw_schema', key='list_dim_tables')
        
        filename_fact_table = ["fact_donation_transaction", "fact_volunteer_applications", "fact_volunteer_testimoni", "fact_article_popular", "fact_bookmark_fundraising", "fact_bookmark_volunteer_vacancies"]
        filename_dim_table = ["dim_fundraisings", "dim_fundraising_categories", "dim_donation_manual", "dim_organization", "dim_user", "dim_volunteer_applictaion", "dim_volunteer_vacancies", "dim_testimoni_volunteer", "dim_article", "dim_bookmark_fundraising", "dim_bookmark_volunter_vacancies", "dim_bookmark_article", "dim_comments", "dim_like_comments"]
        
        ds = context['ds']
        ds = ds.replace("-", "")
        # for df, filename in zip(df_fact_table, filename_fact_table):
        #     self.save_to_csv(df, "dags/data_loaded", "fact", f"{ds}_{filename}", ds)
        
        # for df, filename in zip(df_dim_table, filename_dim_table):
        #     self.save_to_csv(df, "dags/data_loaded", "dim", f"{ds}_{filename}", ds)
        
        # # Save fact tables
        # self.save_to_csv(df_fact_table, "dags/data_loaded", "fact", f"{ds}_{filename_fact_table}", ds)
        
        # # Save dimension tables
        # self.save_to_csv(df_dim_table, "dags/data_loaded", "dim", f"{ds}_{filename_dim_table}", ds)
        
        # Save fact tables
        for df, filename in zip(df_fact_table, filename_fact_table):
            self.save_to_csv(df, "dags/data_loaded", f"fact", f"{ds}_{filename}", ds)

        # Save dimension tables
        for df, filename in zip(df_dim_table, filename_dim_table):
            self.save_to_csv(df, "dags/data_loaded", f"dim", f"{ds}_{filename}", ds)
        
        
        return "Data saved to CSV files."


class LoadGoogleBigQuery(PythonOperator):
    def __init__(self, gcp_conn, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.gcp_conn = gcp_conn
    
    def upload_df_to_gbq(self, dataset_id, table_names, ds, data_directory):
        gcp_conn = BaseHook.get_connection(self.gcp_conn)
        client = bigquery.Client()
        
        # ambil list semua file didalam direktori
        file_names = os.listdir(data_directory)
        
        for table_name in table_names:
            table_id = f"{dataset_id}.{table_name}"

            for file_name in file_names:
                # Extract partition date from the file name (assuming format is YYYYMMDD_filename.csv)
                partition_date = file_name.split('_')[0]

                # Define the table reference with partition decorator
                table_ref = client.dataset(dataset_id).table(
                    f'{table_name}${partition_date}'
                )

                # Load the CSV file into a Pandas DataFrame
                file_path = os.path.join(data_directory, file_name)
                df = pd.read_csv(file_path)
                df['updated_at_bq'] = datetime.now()  
                
                if 'created_at' in df.columns:
                    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')


                job_config = bigquery.LoadJobConfig(
                    create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    source_format=bigquery.SourceFormat.CSV,
                    skip_leading_rows=1,
                    autodetect=True,
                    time_partitioning=bigquery.TimePartitioning(
                        type_="DAY",
                        field="created_at",  
                        require_partition_filter=True
                    ),
                )

                # Load the data into the partitioned table
                load_job = client.load_table_from_dataframe(
                    df, table_ref, job_config=job_config
                )

                # load_job.result()  # Waits for the job to complete

                # print(f'Loaded {load_job.output_rows} rows into {table_ref}')
                try:
                    load_job.result()  # Tunggu job selesai
                    print(f'Loaded {load_job.output_rows} rows into {table_ref}')
                except Exception as e:
                    print(f'Failed to load data into {table_ref}: {str(e)}')

        # Convert DataFrame to CSV
        # csv_buffer = StringIO()
        # df.to_csv(csv_buffer, index=False)
        # csv_buffer.seek(0)

        
        # partition_by = bigquery.TimePartitioning(field="created_at")

        # job_config = bigquery.LoadJobConfig(
        #     source_format=bigquery.SourceFormat.CSV,
        #     skip_leading_rows=1,
        #     autodetect=True,
        #     write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        #     time_partitioning=partition_by
        # )

        # Load CSV data from StringIO buffer
        # job = client.load_table_from_file(csv_buffer, table_id, job_config=job_config)
        # job.result()

        # # Get table information
        # table = client.get_table(table_id)
        # print(
        #     "Loaded {} rows and {} columns to {}".format(
        #         table.num_rows, len(table.schema), table_id
        #     )
        # )
    
    def execute(self, context):
        env_path = "/opt/airflow/.env"
        load_dotenv(dotenv_path=env_path)
        dataset_id_fact_ds = os.getenv("dataset_id_fact_ds")
        dataset_id_dim_ds = os.getenv("dataset_id_dim_ds")
        
        # df_fact_table=context['ti'].xcom_pull(task_ids='transform_dw_schema', key='list_fact_table')
        # df_dim_table=context['ti'].xcom_pull(task_ids='transform_dw_schema', key='list_dim_tables')
        
        ds = context['ds']
        ds = ds.replace("-", "")
        
        table_fact_table = ["fact_donation_transaction", "fact_volunteer_applications", "fact_volunteer_testimoni", "fact_article_popular", "fact_bookmark_fundraising", "fact_bookmark_volunteer_vacancies"]
        table_dim_table = ["dim_fundraisings", "dim_fundraising_categories", "dim_donation_manual", "dim_organization", "dim_user", "dim_volunteer_applictaion", "dim_volunteer_vacancies", "dim_testimoni_volunteer", "dim_article", "dim_bookmark_fundraising", "dim_bookmark_volunter_vacancies", "dim_bookmark_article", "dim_comments", "dim_like_comments"]
        
        self.upload_df_to_gbq(dataset_id_fact_ds, table_fact_table, ds, f"dags/data_loaded/fact/{ds}/" )
        self.upload_df_to_gbq(dataset_id_dim_ds, table_dim_table, ds, f"dags/data_loaded/dim/{ds}/" )
        
        # # Upload fact tables
        # for x in range(len(df_fact_table)):
        #     if not df_fact_table[x].empty:
        #         self.upload_df_to_gbq(dataset_id_fact_ds, table_fact_table[x], df_fact_table[x])
        #     else:
        #         print(f"DataFrame '{table_fact_table[x]}' is empty. Skipping upload to BigQuery.")
        
        # # Upload dimension tables
        # for y in range(len(df_dim_table)):
        #     if not df_dim_table[y].empty:
        #         self.upload_df_to_gbq(dataset_id_dim_ds, table_dim_table[y], df_dim_table[y])
        #     else:
        #         print(f"DataFrame '{table_dim_table[y]}' is empty. Skipping upload to BigQuery.")
        
        return "Data successfully uploaded to Google BigQuery!"
    
    


# class LoadGoogleBigQuery(PythonOperator):
#     def __init__(self, gcp_conn, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.gcp_conn = gcp_conn
    
#     def upload_df_to_gbq(self, dataset_id, table_names, ds, data_directory):
#         gcp_conn = BaseHook.get_connection(self.gcp_conn)
#         client = bigquery.Client()
        
#         # ambil list semua file didalam direktori
#         file_names = os.listdir(data_directory)
        
#         for table_name in table_names:
#             table_id = f"{dataset_id}.{table_name}"

#             for file_name in file_names:
#                 # Extract partition date from the file name (assuming format is YYYYMMDD_filename.csv)
#                 partition_date = file_name.split('_')[0]

#                 # Define the table reference with partition decorator
#                 table_ref = client.dataset(dataset_id).table(
#                     f'{table_name}${partition_date}'
#                 )

#                 # Load the CSV file into a Pandas DataFrame
#                 file_path = os.path.join(data_directory, file_name)
#                 df = pd.read_csv(file_path)
#                 df['updated_at_bq'] = ds  


#                 job_config = bigquery.LoadJobConfig(
#                     create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
#                     write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
#                     source_format=bigquery.SourceFormat.CSV,
#                     skip_leading_rows=1,
#                     autodetect=True,
#                     time_partitioning=bigquery.TimePartitioning(
#                         type_="DAY",
#                         field="created_at",  
#                         require_partition_filter=True
#                     ),
#                 )

#                 # Load the data into the partitioned table
#                 load_job = client.load_table_from_dataframe(
#                     df, table_ref, job_config=job_config
#                 )

#                 load_job.result()  # Waits for the job to complete

#                 print(f'Loaded {load_job.output_rows} rows into {table_ref}')

    
#     def execute(self, context):
#         env_path = "/opt/airflow/.env"
#         load_dotenv(dotenv_path=env_path)
#         dataset_id_fact_ds = os.getenv("dataset_id_fact_ds")
#         dataset_id_dim_ds = os.getenv("dataset_id_dim_ds")
        
        
#         ds = context['ds']
#         ds = ds.replace("-", "")
        
#         table_fact_table = ["fact_donation_transaction", "fact_volunteer_applications", "fact_volunteer_testimoni", "fact_article_popular", "fact_bookmark_fundraising", "fact_bookmark_volunteer_vacancies"]
#         table_dim_table = ["dim_fundraisings", "dim_fundraising_categories", "dim_donation_manual", "dim_organization", "dim_user", "dim_volunteer_applictaion", "dim_volunteer_vacancies", "dim_testimoni_volunteer", "dim_article", "dim_bookmark_fundraising", "dim_bookmark_volunter_vacancies", "dim_bookmark_article", "dim_comments", "dim_like_comments"]
        
#         self.upload_df_to_gbq(dataset_id_fact_ds, table_fact_table, ds, f"dags/data_loaded/fact/{ds}/" )
#         self.upload_df_to_gbq(dataset_id_dim_ds, table_dim_table, ds, f"dags/data_loaded/dim/{ds}/" )
        
        
#         return "Data successfully uploaded to Google BigQuery!"