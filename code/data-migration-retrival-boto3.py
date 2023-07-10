import pandas as pd
import boto3
import json
from sqlalchemy import create_engine, text

def load_data_from_s3_to_rds1(s3_bucket, s3_folder, db_host, db_port, db_name, db_user, db_password, table_name, aws_access_key_id, aws_secret_access_key):
    # Create the SQLAlchemy engine
    engine1 = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}')

    create_db_statement = text(f'CREATE DATABASE IF NOT EXISTS {db_name}')

    with engine1.connect() as conn_no_db1:
     conn_no_db1.execute(create_db_statement)  


    engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    
    # Connect to the S3 bucket using access keys
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    # List objects in the S3 folder
    s3_objects = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_folder)

    # Get the list of JSON files
    file_list = [obj['Key'] for obj in s3_objects['Contents'] if obj['Key'].endswith('.json')]

    # Initialize an empty DataFrame
    df = pd.DataFrame()

    # Read and concatenate the JSON files in s3 folder into dataframe for loading into RDS
    for file in file_list:
        s3_object = s3_client.get_object(Bucket=s3_bucket, Key=file)
        json_data = s3_object['Body'].read().decode('utf-8')
        df = pd.concat([df, pd.json_normalize(json.loads(json_data))], ignore_index=True)


        # Create the SQLAlchemy engine
        engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

        df = df.astype(str)

        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    
    



def Sample_query_from_RDS_table(db_host, db_port, db_user, db_password, db_name, table_name):
    try:
        # Create the SQLAlchemy engine
        engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

        # Connect to the database
        with engine.connect() as connection:
            # Query all data from the table
            query = f"SELECT * FROM {table_name}"
            result = connection.execute(query)

            # Fetch all the rows
            rows = result.fetchall()

            # Print the rows
            for row in rows:
                print(row)

    except Exception as e:
        print("Error occurred while connecting to the database:", str(e))










# Enter your AWS credentials 

s3_bucket = "xxxxxx"
s3_folder = "xxxxxxxx"

rds_host = "xxxxxxxxxxx"
rds_port = 3306  # Replace with the actual port number
rds_database = "xxxxxxx"
rds_user = "xxxxxx"
rds_pass = "xxxxxxxx"
table_name = "xxxxxx"



# Specify your AWS access key and secret key
access_key = '..............'
secret_key = '.................'

#Driver code

load_data_from_s3_to_rds1(s3_bucket, s3_folder,rds_host,rds_port,rds_database,rds_user,rds_pass,table_name,access_key,secret_key)
   
Sample_query_from_RDS_table(rds_host, rds_port, rds_user, rds_pass, rds_database, table_name)