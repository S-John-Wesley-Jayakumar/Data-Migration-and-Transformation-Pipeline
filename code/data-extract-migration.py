import boto3
import os
import requests
import zipfile

def download_and_extract_zip(url, destination_dir, file_name):
    # Create the destination directory if it doesn't exist
    if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)

    # Send a GET request with headers to download the ZIP file
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"  # Replace with your actual User-Agent string
    }
    response = requests.get(url, headers=headers)

    print(response)

    # Check if the request was successful
    if response.status_code == 200:
        # Save the ZIP file
        zip_path = os.path.join(destination_dir, file_name)
        with open(zip_path, "wb") as zip_file:
            zip_file.write(response.content)

        # Extract the contents of the ZIP file
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(destination_dir)

        # Remove the ZIP file
        os.remove(zip_path)

        print("Extraction completed.")
    else:
        print("Failed to download the ZIP file.")




def upload_files_to_s3(local_folder, bucket_name, s3_folder, access_key, secret_key):
    # Create a Boto3 S3 client with the provided access key and secret key
    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    # Iterate through the files in the local folder
    for root, dirs, files in os.walk(local_folder):
        print("Files upload started...")

        for file_name in files:
            local_file_path = os.path.join(root, file_name)

            # Construct the S3 object key by combining the folder path and file name
            s3_key = os.path.join(s3_folder, os.path.relpath(local_file_path, local_folder))

            print(f"Uploading file: {local_file_path} to S3 key: {s3_key}")

            # Upload the file to S3
            s3.upload_file(local_file_path, bucket_name, s3_key)

            print(f"File uploaded: {local_file_path}")

        print("Files uploaded")




# Example usage
url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
destination_dir = r"...enter your path...."
file_name = "submission.zip"

download_and_extract_zip(url, destination_dir, file_name)


# Specify the local folder path
local_folder =r"...enter your path...."

# Specify your S3 bucket name and folder path
s3_bucket_name = 'xxxxxxxxxxxx'
s3_folder = 'xxxxxxxxx'

# Specify your AWS access key and secret key
access_key = '.........................'
secret_key = '............'

# Call the function to upload files to S3
upload_files_to_s3(local_folder, s3_bucket_name, s3_folder, access_key, secret_key)
