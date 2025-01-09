import json
import os
import time

import boto3
from dotenv import load_dotenv

load_dotenv()


class S3:
    def __init__(self):
        """initialize S3 client and other necessary variables"""
        self.bucket_name = os.getenv('AWS_BUCKET_NAME')
        self.region_name = os.getenv('AWS_DEFAULT_REGION')
        self.s3 = boto3.client(
            's3',
            region_name=self.region_name,
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        )

    def does_bucket_exist(self) -> bool:
        """Check if bucket exists, if not create one"""
        try:
            self.s3.head_bucket(Bucket=self.bucket_name)
            return True
        except Exception as e:
            print(f'Bucket does not exist: {e}')
            return False

    def create_bucket(self):
        """Create an S3 bucket"""
        try:
            # check if bucket exists -> if it does do nothing
            if self.does_bucket_exist():
                return

            # create bucket
            self.s3.create_bucket(
                Bucket=self.bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': self.region_name
                }
            )
            print("S3 bucket created successfully.")
        except Exception as e:
            print(f'Error while creating bucket: {e}')

    def save_json_file(self, weather_data) -> bool:
        """Save JSON file to S3 bucket"""
        try:
            # check if bucket exists -> if it does do nothing
            if not self.does_bucket_exist():
                return False

            if not weather_data:
                return False

            ts = time.time()
            city = weather_data['name']
            file_name = f"{city}-{ts}.json"

            print(f'Saving {file_name} to S3...')

            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=file_name,
                Body=json.dumps(weather_data),
                ContentType='application/json'
            )

            print(f'File: {file_name} uploaded successfully to S3!!!')
            return True
        except Exception as e:
            print(f'Error while saving to S3: {e}')
            return False
