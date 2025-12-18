from abc import ABC, abstractmethod
import argparse
import hashlib
import json
import logging
import os
from typing import Union, Dict, List, Any

import boto3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Storage(ABC):
    @abstractmethod
    def save_file(self, data: Union[str, Dict[str, Any], List[Dict[str, Any]]], filepath: str) -> None:
        pass

    @abstractmethod
    def load_file(self, filepath: str) -> Union[str, Dict[str, Any]]:
        pass


class LocalStorage(Storage):
    def __init__(self, base_path: str):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)

    def save_file(self, data: Union[str, Dict[str, Any], List[Dict[str, Any]]], filepath: str, check_md5=False) -> None:
        full_path = os.path.join(self.base_path, filepath)

        json_data = json.dumps(data).encode('utf-8')

        if check_md5:
            data_md5 = hashlib.md5(json_data).hexdigest()
            md5_hash = None
            with open(full_path, "rb") as file:
                file_contents = file.read()
                md5_hash = hashlib.md5(file_contents).hexdigest()
            if md5_hash == data_md5:
                logger.info(f"File {filepath} already exists in local path '{self.base_path}' with same md5")
                return

        with open(full_path, 'w') as f:
            json.dump(data, f)
        logger.info(f"Saved file to {full_path}")

    def get_file_md5(self, filepath: str):
        full_path = os.path.join(self.base_path, filepath)
        if not os.path.exists(full_path):
            return None
        with open(full_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()

    def load_file(self, filepath: str) -> Union[str, Dict[str, Any]]:
        full_path = os.path.join(self.base_path, filepath)
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"File not found: {full_path}")

        with open(full_path, 'r') as f:
            return json.load(f)


class S3Storage(Storage):
    def __init__(self, bucket_name: str):
        # Note: boto3 package would need to be installed for S3 functionality
        self.bucket_name = bucket_name
        self.s3 = boto3.resource('s3')
        self.bucket = self.s3.Bucket(self.bucket_name)
        self.md5s = self.get_all_object_md5()

    def save_file(self, data: Union[str, Dict[str, Any], List[Dict[str, Any]]], filepath: str, check_md5=False) -> None:
        json_data = json.dumps(data).encode('utf-8')

        if check_md5:
            data_md5 = hashlib.md5(json_data).hexdigest()
            s3_md5 = self.md5s.get(filepath)
            if s3_md5 is not None:
                if s3_md5 == data_md5:
                    logger.info(f"File {filepath} already exists in S3 bucket {self.bucket_name} with same md5")
                    return

        self.bucket.put_object(
            Key=filepath,
            Body=json_data,
            ContentType='application/json',
        )
        logger.info(f"Saved file to S3 bucket {self.bucket_name} at {filepath}")

    def get_object_md5(self, object_key: str) -> str:
        response = self.s3.head_object(Bucket=self.bucket_name, Key=object_key)
        return response.get("ETag", "").strip('"')

    def get_all_object_md5(self) -> dict:
        objects = {}
        for obj in self.bucket.objects.all():
            objects[obj.key] = obj.e_tag.strip('"')
        return objects

    def load_file(self, object_key: str) -> Union[str, Dict[str, Any]]:
        obj = self.s3.Object(self.bucket_name, object_key)
        file_content = obj.get()['Body'].read().decode('utf-8')
        json_data = json.loads(file_content)
        return json_data


def copy_s3_to_local(bucket_name: str, base_path: str):
    s3_storage = S3Storage(bucket_name=bucket_name)
    local_storage = LocalStorage(base_path=base_path)

    s3_md5s = s3_storage.get_all_object_md5()
    status = {
        'added_new_files': [],
        'updated_files': [],
        'existing_files_with_same_md5': [],
    }

    for key, s3_md5 in s3_md5s.items():
        local_md5 = local_storage.get_file_md5(key)
        if local_md5 is None:
            status['added_new_files'].append(key)
            logger.info(f"Copying *new* {key} from S3 to local storage")
            local_storage.save_file(s3_storage.load_file(key), key)
        if s3_md5 != local_md5:
            status['updated_files'].append(key)
            logger.info(f"Overwriting {key} from S3 to local storage")
            local_storage.save_file(s3_storage.load_file(key), key)
        else:
            status['existing_files_with_same_md5'].append(key)
            logger.info(f"{key} already exists in local storage with same md5")

    logger.info("Summary:")
    for k, v in status.items():
        logger.info(f"{len(v)} {k}")


def main():
    parser = argparse.ArgumentParser(description='Store/Retrieve TCGPlayer order information')
    parser.add_argument('--bucket-name', required=True, help='S3 bucket name')
    parser.add_argument('--base-path', required=True, help='Local storage path')
    parser.add_argument('--action', required=True, choices=['copy-s3-to-local', 'copy-local-to-s3'], help='Action to perform')
    args = parser.parse_args()

    if args.action == 'copy-s3-to-local':
        copy_s3_to_local(args.bucket_name, args.base_path)


if __name__ == "__main__":
    main()
