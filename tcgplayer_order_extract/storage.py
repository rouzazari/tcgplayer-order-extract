from abc import ABC, abstractmethod
import json
import logging
import os
from typing import Union, Dict, List, Any

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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

    def save_file(self, data: Union[str, Dict[str, Any], List[Dict[str, Any]]], filepath: str) -> None:
        full_path = os.path.join(self.base_path, filepath)

        with open(full_path, 'w') as f:
            json.dump(data, f)
        logger.info(f"Saved file to {full_path}")

    def load_file(self, filepath: str) -> Union[str, Dict[str, Any]]:
        raise NotImplementedError("LocalStorage does not support loading files.")


class S3Storage(Storage):
    def __init__(self, bucket_name: str):
        # Note: boto3 package would need to be installed for S3 functionality
        self.bucket_name = bucket_name
        self.s3 = boto3.resource('s3')
        self.bucket = self.s3.Bucket(self.bucket_name)

    def save_file(self, data: Union[str, Dict[str, Any], List[Dict[str, Any]]], filepath: str) -> None:
        json_string = json.dumps(data)
        body = json_string.encode('utf-8')
        self.bucket.put_object(
            Key=filepath,
            Body=body,
            ContentType='application/json',
        )
        logger.info(f"Saved file to S3 bucket {self.bucket_name} at {filepath}")

    def load_file(self, object_key: str) -> Union[str, Dict[str, Any]]:
        raise NotImplementedError("S3Storage does not support loading files.")
