import codecs
from json import load, dumps
from dataclasses import dataclass, field
from contextlib import contextmanager
from csv import reader, writer
from datetime import datetime
from io import StringIO, TextIOWrapper
from typing import Union, Callable

from boto3 import resource

from botocore.response import StreamingBody
from general_utils.aws_wrappers.utils import Aws


class S3Bucket(Aws):
    def __init__(self, bucket_name: str, **kwargs) -> None:
        """
        Args:
            bucket_name (str): Name of the desired bucket to instantiate.
            **kwargs: Arguments to pass to the S3 resource.
        """
        super().__init__(resource, "s3", **kwargs)

        self.class_private_vars()["_bucket_name"] = bucket_name
        self.class_private_vars()["_Bucket"] = self.resource.Bucket(self.bucket_name)

    def list_objects(self) -> list:
        """List all the objects in the bucket that exists in the bucket.

        Returns:
            list
        """
        return list(self.Bucket.objects.all())

    def list_objects_versions(self) -> list:
        """Lists all the objects including versioning objects that are in the bucket.

        Returns:
            list
        """
        return list(self.Bucket.object_versions.all())

    def put(self, Body: Union[str, bytes], Key: str, **kwargs):
        self.Bucket.put_object(Body=Body, Key=Key, **kwargs)

    def delete_objects(
        self,
        keys_to_delete: list,
        delete_all_objects_in_bucket: bool = False,
        delete_all_versions=False,
    ) -> None:
        """Method to delete objects from the bucket.

        Args:
            keys_to_delete (list): Name of the objects to delete.
            delete_all_objects_in_bucket (bool, optional): Deletes all objects and versions from the bucket. Defaults to False.
            delete_all_versions (bool, optional): Deletes the objects and the versions of the objects. Defaults to False.
        """
        if delete_all_versions:

            def object_filter_map(item):
                if item.object_key in keys_to_delete:
                    return {"Key": item.object_key, "VersionId": item.id}

            keys = list(
                filter(None, map(object_filter_map, self.list_objects_versions()))
            )

        elif delete_all_objects_in_bucket:
            keys = list(
                filter(
                    None,
                    map(
                        lambda x: {"Key": x.object_key, "VersionId": x.id},
                        self.list_objects_versions(),
                    ),
                )
            )
        else:
            keys = [{"Key": key} for key in keys_to_delete]

        params = {
            "Objects": keys,
            "Quiet": True,
        }

        self.Bucket.delete_objects(Delete=params)

    def copy_object(
        self,
        target_key_name: str,
        src_bucket: str,
        src_key: str,
        target_bucket: str = None,
        **kwargs,
    ) -> None:
        """Method to copy objects from one bucket to another or the same bucket.

        Args:
            target_key_name (str): Name of the new object.
            src_bucket (str): Bucket where the file is stored.
            src_key (str): Name of the object to copy.
            target_bucket (str, optional): Bucket to store the object. Defaults to None.
        """
        copy_source = {"Bucket": src_bucket, "Key": src_key}

        if target_bucket is not None:
            bucket = self.resource.Bucket(target_bucket)
        else:
            bucket = self.Bucket
        bucket.copy(copy_source, target_key_name, **kwargs)

    def upload_file(self, file_path: str, key_name: str, **kwargs) -> None:
        self.Bucket.upload_file(file_path, key_name, **kwargs)

    def download_file(self, file_path: str, key_name: str, **kwargs) -> None:
        self.Bucket.download_file(file_path, key_name, **kwargs)


class S3Object(Aws):
    def __init__(self, bucket_name: str, object_name: str, **kwargs) -> None:
        """
        Args:
            bucket_name (str): Name of the bucket where the object is stored.
            object_name (str): Name of the object to instantiate.
            **kwargs: Arguments to pass to the S3 resource.
        """
        super().__init__(resource, "s3", **kwargs)

        self.class_private_vars()["_bucket_name"] = bucket_name
        self.class_private_vars()["_key"] = object_name
        self.class_private_vars()["_Object"] = self.resource.Object(
            bucket_name=self.bucket_name, key=self.key
        )
        self.class_private_vars()["_S3_object_response"] = None

    def _delete(self, **kwargs) -> None:
        self.Object.delete(**kwargs)

    def get_object(self, **kwargs) -> dict:
        """Method to get S3 objects.

            If used with S3ReadLine, use Range=f"bytes={initial_offset}-"

        Returns:
            dict
        """
        self.class_private_vars()["_S3_object_response"] = self.Object.get(**kwargs)
        return self.S3_object_response

    def soft_delete_object(self) -> None:
        """Method to soft delete the object."""
        self._delete()

    def delete_all_versions(self, confirm=False) -> None:
        """Method to delete ALL the versions of this file.

        Args:
            confirm (bool, optional): Argument must be set to True in order to delete all versions of the object. Defaults to False.
        """
        if confirm:
            [
                self._delete(VersionId=item.id)
                for item in self.Object.Bucket().object_versions.filter(Prefix=self.key)
            ]

    def delete_object(self) -> None:
        """Method to permanently delete the current version id of the file."""
        self._delete(VersionId=self.Object.version_id)

    def get_last_modified_date(
        self, return_iso_format: bool = False
    ) -> Union[str, datetime]:

        date_time = self.Object.last_modified

        if return_iso_format:
            return date_time.isoformat()

        return date_time

    def get_metadata(self) -> dict:
        """Method to get S3 bucket metadata.

        Raises:
            ValueError: If the data from the object has not been requested from S3.

        Returns:
            dict
        """
        if self.S3_object_response is None:
            raise ValueError("Need to get S3 object first")

        return self.S3_object_response["Metadata"]

    def get_streaming_body(self) -> StreamingBody:
        """Method to get the body of the S3 object.

        Returns:
            StreamingBody
        """
        if self.S3_object_response is None:
            self.get_object()

        return self.S3_object_response["Body"]


@dataclass
class S3FileAppendData:
    S3Object: Union[S3Object, TextIOWrapper]
    rows_to_append: list = field(init=False)
    writer: Callable = field(init=False)
    new_file: bool = field(init=False)
    is_csv: bool = field(init=False)
    is_json: bool = field(init=False)
    csvio: Union[StringIO, TextIOWrapper] = field(init=False)

    def __post_init__(self):

        self.is_csv = False
        self.is_json = False

        if isinstance(self.S3Object, TextIOWrapper):

            if self.S3Object.key.endswith(".csv"):

                existing_rows = list(reader(self.S3Object))
                self.S3Object.close()

                self.is_csv = True

            if self.S3Object.key.endswith(".json"):
                existing_rows = load(self.S3Object)
                self.is_json = True

            if existing_rows:

                self.new_file = False
            else:
                self.new_file = True

        elif isinstance(self.S3Object, S3Object):

            if self.S3Object.key.endswith(".csv"):

                self.is_csv = True

            if self.S3Object.key.endswith(".json"):
                self.is_json = True
            try:

                if self.is_csv:
                    existing_rows = list(
                        reader(
                            codecs.getreader("utf-8")(
                                self.S3Object.get_object()["Body"]
                            )
                        )
                    )

                if self.is_json:
                    existing_rows = load(self.S3Object.get_object()["Body"])

                self.new_file = False

            except Exception:
                self.new_file = True
                existing_rows = []
        else:
            raise TypeError(f"Instance {type(self.S3Object)} not implemented.")

        self.rows_to_append = []

        if not any([self.is_csv, self.is_json]):
            raise TypeError("File type is not implemented")

        if self.is_csv:
            self.csvio = StringIO()
            self.writer = writer(self.csvio)

            if not self.new_file:
                self.writer.writerows(existing_rows)

        if self.is_json:
            self.rows_to_append.extend(existing_rows)

    def write_content_to_s3(self) -> None:
        """Internal Method to write new data to the S3 file."""
        self.S3Object.Object.put(Body=self.return_data_to_s3())

    def add_rows(self, new_rows: list, append=False) -> None:
        """Method to add rows to the object. New rows can be a list of lists/dicts if append is False.

        Args:
            new_rows (list): New rows to add to the file
            append (bool, optional). Defaults to False.
        """
        if append:
            self.rows_to_append.append(new_rows)
        else:
            self.rows_to_append.extend(new_rows)

    def add_headers(self, headers: list) -> None:
        """Method to prepend headers to the existing rows. Only works if the file is New.

        Args:
            headers (list):
        """
        if self.new_file:  # Avoids inserting rows in files that already exist
            self.existing_rows.insert(0, headers)

    def write_rows(self) -> None:
        """Method to write the rows stored in the object to the writer attribute."""
        if self.is_csv:
            self.writer.writerows(self.rows_to_append)

    def return_data_to_s3(self) -> str:
        """Returns the value stored in the StringIO object.

        Returns:
            str
        """
        if self.is_csv:
            return self.csvio.getvalue()

        if self.is_json:
            return dumps(self.rows_to_append)

    def close_file(self) -> None:
        if self.is_csv:
            self.csvio.close()


@contextmanager
def S3FileAppendContextManager(
    S3Object: S3Object, propagate_errors: bool
) -> S3FileAppendData:
    """Context manager to append data to CSV files stored in S3.
    Use the context manager object.add_rows(new_rows: list, append=False) to add new rows.
    If headers want to be added at any point in a new file, use object.add_headers(headers:list)

    Args:
        S3Object (S3Object)
        propagate_errors (bool): Propages error found within the context manager.

    Raises:
        Exception

    Yields:
        S3FileAppendData
    """

    AwsObject = S3FileAppendData(S3Object=S3Object)

    try:
        yield AwsObject

    except Exception as e:
        AwsObject.close_file()

        if propagate_errors:
            raise e

    else:
        AwsObject.write_rows()
        AwsObject.write_content_to_s3()

    finally:

        AwsObject.close_file()
