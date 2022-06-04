from abc import ABC
from urllib.parse import unquote_plus
from typing import Tuple, Callable

CLIENTS_IMPLEMENTED = [
    "secretsmanager",
    "dynamodbstreams",
    "kms",
    "dynamodb",
]

SKIP_KEY_ARGS = [
    "aws_boto3",
    "secret_arn",
    "bucket_name",
    "object_name",
    "KeyId",
    "table_name",
    "stream_arn",
]
RESOURCES_IMPLEMENTED = ["s3", "cloudwatch"]


def extract_bucket_file_name_from_event(record: dict) -> Tuple[str, str]:
    """Method to extract Bucket and Key names from a PUT event

    Args:
        record (dict)

    Returns:
        Tuple[str, str]
    """
    key_name = unquote_plus(record["s3"]["object"]["key"], encoding="utf-8")
    bucket_name = unquote_plus(record["s3"]["bucket"]["name"], encoding="utf-8")

    return key_name, bucket_name


class Aws(ABC):
    def __init__(self, aws_boto3: Callable, *args, **kwargs) -> None:

        for key in kwargs:
            if key in SKIP_KEY_ARGS:
                del kwargs[key]

        private_components = {f"_{k}": v for k, v in kwargs.items()}
        self.__dict__.update(private_components)
        self.__dict__["_args"] = args

        if aws_boto3.__name__ == "resource":
            self.__dict__["_resource"] = aws_boto3(
                *self._return_args(), **self._return_kargs()
            )
            if self.resource.meta.service_name not in RESOURCES_IMPLEMENTED:
                raise AttributeError(
                    "Wrapper for this resource has not been implemented"
                )

        elif aws_boto3.__name__ == "client":
            self.__dict__["_client"] = aws_boto3(
                *self._return_args(), **self._return_kargs()
            )

            if self.client.meta.service_model.service_name not in CLIENTS_IMPLEMENTED:
                raise AttributeError("Wrapper for this client has not been implemented")

    def __getattr__(self, name):
        private_name = f"_{name}"
        try:
            return self.__dict__[private_name]
        except KeyError:
            raise AttributeError(f"{self!r} object has no attribute {name!r}")

    def __setattr__(self, name, value):
        raise AttributeError(f"Cant set attribute {name!r}")

    def __delattr__(self, name):
        raise AttributeError(f"Cannot delete attribute {name!r}")

    def __repr__(self):

        return "{}({})".format(
            type(self).__name__,
            ", ".join(
                "{k}={v}".format(
                    k=k[1:],
                    v=v,
                )
                for k, v in self.__dict__.items()
            ),
        )

    def _return_args(self):

        return self.__dict__["_args"]

    def _return_kargs(self):

        return {f"{k[1:]}": v for k, v in self.__dict__.items() if k not in ["_args"]}

    def class_private_vars(self) -> dict:
        """Method to return a dictionary with all the args and kwargs.
            Method allows the modification of the attribute values.

        Returns:
            dict
        """
        return vars(self)
