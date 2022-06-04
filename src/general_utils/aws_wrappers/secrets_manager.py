from json import loads

from boto3 import client

from general_utils.aws_wrappers.utils import Aws


class AwsSecretManager(Aws):
    """Class to manage AWS Secret Manager Operations."""

    def __init__(self, secret_arn: str, **kwargs):
        """
        Args:
            secret_arn (str): Secret name or ARN of the secret.
            **kwargs: Arguments to pass to the secrets manager client.
        """
        super().__init__(client, "secretsmanager", **kwargs)

        self._get_secret(secret_arn=secret_arn)

    def _get_secret(self, secret_arn: str) -> dict:
        secret_data = self.client.get_secret_value(SecretId=secret_arn)

        for key, value in secret_data.items():

            self.class_private_vars()[f"_{key}"] = value
        return secret_data

    def get_secret_keys(self) -> list:
        """Method to get the keys in the secret.

        Returns:
            list
        """
        return loads(self.SecretString).keys()

    def get_secret_token(self, secret_key: str) -> str:
        """Method to extract the API token from the secret.

        Args:
            secret_key (str): Secret Key

        Raises:
            ClientError

        Returns:
            str
        """

        return loads(self.SecretString)[secret_key]
