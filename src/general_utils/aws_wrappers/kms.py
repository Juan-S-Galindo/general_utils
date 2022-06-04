from ast import arg
import base64

from boto3 import client

from general_utils.aws_wrappers.utils import Aws


class AwsKms(Aws):
    """Class to handle KMS encryption and decrytion"""

    def __init__(self, KeyId: str, **kwargs) -> None:
        """
        Args:
            KeyId (str): AWS KMS key id. Format: alias/key name -> Test example: alias/Sebastian/hl7
            **kwargs: Arguments to pass to the kms client.
        """

        super().__init__(client, "kms", **kwargs)

        self.class_private_vars()[f"_KeyId"] = (
            KeyId if "alias" == KeyId[0:5] else "alias/" + KeyId
        )

    def _send_encrypt_request(self, secret: str) -> dict:
        return self.client.encrypt(
            KeyId=self.KeyId,
            Plaintext=bytes(secret, encoding="utf8"),
        )

    def _send_decrypt_request(self, secret: str) -> dict:
        return self.client.decrypt(
            KeyId=self.KeyId, CiphertextBlob=bytes(base64.b64decode(secret))
        )

    def encrypt(self, secret: str) -> str:
        """Method to encrypt words using AWS KMS

        Args:
            secret (str)

        Returns:
            str
        """

        ciphertext = self._send_encrypt_request(secret=secret)

        for key, value in ciphertext.items():

            if key != "CiphertextBlob":
                self.class_private_vars()[f"_{key}"] = value

        return base64.b64encode(ciphertext["CiphertextBlob"]).decode("utf-8")

    def bulk_encrypt(self, *args) -> list:
        """Method to encrypt multiple items using KMS.

        Returns:
            list
        """

        def _bulk_request_func(secret):
            return base64.b64encode(
                self._send_encrypt_request(secret)["CiphertextBlob"]
            ).decode("utf-8")

        return list(map(_bulk_request_func, args))

    def decrypt(self, secret: str) -> str:
        """Method to decrypt secrets using AWS KMS.

        Args:
            secret (str)

        Returns:
            str
        """
        plaintext = self._send_decrypt_request(secret=secret)

        for key, value in plaintext.items():

            if key not in ["KeyId", "Plaintext"]:
                self.class_private_vars()[f"_{key}"] = value

        return plaintext["Plaintext"].decode("UTF-8")

    def bulk_decrypt(self, *args, **kwargs) -> list:
        """Method to decrypt mutiple items using KMS.

        Returns:
            list
        """
        for k in kwargs.keys():

            if k.lower() == "logger":
                LOGGER = kwargs[k]

            else:
                LOGGER = None

        def _bulk_request_func(secret):
            try:
                if isinstance(secret, list) or isinstance(secret, set):
                    return [
                        self._send_decrypt_request(secret=value)["Plaintext"].decode(
                            "UTF-8"
                        )
                        for value in secret
                    ]

                elif isinstance(secret, str):
                    return self._send_decrypt_request(secret=secret)[
                        "Plaintext"
                    ].decode("UTF-8")

                else:
                    raise ValueError(
                        f"Bulk decrypt cannot handle instances: {type(secret)}"
                    )
            except Exception as e:
                if LOGGER is not None:
                    LOGGER.error(
                        f"Lambda with Bulk Decrypt: {str(e)} - value: {secret}"
                    )
                return secret

        return list(map(_bulk_request_func, args))
