from boto3 import client

from general_utils.aws_wrappers.utils import Aws


class AwsDynamoDb(Aws):
    def __init__(self, table_name: str, **kwargs):
        """
        Args:
            table_name (str): Name of the table to instantiate.
            **kwargs: Arguments to pass to the dynamo db client.
        """
        super().__init__(client, "dynamodb", **kwargs)

        self.class_private_vars()[f"_table_name"] = table_name

        self.import_table_fields()

    def import_table_fields(self):
        table_response = self.client.describe_table(TableName=self.table_name)

        for key, value in table_response.items():
            self.class_private_vars()[f"_{key}"] = value

        try:
            self.class_private_vars()[f"_stream_arn"] = self.Table["LatestStreamArn"]
        except KeyError:
            print(
                "Stream needs to be enabled in the Exports and Streams section of the table."
            )
