import timeit
from dataclasses import dataclass, field

from boto3 import client

from general_utils.aws_wrappers.utils import Aws


class AwsChangeStream(Aws):
    """
    Notes:
    Each shard in the stream has a SequenceNumberRange associated with it.
    If the SequenceNumberRange has a StartingSequenceNumber but no EndingSequenceNumber,then the shard is still open (able to receive more stream records).
    If both StartingSequenceNumber and EndingSequenceNumber are present, then that shard is closed and can no longer receive more data.

    Records are deleted after 24 hours, If all the records are deleted then the shard is deleted.

    Shards can close at any time. In order to get the current state of the shards, you can use client.describe_stream()/self.get_shard_object_array() method but it can only be called up to  10 times per second.
    """

    def __init__(self, stream_arn: str, **kwargs) -> None:
        """
        Args:
            stream_arn (str): Arn of the desired change stream.
            **kwargs: Arguments to pass to the dynamo streams client.
        """

        super().__init__(client, "dynamodbstreams", **kwargs)

        self.class_private_vars()[f"_stream_arn"] = stream_arn

    def get_shard_object_array(self):
        stream_response = self.client.describe_stream(StreamArn=self.stream_arn)
        for key, value in stream_response.items():
            self.class_private_vars()[f"_{key}"] = value

        return self.StreamDescription["Shards"]

    def create_shard_iterator(
        self, iterator_type: str, shard_id: str, SequenceNumber: int = None
    ):

        if iterator_type not in [
            "TRIM_HORIZON",
            "AT_SEQUENCE_NUMBER",
            "AFTER_SEQUENCE_NUMBER",
            "LATEST",
        ]:
            raise TypeError("Iterator type is not valid")

        iterator_args = {
            "StreamArn": self.stream_arn,
            "ShardId": shard_id,
            "ShardIteratorType": iterator_type,
        }

        if iterator_type in ["AFTER_SEQUENCE_NUMBER", "AT_SEQUENCE_NUMBER"]:

            if SequenceNumber is None:
                raise ValueError("This iterator type needs a SequenceNumber")

            iterator_args["SequenceNumber"] = SequenceNumber

        return self.client.get_shard_iterator(**iterator_args)

    def get_data_from_iterator(self, shard_iterator):

        return self.client.get_records(ShardIterator=shard_iterator, Limit=1000)


@dataclass
class StreamOrchestrator:
    """Class to Orchestrate the record extraction from mulitple shards.

    sequence_number is the starting point. If the sequence number is not found in the shards,
    All the records are processed using trim horizon.

    if the sequence_number is found in one of the shards, records in the shard are processed after the sequence_number.
    Subsequent shards are processed using trim hirozon.

    Once all the closed shards are processed, the code loops in the open shard until is closed. Once is closed, the function returns
    message.

    """

    sequence_number: int
    AwsChangeStream: AwsChangeStream
    shard_array: list = field(init=False)
    open_shards: list = field(init=False)
    closed_shards: list = field(init=False)
    sequence_shard_info: dict = field(init=False)
    is_sequence_shard_open: bool = field(init=False)
    next_shard_iterator: iter = field(init=False)

    def __post_init__(self):
        self.sequence_number = str(self.sequence_number)

        self.shard_array = self.AwsChangeStream.get_shard_object_array()
        self.open_shards = self.get_open_shards()
        self.closed_shards = self.get_closed_shards()
        self.sequence_shard_info = self.find_shard_info(
            sequence_number=self.sequence_number
        )

        # If sequence number in shards, this will not be none
        if self.sequence_shard_info is not None:
            self.is_sequence_shard_open = StreamOrchestrator.is_shard_open(
                self.sequence_shard_info
            )
        else:
            self.is_sequence_shard_open = False

        self.next_shard_iterator = None

    @staticmethod
    def is_shard_open(shard_info: dict) -> bool:
        return "EndingSequenceNumber" not in shard_info["SequenceNumberRange"].keys()

    def get_closed_shards(self) -> list:
        return sorted(
            list(
                filter(
                    lambda x: "EndingSequenceNumber" in x["SequenceNumberRange"].keys(),
                    self.shard_array,
                )
            ),
            key=lambda x: x["SequenceNumberRange"]["StartingSequenceNumber"],
        )

    def get_open_shards(self) -> list:
        return list(filter(StreamOrchestrator.is_shard_open, self.shard_array))

    def find_shard_info(self, sequence_number: str) -> dict:

        try:
            filtered_array = list(
                filter(
                    lambda x: float(x["SequenceNumberRange"]["StartingSequenceNumber"])
                    <= float(sequence_number)
                    <= float(x["SequenceNumberRange"]["EndingSequenceNumber"]),
                    self.shard_array,
                )
            )
        except KeyError:
            filtered_array = self.get_open_shards()

        if filtered_array:
            return filtered_array[0]

    @staticmethod
    def filter_shard_array(shard_id: str, shard_array: list) -> dict:
        return list(filter(lambda x: x["ShardId"] == shard_id, shard_array))[0]

    def get_data_from_iterator(self, shard_iterator) -> dict:

        return self.AwsChangeStream.get_data_from_iterator(
            shard_iterator=shard_iterator
        )

    def get_closed_children_shards(self, starting_shard: dict) -> list:

        filtered_shards = []

        while True:

            child_shard = list(
                filter(
                    lambda x: x["ParentShardId"] == starting_shard["ShardId"],
                    self.shard_array,
                )
            )

            if child_shard:

                child_shard = child_shard[0]

                if "EndingSequenceNumber" in child_shard["SequenceNumberRange"].keys():
                    filtered_shards.append(child_shard)
                    starting_shard = child_shard

                else:
                    break
            else:
                break

        return filtered_shards

    def get_data_from_shard(self, *args, **kargs) -> dict:

        if self.next_shard_iterator is None:

            iterator = self.AwsChangeStream.create_shard_iterator(*args, **kargs)[
                "ShardIterator"
            ]

        else:
            iterator = self.next_shard_iterator

        iterator_data = self.get_data_from_iterator(shard_iterator=iterator)

        if "NextShardIterator" not in iterator_data.keys():  # and not open_shard:

            return None

        self.next_shard_iterator = iterator_data["NextShardIterator"]

        return iterator_data

    def get_stream_data(
        self, open_shard: bool, limit_records: int = None, *args, **kargs
    ) -> None:

        self.next_shard_iterator = None

        if open_shard:

            if args:

                shard_id = [arg for arg in args if "shardId" in arg][0]

            if kargs:

                for key, value in kargs.items():
                    if "shardId" in value:
                        shard_id = kargs[key]

            start = timeit.default_timer()

        counter = 0

        while True:

            data = self.get_data_from_shard(*args, **kargs)

            if not open_shard:
                if data is None:
                    break

            if data["Records"]:
                print(
                    data["Records"]
                )  # modify to add Kafka/RabbitMQ or list to append data.

            if open_shard:

                stop = timeit.default_timer()

                if (stop - start) > 1:  # Can only request array 10 times per sec max.

                    shard_dict = StreamOrchestrator.filter_shard_array(
                        shard_id=shard_id,
                        shard_array=self.AwsChangeStream.get_shard_object_array(),
                    )

                    if not StreamOrchestrator.is_shard_open(shard_dict):
                        return {"shard_id": shard_id, "closed": True}

                    start = timeit.default_timer()

            if limit_records:

                if counter == limit_records - 1:
                    break

                counter += 1

    def trim_horizon_closed_shards(self, closed_shards: list = None) -> None:

        if closed_shards is None:
            closed_shards_array = self.closed_shards

        else:
            closed_shards_array = closed_shards

        closed_shards = list(
            map(lambda x: x["ShardId"], closed_shards_array)
        )  # Trims horizon all closed Shards

        open_shard_bool = len(closed_shards) * [False]
        limit_records_none = len(closed_shards) * [None]
        iterator_type = len(closed_shards) * ["TRIM_HORIZON"]

        list(
            map(
                self.get_stream_data,
                open_shard_bool,
                limit_records_none,
                iterator_type,
                closed_shards,
            )
        )

    def start_sequence_closed_shards(self) -> None:

        if self.sequence_shard_info is not None:

            self.get_stream_data(
                open_shard=False,
                iterator_type="AFTER_SEQUENCE_NUMBER",
                shard_id=self.sequence_shard_info["ShardId"],
                SequenceNumber=self.sequence_number,
            )

            child_shards = self.get_closed_children_shards(
                starting_shard=self.sequence_shard_info
            )

            if child_shards:

                self.trim_horizon_closed_shards(closed_shards=child_shards)

    def trim_horizon_open_shards(self, limit_records: int = None) -> None:

        self.get_stream_data(
            open_shard=True,
            iterator_type="TRIM_HORIZON",
            shard_id=self.open_shards[0]["ShardId"],
            limit_records=limit_records,
        )

    def start_sequence_open_shards(self, limit_records: int = None) -> None:

        self.get_stream_data(
            open_shard=True,
            iterator_type="AFTER_SEQUENCE_NUMBER",
            shard_id=self.open_shards[0]["ShardId"],
            SequenceNumber=self.sequence_number,
            limit_records=limit_records,
        )

    def process_records(self, limit_records: int = None):

        if self.sequence_shard_info is None:

            print("sequence not found, trimming all horizons")

            self.trim_horizon_closed_shards()
            open_shard_status = self.trim_horizon_open_shards(
                limit_records=limit_records
            )

        else:

            if self.is_sequence_shard_open:
                print("found sequence in open shard")
                open_shard_status = self.start_sequence_open_shards(
                    limit_records=limit_records
                )

            else:
                print("found sequence in closed shard")
                self.start_sequence_closed_shards()
                open_shard_status = self.trim_horizon_open_shards(
                    limit_records=limit_records
                )

        return open_shard_status
