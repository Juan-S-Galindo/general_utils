from botocore.exceptions import ClientError

from boto3 import resource

from general_utils.aws_wrappers.utils import Aws
from aws_lambda_powertools import Logger

from typing import Iterable, Any

from datetime import datetime


class CloudWatchResource(Aws):
    def __init__(self, aws_lambda_powertools_logger: Logger, **kwargs):
        """
        Args:
            aws_lambda_powertools_logger (Logger)
        """
        super().__init__(resource, "cloudwatch", **kwargs)

        self.class_private_vars()["_logger"] = aws_lambda_powertools_logger

    def list_metrics(self, namespace: str, name: str, recent: bool = False) -> Iterable:
        """Gets the metrics within a namespace that have the specified name.
        If the metric has no dimensions, a single metric is returned.
        Otherwise, metrics for all dimensions are returned.

        Args:
            namespace (str): The namespace of the metric.
            name (str): The name of the metric.
            recent (bool, optional): When True, only metrics that have been active in the last
                       three hours are returned. Defaults to False.

        Returns:
            Iterable:  An iterator that yields the retrieved metrics.
        """
        try:
            kwargs = {"Namespace": namespace, "MetricName": name}
            if recent:
                kwargs["RecentlyActive"] = "PT3H"  # List past 3 hours only
            metric_iter = self.resource.metrics.filter(**kwargs)
            self.logger.debug("Got metrics for %s.%s.", namespace, name)
        except ClientError:
            self.logger.exception("Couldn't get metrics for %s.%s.", namespace, name)
            raise
        else:
            return metric_iter

    def put_metric_data(self, namespace: str, name: str, value: Any, unit: str) -> None:
        """Sends a single data value to CloudWatch for a metric. This metric is given
        a timestamp of the current UTC time.

        Args:
            namespace (str): The namespace of the metric.
            name (str): The name of the metric.
            value (Any): The value of the metric.
            unit (str): The unit of the metric.
        """
        try:
            metric = self.resource.Metric(namespace, name)
            metric.put_data(
                Namespace=namespace,
                MetricData=[{"MetricName": name, "Value": value, "Unit": unit}],
            )
            self.logger.debug("Put data for metric %s.%s", namespace, name)
        except ClientError:
            self.logger.exception("Couldn't put data for metric %s.%s", namespace, name)
            raise

    def put_metric_data_set(
        self, namespace: str, name: str, timestamp: datetime, unit: str, data_set: dict
    ) -> None:
        """Sends a set of data to CloudWatch for a metric. All of the data in the set
        have the same timestamp and unit.

        Args:
            namespace (str): The namespace of the metric.
            name (str): The name of the metric.
            timestamp (datetime): The UTC timestamp for the metric.
            unit (str): The unit of the metric.
            data_set (dict): The set of data to send. This set is a dictionary that
                         contains a list of values and a list of corresponding counts.
                         The value and count lists must be the same length.
        """
        try:
            metric = self.resource.Metric(namespace, name)
            metric.put_data(
                Namespace=namespace,
                MetricData=[
                    {
                        "MetricName": name,
                        "Timestamp": timestamp,
                        "Values": data_set["values"],
                        "Counts": data_set["counts"],
                        "Unit": unit,
                    }
                ],
            )
            self.logger.debug("Put data set for metric %s.%s.", namespace, name)
        except ClientError:
            self.logger.exception(
                "Couldn't put data set for metric %s.%s.", namespace, name
            )
            raise

    def get_metric_statistics(
        self,
        namespace: str,
        name: str,
        start: datetime,
        end: datetime,
        period,
        stat_types: str,
    ) -> int:
        """Gets statistics for a metric within a specified time span. Metrics are grouped
        into the specified period.

        Args:
            namespace (str): The namespace of the metric.
            name (str): The name of the metric.
            start (datetime): The UTC start time of the time span to retrieve.
            end (datetime): The UTC end time of the time span to retrieve.
            period (_type_): The period, in seconds, in which to group metrics. The period
                       must match the granularity of the metric, which depends on
                       the metric's age. For example, metrics that are older than
                       three hours have a one-minute granularity, so the period must
                       be at least 60 and must be a multiple of 60.
            stat_types (str): The type of statistics to retrieve, such as average value
                           or maximum value.

        Returns:
            int: The retrieved statistics for the metric.
        """
        try:
            metric = self.resource.Metric(namespace, name)
            stats = metric.get_statistics(
                StartTime=start, EndTime=end, Period=period, Statistics=stat_types
            )
            self.logger.debug(
                "Got %s statistics for %s.", len(stats["Datapoints"]), stats["Label"]
            )
        except ClientError:
            self.logger.exception("Couldn't get statistics for %s.%s.", namespace, name)
            raise
        else:
            return stats

    def create_metric_alarm(
        self,
        metric_namespace: str,
        metric_name: str,
        alarm_name: str,
        stat_type: str,
        period: int,
        eval_periods: int,
        threshold: int,
        comparison_op: str,
    ) -> dict:
        """Creates an alarm that watches a metric.

        Args:
            metric_namespace (str): The namespace of the metric.
            metric_name (str): The name of the metric.
            alarm_name (str): The name of the alarm.
            stat_type (str): The type of statistic the alarm watches.
            period (int): The period in which metric data are grouped to calculate
                       statistics.
            eval_periods (int): The number of periods that the metric must be over the
                             alarm threshold before the alarm is set into an alarmed
                             state.
            threshold (int): The threshold value to compare against the metric statistic.
            comparison_op (str): The comparison operation used to compare the threshold
                              against the metric.

        Returns:
            dict: The newly created alarm.
        """
        try:
            metric = self.resource.Metric(metric_namespace, metric_name)
            alarm = metric.put_alarm(
                AlarmName=alarm_name,
                Statistic=stat_type,
                Period=period,
                EvaluationPeriods=eval_periods,
                Threshold=threshold,
                ComparisonOperator=comparison_op,
            )
            self.logger.debug(
                "Added alarm %s to track metric %s.%s.",
                alarm_name,
                metric_namespace,
                metric_name,
            )
        except ClientError:
            self.logger.exception(
                "Couldn't add alarm %s to metric %s.%s",
                alarm_name,
                metric_namespace,
                metric_name,
            )
            raise
        else:
            return alarm

    def get_metric_alarms(self, metric_namespace: str, metric_name: str) -> Iterable:
        """Gets the alarms that are currently watching the specified metric.

        Args:
            metric_namespace (str): The namespace of the metric.
            metric_name (str): The name of the metric.

        Returns:
            Iterable: An iterator that yields the alarms.
        """
        metric = self.resource.Metric(metric_namespace, metric_name)
        alarm_iter = metric.alarms.all()
        self.logger.debug("Got alarms for metric %s.%s.", metric_namespace, metric_name)
        return alarm_iter

    def enable_alarm_actions(self, alarm_name: str, enable: bool) -> None:
        """Enables or disables actions on the specified alarm. Alarm actions can be
        used to send notifications or automate responses when an alarm enters a
        particular state.

        Args:
            alarm_name (str): The name of the alarm.
            enable (bool): When True, actions are enabled for the alarm. Otherwise, they
                       disabled.
        """
        try:
            alarm = self.resource.Alarm(alarm_name)
            if enable:
                alarm.enable_actions()
            else:
                alarm.disable_actions()
            self.logger.debug(
                "%s actions for alarm %s.",
                "Enabled" if enable else "Disabled",
                alarm_name,
            )
        except ClientError:
            self.logger.exception(
                "Couldn't %s actions alarm %s.",
                "enable" if enable else "disable",
                alarm_name,
            )
            raise

    def delete_metric_alarms(self, metric_namespace: str, metric_name: str) -> None:
        """Deletes all of the alarms that are currently watching the specified metric.

        Args:
            metric_namespace (str): The namespace of the metric.
            metric_name (str): The name of the metric.
        """
        try:
            metric = self.resource.Metric(metric_namespace, metric_name)
            metric.alarms.delete()
            self.logger.debug(
                "Deleted alarms for metric %s.%s.", metric_namespace, metric_name
            )
        except ClientError:
            self.logger.exception(
                "Couldn't delete alarms for metric %s.%s.",
                metric_namespace,
                metric_name,
            )
            raise
