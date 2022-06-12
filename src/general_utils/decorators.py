from aws_lambda_powertools import Logger
from typing import Callable
from functools import wraps
from time import sleep


class HtmlResponseObject:
    pass


def api_multi_call_decorator(times_to_retry: int) -> HtmlResponseObject:
    """Decorator to call the API multiple times if the response code is not 200s.

    Args:
        times_to_retry (int)
    Raises:
            Exception: If the number of max attempts is reached without 200 response
    Returns:
        HtmlResponseObject

    """

    def function_wrapper(payload_object: HtmlResponseObject) -> HtmlResponseObject:

        if times_to_retry < 1:
            raise ValueError("times_to_retry argument cannot be less than 1")

        def multi_attempt_api_call(*args, **kwargs):

            attempt = 0

            while attempt <= times_to_retry:

                if attempt == times_to_retry:

                    raise Exception(
                        f"API retry attempts exceeded. Error: {response.text if 'text' in dir(response) else 'No info on this error'}"
                    )

                response = payload_object(*args, **kwargs)

                if 200 <= response.status_code < 300:

                    break

                attempt += 1

            return response

        return multi_attempt_api_call

    return function_wrapper


def function_retry_decorator(
    max_attempts: int,
    logger: Logger,
    time_between_attempts: int,
    kill_execution: bool,
) -> Callable:
    """Function to try to call a function a specified number of times and absorb any errors encountered until max_attempts are reached.

    Args:
        max_attempts (int)
        logger (Logger)
        time_between_attempts (int)
        kill_execution (bool)

    Returns:
        Callable
    """

    def function_wrapper(function: Callable) -> Callable:

        if max_attempts < 1:
            raise ValueError("max_attempts argument cannot be less than 1")

        @wraps(function)
        def multi_attempt_function_call(*args, **kwargs) -> Callable:

            for attempt in range(1, max_attempts + 1):
                try:
                    return function(*args, **kwargs)
                except Exception as e:
                    error = e
                    logger.error(
                        ex=error,
                        message=f"Exception raised during execution. Attempt: {attempt}.",
                    )
                    sleep(time_between_attempts)
            else:
                logger.info(
                    f"Failed to successfully call function. Killing process: {kill_execution}."
                )
                if kill_execution:
                    exit(1)
                else:
                    raise error

        return multi_attempt_function_call

    return function_wrapper
