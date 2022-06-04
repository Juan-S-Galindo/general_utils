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
                        f"API retry attempts exceeded. error: {response.text if 'text' in dir(response) else 'No info on this error'}"
                    )

                response = payload_object(*args, **kwargs)

                if 200 <= response.status_code < 300:

                    break

                attempt += 1

            return response

        return multi_attempt_api_call

    return function_wrapper
