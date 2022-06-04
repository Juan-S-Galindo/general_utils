from os import path, path, makedirs
from shutil import rmtree
from requests import post, get, put
from typing import Union, List, Callable
from json import load
from math import ceil, floor
from datetime import datetime
from pathlib import Path

from general_utils.decorators import api_multi_call_decorator


def path_join(*args: str) -> str:
    """Function to join args in a path using the system format.

    Returns:
        str
    """
    return path.join(*args)


def get_file_size(file_path: str) -> int:
    """Function to get the size of file in bytes.

    Args:
        file_path (str)

    Returns:
        int
    """
    return path.getsize(file_path)


def http_request(
    url: str, request_function: Callable, max_attempts: int, timeout: int, **kargs
) -> dict:
    """Function to perform http put request. If response is not 200, function retries N times specified by argument max_attempts

    Args:
        url (str)
        request_function (Callable)
        max_attempts (int)
        timeout (int)

    Returns:
        dict
    """

    @api_multi_call_decorator(max_attempts)
    def get_wrapper():
        return request_function(url, timeout=timeout, **kargs)

    return get_wrapper()


def send_put_request(url: str, max_attempts: int, timeout: int, **kargs) -> dict:
    """Function to perform http put request. If response is not 200, function retries N times specified by argument max_attempts


    Args:
        url (str)
        max_attempts (int)
        timeout (int)

    Returns:
        dict
    """
    return http_request(
        url, request_function=put, timeout=timeout, max_attempts=max_attempts, **kargs
    )


def send_get_request(url: str, max_attempts: int, timeout: int, **kargs) -> dict:
    """Function to perform http get request. If response is not 200, function retries N times specified by argument max_attempts


    Args:
        url (str)
        max_attempts (int)
        timeout (int)

    Returns:
        dict
    """
    return http_request(
        url, request_function=get, timeout=timeout, max_attempts=max_attempts, **kargs
    )


def send_post_request(url: str, max_attempts: int, timeout: int, **kargs) -> dict:
    """Function to perform http post request. If response is not 200, function retries N times specified by argument max_attempts


    Args:
        url (str)
        max_attempts (int)
        timeout (int)

    Returns:
        dict
    """
    return http_request(
        url,
        request_function=post,
        timeout=timeout,
        max_attempts=max_attempts,
        **kargs,
    )


def extension_check(extension: str, extension_check: str) -> bool:
    """Function to comapre if a file extension matches a specified extension.

    Args:
        extension (str)
        extension_check (str): The extension we want to match.

    Returns:
        bool: If the extension matches the desired extension, True else False
    """
    return extension == extension_check


def filter_dict_or_dicts_in_list(
    input: Union[dict, List[dict]], filter_dictionary_keys: list
) -> Union[dict, list]:
    """If input is a dictionary, the method filters the dictionary and returns the subset of keys specified in the filter_dictionary_keys argument.
        If the input is a list of dictinaries, each dictionary is filtered to return the subset of keys specified in the filter_dictionary_keys argument
        and then returns the list of subset dictionaries.

    Args:
        input (Union[dict,List[dict]])
        filter_dictionary_keys (list)

    Raises:
        TypeError: If input is not list or dict instance.

    Returns:
        Union[dict,list]
    """
    if isinstance(input, list):

        def subset_dict_filter(item):

            if not isinstance(item, dict):
                raise TypeError("item can only be dict instance.")

            _ = {}

            for key in item:

                if key in filter_dictionary_keys:

                    _[key] = item[key]

            if _:
                return _
            else:
                return None

        return list(filter(None, map(subset_dict_filter, input)))

    elif isinstance(input, dict):

        return {
            key: value for key, value in input.items() if key in filter_dictionary_keys
        }

    else:
        raise TypeError(
            f"input argument can only be list or dict instead got type: {type(input)} - input: {input}"
        )


def get_json_vars(file_name: str, config_folder: str, **kargs) -> dict:
    """Function to get values inside key-value pairs into variables.

    Args:
        file_name (str)
        config_folder (str)

    Returns:
        dict
    """
    with open(path_join(config_folder, file_name), "r") as config_mapper:

        json_mapper = load(config_mapper)

        for key, value in kargs.items():

            kargs[key] = json_mapper[value]
    return (karg for karg in kargs.values())


def dict_set(dict_list: List[dict]) -> List[dict]:
    """Function to generate a set from dictionaries without unique key identifier.

    Args:
        dict_list (List[dict])

    Returns:
        List[dict]
    """
    return [dict(t) for t in set(tuple(sorted(d.items())) for d in dict_list)]


def databus_docdb_mapper(dictionary: dict, mapping_dict: dict = None) -> dict:
    """Function to flat nested dictionaries using a mapping dictionary.

    Args:
        dictionary (dict)
        mapping_dict (dict, optional). Defaults to None.

    Returns:
        dict

    Yields:
        Iterator[dict]
    """
    for key, value in dictionary.items():

        if key in mapping_dict:

            if type(value) is dict:

                yield from databus_docdb_mapper(value, mapping_dict=mapping_dict[key])

            elif type(value) is list:

                for dictionary in value:

                    yield from databus_docdb_mapper(
                        dictionary, mapping_dict=mapping_dict[key]
                    )
            else:

                yield (mapping_dict[key], value)


def directory_exists(path_dir: str) -> bool:
    """Function to check if a dictory path exists

    Args:
        path_dir (str)

    Returns:
        bool
    """
    return path.isdir(path_dir)


def file_exists(path_file: str) -> bool:
    """Function to check if file exists

    Args:
        path_file (str)

    Returns:
        bool
    """
    return path.isfile(path_file)


def delete_path(path: str) -> None:
    """Function to delete directory paths

    Args:
        path (str)
    """
    rmtree(path)


def create_directories(path: str) -> None:
    """Function to create all directories necessary to make a path valid.

    Args:
        path (str)
    """
    makedirs(path)


def current_timestamp(
    iso_format: bool = False, string_format: str = None
) -> Union[datetime, str]:
    """Function to get the a time stamp in different formats.

    Args:
        iso_format (bool, optional): Function returns a string timestamp in ISO 8601 format. Defaults to False.
        string_format (str, optional): Function returns a string timestam based on the format codes given. Defaults to None.
                                        Reference: https://www.geeksforgeeks.org/python-strftime-function/

    Returns:
        Union[datetime,str]: If iso_format or string_format:String else datetime object.
    """
    if iso_format:
        value = datetime.now().isoformat()
    elif string_format is not None:
        value = datetime.now().strftime(string_format)
    else:
        value = datetime.now()

    return value


def round_number(number: float, round_up: bool = False, decimals: int = 2) -> float:
    """Function to round up/down numbers.

    Args:
        number (float): Nomber to round up/down.
        round_up (bool, optional): Defualt behavior is to round down. Set to True to round up. Defaults to False.
        decimals (int, optional): Desired number of significant digits. Defaults to 2.

    Raises:
        TypeError: If decimals input is not integer.
        ValueError: If decimals input is negative.

    Returns:
        float
    """
    if decimals < 0:
        raise ValueError("Decimal places has to be 0 or more")
    elif not isinstance(decimals, int):
        raise TypeError("Decimal places must be an integer")
    elif decimals == 0:
        return ceil(number) if round_up else floor(number)

    factor = 10**decimals

    return (
        ceil(number * factor) / factor if round_up else floor(number * factor) / factor
    )


def clean_str(
    input: str, upper_case: bool = False, fill_underscore: bool = False
) -> str:
    """Function to remove extra spaces and upper/lower case letters in a string.

    Args:
        input (str)
        upper_case (bool, optional): String is returned in upper case. Defaults to False.

    Returns:
        str
    """
    if isinstance(input, str):
        if fill_underscore:
            delim = "_"
        else:
            delim = " "
        input = delim.join(filter(None, str(input).split(" ")))

        return input.upper() if upper_case else input.lower()
    else:
        return input


def path_parse(
    file_name: str, get_file_name: bool = False, get_folders: bool = False
) -> str:
    """Method to get the file name or prefix from a s3 object key name.

    Args:
        file_name (str)
        get_file_name (bool, optional). Defaults to False.
        get_folders (bool, optional). Defaults to False.

    Returns:
        str
    """
    path_object = Path(file_name)

    if get_file_name:
        return str(path_object.name)

    if get_folders:
        folder = str(path_object.parent)
        return folder if folder != "." else "root"


def file_name_parser(file_name: str, return_extension: bool = False) -> str:
    """Function to separate the string and the extension from a file name

    Args:
        file_name (str)
        return_extension (bool, optional): if True, returns the string of the file name. If False, returns the file extension. Defaults to False.

    Returns:
        str
    """
    file_name_data_list = path.splitext(p=file_name)

    if return_extension:

        return file_name_data_list[-1]

    else:
        return file_name_data_list[0]


def get_file_name_from_key(key: str) -> str:
    """Function to get the filename from a path.

    Args:
        path (str)

    Returns:
        str
    """
    return path.basename(key)


def is_csv(file_name: str) -> bool:
    """Function to check if the file is a csv file.

    Args:
        file_name (str)

    Returns:
        bool
    """
    extension = file_name_parser(file_name=file_name, return_extension=True)

    return extension_check(extension=extension, extension_check=".csv")


def remove_non_digit_from_str(input: str) -> str:
    """Function to remove non digit characters from string.
    Args:
        input (str)
    Returns:
        str
    """
    return "".join(list(filter(lambda x: x.isdigit(), str(input))))
