from datetime import datetime

from requests import post, get, put

import pytest
from unittest import TestCase

from src.general_utils import utils


@pytest.mark.parametrize(
    "inputs,output",
    [
        (
            {"file_name": __file__, "get_file_name": False, "get_folders": True},
            "unittests",
        ),
        (
            {"file_name": __file__, "get_file_name": True, "get_folders": False},
            "test_utils.py",
        ),
    ],
)
def test_path_parse(inputs, output):

    assert utils.path_parse(**inputs).endswith(output)


def test_path_parse_get_file_name():

    assert utils.path_parse(__file__, get_file_name=True) == "test_utils.py"


def test_path_parse_raise_error():

    with pytest.raises(ValueError):
        utils.path_parse(file_name=__file__, get_file_name=True, get_folders=True)


BASE_DIR = utils.path_parse(file_name=__file__, get_folders=True)


@pytest.mark.parametrize(
    "inputs,output",
    [
        ({"file_name": "test_utils.py", "return_extension": False}, "test_utils"),
        ({"file_name": "test_utils.py", "return_extension": True}, ".py"),
    ],
)
def test_file_name_parser(inputs, output):

    assert utils.file_name_parser(**inputs) == output


@pytest.mark.parametrize(
    "input,output",
    [
        (["test", "test_2"], "test/test_2"),
        (
            ["test", "test_2", "test_3", "test_utils.py"],
            "test/test_2/test_3/test_utils.py",
        ),
    ],
)
def test_path_join(input, output):

    assert utils.path_join(*input) == output


@pytest.mark.parametrize("input,output", [("test.zip", False), ("test.csv", True)])
def test_is_csv(input, output):

    assert utils.is_csv(input) == output


def test_get_file_size_file_exists():
    file_path = utils.path_join(BASE_DIR, "test_data/test_file.csv")

    assert utils.file_exists(file_path)
    assert utils.get_file_size(file_path) == 8660


def test_directory_create_exists_delete():

    dirs_to_create = ["TEST_DIR1", "TEST_DIR2"]

    new_path = utils.path_join(BASE_DIR, *dirs_to_create)

    utils.create_directories(new_path)
    assert utils.directory_exists(new_path)

    while dirs_to_create:

        path_to_delete = utils.path_join(BASE_DIR, *dirs_to_create)
        utils.delete_path(path_to_delete)

        assert not utils.directory_exists(path_to_delete)

        dirs_to_create.pop()


@pytest.mark.parametrize(
    "inputs,output",
    [
        (
            {
                "input": " test    string ",
                "upper_case": False,
                "fill_underscore": False,
            },
            "test string",
        ),
        (
            {"input": " test    string ", "upper_case": False, "fill_underscore": True},
            "test_string",
        ),
        (
            {"input": " test    string ", "upper_case": True, "fill_underscore": False},
            "TEST STRING",
        ),
        (
            {"input": " test    string ", "upper_case": True, "fill_underscore": True},
            "TEST_STRING",
        ),
    ],
)
def test_clean_str(inputs, output):

    assert utils.clean_str(**inputs) == output


@pytest.mark.parametrize(
    "input,output",
    [
        ("1234567890", "1234567890"),
        ("1+2-3]4[5,6/7?8A9B0", "1234567890"),
    ],
)
def test_remove_non_digit_from_str(input, output):

    assert utils.remove_non_digit_from_str(input) == output


@pytest.mark.parametrize(
    "inputs,output",
    [
        (
            {"iso_format": False, "string_format": None, "use_utc": False},
            datetime.now(),
        ),
        (
            {"iso_format": False, "string_format": None, "use_utc": True},
            datetime.utcnow(),
        ),
    ],
)
def test_current_timestamp(inputs, output):

    assert utils.current_timestamp(**inputs).hour == output.hour
    assert utils.current_timestamp(**inputs).minute == output.minute


@pytest.mark.parametrize(
    "inputs,output",
    [
        (
            {"iso_format": True, "string_format": None, "use_utc": False},
            datetime.now().isoformat().partition(":")[0],
        ),
        (
            {"iso_format": True, "string_format": None, "use_utc": True},
            datetime.utcnow().isoformat().partition(":")[0],
        ),
    ],
)
def test_current_timestamp_isoformat_strings(inputs, output):

    assert utils.current_timestamp(**inputs).partition(":")[0] == output


@pytest.mark.parametrize(
    "inputs,output",
    [
        ({"number": 2.659, "round_up": True}, 2.66),
        ({"number": 2.659, "round_up": False}, 2.65),
        ({"number": 2.659, "round_up": True, "decimals": 1}, 2.7),
        ({"number": 2.659, "round_up": False, "decimals": 1}, 2.6),
        ({"number": 2.659, "round_up": True, "decimals": 0}, 3),
        ({"number": 2.659, "round_up": False, "decimals": 0}, 2),
    ],
)
def test_round_number(inputs, output):

    assert utils.round_number(**inputs) == output


def test_dict_set():

    case = TestCase()
    input_list = [
        {"key_1": 1, "key2": 2},
        {"key_1": 1, "key2": 2},
        {"key_2": 2, "key3": 3},
    ]

    output = [{"key2": 2, "key_1": 1}, {"key3": 3, "key_2": 2}]

    case.assertCountEqual(utils.dict_set(input_list), output)


def test_filter_dict_or_dicts_in_list_DICT():

    input = {
        "key_1": 1,
        "key_2": 2,
        "key_3": 3,
        "key_4": 4,
        "key_5": 5,
        "key_6": 6,
        "key_7": 7,
        "key_8": 8,
        "key_9": 9,
        "key_10": 10,
    }

    filter_keys = ["key_1", "key_3", "key_5"]

    output = {"key_1": 1, "key_3": 3, "key_5": 5}
    assert utils.filter_dict_or_dicts_in_list(input, filter_keys) == output


def test_filter_dict_or_dicts_in_list_LIST_DICT():

    input = [
        {
            "key_1": 1,
            "key_2": 2,
            "key_3": 3,
            "key_4": 4,
            "key_5": 5,
        },
        {
            "key_6": 6,
            "key_7": 7,
            "key_8": 8,
            "key_9": 9,
            "key_10": 10,
        },
    ]

    filter_keys = ["key_1", "key_3", "key_6"]

    output = [{"key_1": 1, "key_3": 3}, {"key_6": 6}]
    assert utils.filter_dict_or_dicts_in_list(input, filter_keys) == output


@pytest.mark.parametrize(
    "inputs",
    (
        {
            "url": "https://httpbin.org/put",
            "request_function": put,
            "max_attempts": 5,
            "timeout": 10,
        },
        {
            "url": "https://httpbin.org/get",
            "request_function": get,
            "max_attempts": 5,
            "timeout": 10,
        },
        {
            "url": "https://httpbin.org/post",
            "request_function": post,
            "max_attempts": 5,
            "timeout": 10,
        },
    ),
)
def test_http_request(inputs):

    response = utils._http_request(**inputs)

    assert response.status_code == 200


@pytest.mark.parametrize(
    "inputs,function",
    (
        [
            {"url": "https://httpbin.org/put", "max_attempts": 5, "timeout": 10},
            utils.send_get_request,
        ],
        [
            {"url": "https://httpbin.org/get", "max_attempts": 5, "timeout": 10},
            utils.send_post_request,
        ],
        [
            {"url": "https://httpbin.org/post", "max_attempts": 5, "timeout": 10},
            utils.send_put_request,
        ],
    ),
)
def test_http_request_failed_call(inputs, function):

    with pytest.raises(Exception):
        function(**inputs)
