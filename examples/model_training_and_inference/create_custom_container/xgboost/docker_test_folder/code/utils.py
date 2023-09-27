# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""
References:
1. SageMaker XGBoost container implementation
https://github.com/aws/sagemaker-xgboost-container/blob/13a194f0c6e4a3264712dfa84227673eb445e8eb/src/sagemaker_xgboost_container

2. SageMaker Training Toolkit:
https://github.com/aws/sagemaker-training-toolkit

3. SageMaker Inference Toolkit implementation:
https://github.com/aws/sagemaker-inference-toolkit/blob/master/src/sagemaker_inference

4. BatchTransform BYO XGBoost
https://github.com/aws/amazon-sagemaker-examples/blob/main/aws_sagemaker_studio/sagemaker_studio_image_build/xgboost_bring_your_own/Batch_Transform_BYO_XGB.ipynb
"""

from typing import Sequence, Union

import pandas as pd
import csv
import xgboost as xgb


def convert_to_float_or_default_to_categorical(pd_series: pd.Series) -> pd.Series:
    """
    First try to convert a Pandas DataFrame column to float, if failed then convert to categorical.

    Args:
        pd_series (pd.Series): a column in a Pandas DataFrame

    Returns:
        (pd.Series): A new column of dtype "float64" or "category"
    """
    try:
        return pd_series.astype(float)
    except ValueError:
        return pd_series.astype("category")


def split_and_fillna(csv_row_string: str, delimiter: str) -> Sequence[str]:
    """
    Fill empty fields with NAN after splitting by delimiter.

    Args:
        csv_row_string (str): assumes the string has no leading or trailing newline characters.
        delimiter (str):  csv delimiter

    Returns:
        (Sequence[str]): splitted row with empty fields replaced by "nan"
    """
    return ["nan" if x == "" else x for x in csv_row_string.split(delimiter)]


def clean_input(input: Union[str, bytes]) -> str:
    """
    Decode input using 'UTF-8' encoding if input is byte, then strip off leading and trailing newlines.

    Args:
        input (str/binary): CSV string or binary object(encoded by UTF-8), this is directly from HTTP request POST payloads.

    Returns:
        (str): Cleaned string without leading and trailing newlines
    """

    csv_string = input.decode() if isinstance(input, bytes) else input
    return csv_string.strip()


def sniff_delimiter(csv_string: str) -> str:
    """
    Detect the delimiter of CSV input.

    Args:
        csv_string (str): assumes the string has been stripped of leading or trailing newline characters.

    Returns:
        (str): Sniffed delimiter
    """
    sniff_delimiter = csv.Sniffer().sniff(csv_string.split("\n")[0][:512]).delimiter
    delimiter = "," if sniff_delimiter.isalnum() else sniff_delimiter
    return delimiter


def csv_to_dmatrix(input: Union[str, bytes], dtype=None) -> xgb.DMatrix:
    """Convert a CSV object to a DMatrix object.
    Args:
        input (str/binary): CSV string or binary object(encoded by UTF-8).
                                Assumes the string has been stripped of leading or trailing newline chars.
        dtype (dtype, optional):  Data type of the resulting array. If None, the dtypes will be determined by the
                                        contents of each column, individually. This argument can only be used to
                                        'upcast' the array.  For downcasting, use the .astype(t) method.
    Returns:
        (xgb.DMatrix): XGBoost DataMatrix
    """

    csv_string = clean_input(input)

    delimiter = sniff_delimiter(csv_string)

    print("Determined delimiter of CSV input is '{}'".format(delimiter))

    input_list = [split_and_fillna(row, delimiter) for row in csv_string.split("\n")]

    df = pd.DataFrame(input_list).apply(convert_to_float_or_default_to_categorical)

    return xgb.DMatrix(df, enable_categorical=True)