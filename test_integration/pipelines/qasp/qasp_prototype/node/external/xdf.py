# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from intelliflow.api_ext import *


def create(app: AWSApplication, encryption_key: str) -> MarshalerNode:
    # e.g s3://searchdata-core-xdf-test/na/2021050145
    return app.marshal_external_data(
        S3Dataset(
            "111222333444",
            "searchdata-core-xdf-test",
            "",
            "{}",
            "{}*",  # * -> to ignore the last two digit gibberish added as a suffix to partition value
            dataset_format=DataFormat.PARQUET,
            encryption_key=encryption_key,
        ),
        id="xdf_external",
        dimension_spec={
            "cdo_region": {
                "type": DimensionType.STRING,
                "format": lambda dim: dim.lower(),
                "insensitive": True,
                # format is required to get rid of raw date partition's two digit suffix
                "day": {"type": DimensionType.DATETIME, "format": "%Y%m%d"},
            }
        },
    )
