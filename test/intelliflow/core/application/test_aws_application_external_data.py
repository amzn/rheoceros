# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import datetime

from dateutil.tz import tzlocal

from intelliflow.api_ext import *
from intelliflow.core.signal_processing.signal import *
from intelliflow.mixins.aws.test import AWSTestBase
from intelliflow.mixins.aws.test_catalog import AWSTestGlueCatalog
from intelliflow.utils.test.inlined_compute import NOOPCompute


class TestAWSApplicationExternalData(AWSTestBase, AWSTestGlueCatalog):

    # overrides
    def get_table(self, DatabaseName: str, Name: str) -> Dict[str, Any]:
        if DatabaseName == "booker" and Name == "d_unified_cust_shipment_items":
            return {
                "Table": {
                    "Name": "d_unified_cust_shipment_items",
                    "DatabaseName": "booker",
                    "CreateTime": datetime.datetime(2020, 7, 24, 7, 43, 18, tzinfo=tzlocal()),
                    "UpdateTime": datetime.datetime(2020, 10, 28, 3, 18, 2, tzinfo=tzlocal()),
                    "LastAccessTime": datetime.datetime(2020, 7, 24, 7, 43, 18, tzinfo=tzlocal()),
                    "Retention": 0,
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "base_currency_code", "Type": "string", "Comment": ""},
                            {"Name": "billing_address_id", "Type": "decimal(38,0)", "Comment": ""},
                            {"Name": "bin_id", "Type": "string", "Comment": ""},
                            {"Name": "condition", "Type": "decimal(38,0)", "Comment": ""},
                            {"Name": "confidential_shipping_address", "Type": "decimal(38,0)", "Comment": ""},
                            {"Name": "customer_order_item_detail_id", "Type": "decimal(38,0)", "Comment": ""},
                            {"Name": "distributor_order_id", "Type": "string", "Comment": ""},
                            {"Name": "distributor_shipment_id", "Type": "string", "Comment": ""},
                            {"Name": "lister_id", "Type": "string", "Comment": ""},
                            {"Name": "checkout_session_language", "Type": "string", "Comment": ""},
                            {"Name": "monetory_unit", "Type": "string", "Comment": ""},
                            {"Name": "requested_domain_name", "Type": "string", "Comment": ""},
                        ],
                        "Location": "s3://foo-subscriptions/222333444555/BOOKER/.db/D_UNIFIED_CUST_SHIPMENT_ITEMS",
                        "InputFormat": "amazon.conexio.hive.EDXManifestHiveInputFormat",
                        "OutputFormat": "amazon.conexio.hive.EDXManifestHiveOutputFormat",
                        "Compressed": False,
                        "NumberOfBuckets": 0,
                        "SerdeInfo": {
                            "Name": "edx_serde",
                            "SerializationLibrary": "amazon.conexio.hive.serde.edx.GenericEDXSerDe",
                            "Parameters": {},
                        },
                        "SortColumns": [],
                        "StoredAsSubDirectories": False,
                    },
                    "PartitionKeys": [
                        {"Name": "region_id", "Type": "decimal(38,0)", "Comment": ""},
                        {"Name": "ship_day", "Type": "timestamp", "Comment": ""},
                    ],
                    "TableType": "EXTERNAL_TABLE",
                    "Parameters": {
                        "classification": "csv",
                    },
                    "CreatedBy": "arn:aws:sts::222333444555:assumed-role/foo-glue-service-role-us-east-1/Synchronizer",
                    "IsRegisteredWithLakeFormation": False,
                    "CatalogId": "222333444555",
                },
                "ResponseMetadata": {
                    "RequestId": "4c9ebfe3-eb25-4e7b-b154-cfdb7d64d541",
                    "HTTPStatusCode": 200,
                    "HTTPHeaders": {
                        "date": "Thu, 23 Sep 2021 02:33:11 GMT",
                        "content-type": "application/x-amz-json-1.1",
                        "content-length": "14790",
                        "connection": "keep-alive",
                        "x-amzn-requestid": "4c9ebfe3-eb25-4e7b-b154-cfdb7d64d541",
                    },
                    "RetryAttempts": 0,
                },
            }
        elif DatabaseName == "booker" and Name == "d_unified_cust_shipment_items_PARQUET":
            return {
                "Table": {
                    "Name": "d_unified_cust_shipment_items_PARQUET",
                    "DatabaseName": "booker",
                    "CreateTime": datetime.datetime(2020, 1, 12, 4, 53, 20, tzinfo=tzlocal()),
                    "UpdateTime": datetime.datetime(2020, 1, 13, 8, 18, 21, tzinfo=tzlocal()),
                    "LastAccessTime": datetime.datetime(2020, 1, 12, 4, 53, 18, tzinfo=tzlocal()),
                    "Retention": 0,
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "customer_id", "Type": "decimal(38,0)", "Comment": ""},
                            {"Name": "region_id", "Type": "decimal(38,0)", "Comment": ""},
                            {"Name": "order_day", "Type": "timestamp", "Comment": ""},
                            {"Name": "completed_count", "Type": "bigint", "Comment": ""},
                            {"Name": "uncompleted_count", "Type": "bigint", "Comment": ""},
                        ],
                        "Location": "s3://random-bucket/11111111-2222-3333-4444-555555555555/a07ec394-114a-48f6-9e7a-1c8daa8cee68",
                        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                        "Compressed": False,
                        "NumberOfBuckets": 0,
                        "SerdeInfo": {
                            "Name": "cairns_serde",
                            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                            "Parameters": {},
                        },
                        "BucketColumns": [],
                        "SortColumns": [],
                        "Parameters": {},
                        "StoredAsSubDirectories": False,
                    },
                    "PartitionKeys": [
                        {"Name": "region_id", "Type": "decimal(38,0)", "Comment": ""},
                        {"Name": "order_day", "Type": "timestamp", "Comment": ""},
                    ],
                    "TableType": "MANAGED_TABLE",
                    "Parameters": {
                        "classification": "parquet",
                        "compressionType": "none",
                        "spark.sql.create.version": "2.2 or prior",
                        "spark.sql.sources.schema.numPartCols": "2",
                        "spark.sql.sources.schema.partCol.0": "region_id",
                        "spark.sql.sources.schema.partCol.1": "order_day",
                        "typeOfData": "file",
                    },
                    "CreatedBy": "arn:aws:sts::111222333444:assumed-role/foo-glue-service-role-us-east-1/Synchronizer",
                    "IsRegisteredWithLakeFormation": False,
                    "CatalogId": "111222333444",
                },
                "ResponseMetadata": {
                    "RequestId": "b089d1ea-19e5-4ee5-acf6-58637e68cc64",
                    "HTTPStatusCode": 200,
                    "HTTPHeaders": {
                        "date": "Thu, 23 Sep 2021 02:40:07 GMT",
                        "content-type": "application/x-amz-json-1.1",
                        "content-length": "2103",
                        "connection": "keep-alive",
                        "x-amzn-requestid": "b089d1ea-19e5-4ee5-acf6-58637e68cc64",
                    },
                    "RetryAttempts": 0,
                },
            }
        elif DatabaseName == "dex_ml_catalog" and Name == "d_ad_orders_na":
            return {
                "Table": {
                    "Name": "d_ad_orders_na",
                    "DatabaseName": "dex_ml_catalog",
                    "CreateTime": datetime.datetime(2021, 1, 12, 4, 53, 20, tzinfo=tzlocal()),
                    "UpdateTime": datetime.datetime(2021, 1, 13, 8, 18, 21, tzinfo=tzlocal()),
                    "LastAccessTime": datetime.datetime(2021, 1, 12, 4, 53, 18, tzinfo=tzlocal()),
                    "Retention": 0,
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "customer_id", "Type": "decimal(38,0)", "Comment": ""},
                            {"Name": "order_day", "Type": "timestamp", "Comment": ""},
                            {"Name": "completed_count", "Type": "bigint", "Comment": ""},
                            {"Name": "uncompleted_count", "Type": "bigint", "Comment": ""},
                        ],
                        "Location": "s3://11111111-2222-3333-4444-555555555555/a07ec394-114a-48f6-9e7a-1c8daa8cee68",
                        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                        "Compressed": False,
                        "NumberOfBuckets": 0,
                        "SerdeInfo": {
                            "Name": "cairns_serde",
                            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                            "Parameters": {},
                        },
                        "BucketColumns": [],
                        "SortColumns": [],
                        "Parameters": {},
                        "StoredAsSubDirectories": False,
                    },
                    "PartitionKeys": [{"Name": "order_day", "Type": "timestamp", "Comment": ""}],
                    "TableType": "MANAGED_TABLE",
                    "Parameters": {
                        "classification": "parquet",
                        "compressionType": "none",
                        "spark.sql.create.version": "2.2 or prior",
                        "spark.sql.sources.schema.numPartCols": "1",
                        "spark.sql.sources.schema.numParts": "1",
                        "spark.sql.sources.schema.part.0": '{"type":"struct","fields":[{"name":"customer_id","type":"decimal(38,0)","nullable":true,"metadata":{"comment":""}},{"name":"order_day","type":"timestamp","nullable":true,"metadata":{"comment":""}},{"name":"completed_count","type":"long","nullable":true,"metadata":{"comment":""}},{"name":"uncompleted_count","type":"long","nullable":true,"metadata":{"comment":""}},{"name":"order_day","type":"timestamp","nullable":true,"metadata":{"comment":""}}]}',
                        "spark.sql.sources.schema.partCol.0": "order_day",
                        "typeOfData": "file",
                    },
                    "CreatedBy": "arn:aws:sts::222333444555:assumed-role/foo-glue-service-role-us-east-1/Synchronizer",
                    "IsRegisteredWithLakeFormation": False,
                    "CatalogId": "222333444555",
                },
                "ResponseMetadata": {
                    "RequestId": "b089d1ea-19e5-4ee5-acf6-58637e68cc64",
                    "HTTPStatusCode": 200,
                    "HTTPHeaders": {
                        "date": "Thu, 23 Sep 2021 02:40:07 GMT",
                        "content-type": "application/x-amz-json-1.1",
                        "content-length": "2103",
                        "connection": "keep-alive",
                        "x-amzn-requestid": "b089d1ea-19e5-4ee5-acf6-58637e68cc64",
                    },
                    "RetryAttempts": 0,
                },
            }
        elif DatabaseName == "atrops_ddl" and Name == "o_slam_packages":
            return {
                "Table": {
                    "Name": "o_slam_packages",
                    "DatabaseName": "atrops_ddl",
                    "CreateTime": datetime.datetime(2021, 4, 26, 23, 1, 34, tzinfo=tzlocal()),
                    "UpdateTime": datetime.datetime(2021, 4, 26, 23, 1, 34, tzinfo=tzlocal()),
                    "LastAccessTime": datetime.datetime(2021, 4, 26, 23, 1, 32, tzinfo=tzlocal()),
                    "Retention": 0,
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "region_id", "Type": "int", "Comment": ""},
                            {"Name": "request_date", "Type": "timestamp", "Comment": ""},
                            {"Name": "request_id", "Type": "string", "Comment": ""},
                            {"Name": "shipment_id", "Type": "bigint", "Comment": ""},
                            {"Name": "package_id", "Type": "int", "Comment": ""},
                            {"Name": "route_id", "Type": "string", "Comment": ""},
                            {"Name": "warehouse_id", "Type": "string", "Comment": ""},
                            {"Name": "ship_method", "Type": "string", "Comment": ""},
                            {"Name": "ship_option", "Type": "string", "Comment": ""},
                            {"Name": "carrier_name", "Type": "string", "Comment": ""},
                            {"Name": "zone", "Type": "string", "Comment": ""},
                            {"Name": "ship_cost", "Type": "decimal(10,2)", "Comment": ""},
                            {"Name": "ship_cost_uom", "Type": "string", "Comment": ""},
                        ],
                        "Location": "s3://foo-subscriptions/427809481713/ATROPS_DDL/.db/O_SLAM_PACKAGES",
                        "InputFormat": "amazon.conexio.hive.EDXManifestHiveInputFormat",
                        "OutputFormat": "amazon.conexio.hive.EDXManifestHiveOutputFormat",
                        "Compressed": False,
                        "NumberOfBuckets": 0,
                        "SerdeInfo": {
                            "Name": "edx_serde",
                            "SerializationLibrary": "amazon.conexio.hive.serde.edx.GenericEDXSerDe",
                            "Parameters": {},
                        },
                        "SortColumns": [],
                        "StoredAsSubDirectories": False,
                    },
                    "PartitionKeys": [
                        {"Name": "region_id", "Type": "decimal(38,0)", "Comment": ""},
                        {"Name": "request_date", "Type": "timestamp", "Comment": ""},
                    ],
                    "TableType": "EXTERNAL_TABLE",
                    "Parameters": {},
                    "CreatedBy": "arn:aws:sts::427809481713:assumed-role/foo-glue-service-role-us-east-1/Synchronizer",
                    "IsRegisteredWithLakeFormation": False,
                    "CatalogId": "111222333444",
                },
                "ResponseMetadata": {
                    "RequestId": "3fb566a8-a18c-4327-bc0a-cb31921c528e",
                    "HTTPStatusCode": 200,
                    "HTTPHeaders": {
                        "date": "Thu, 23 Sep 2021 02:56:57 GMT",
                        "content-type": "application/x-amz-json-1.1",
                        "content-length": "5614",
                        "connection": "keep-alive",
                        "x-amzn-requestid": "3fb566a8-a18c-4327-bc0a-cb31921c528e",
                    },
                    "RetryAttempts": 0,
                },
            }
        elif DatabaseName == "searchdata" and Name == "tommy_searches":
            return {
                "Table": {
                    "Name": "tommy_searches",
                    "DatabaseName": "searchdata",
                    "Owner": "searchdata",
                    "UpdateTime": datetime.datetime(2021, 4, 29, 18, 44, 44, tzinfo=tzlocal()),
                    "Retention": 900,
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "gmt_time", "Type": "string"},
                            {"Name": "marketplace_id", "Type": "int"},
                            {"Name": "alias", "Type": "string"},
                            {"Name": "site_variant", "Type": "string"},
                            {"Name": "search_type", "Type": "string"},
                            {"Name": "is_spam", "Type": "int"},
                            {"Name": "is_trusted_customer", "Type": "int"},
                            {"Name": "is_trusted_user", "Type": "int"},
                            {"Name": "is_spam_or_untrusted", "Type": "int"},
                            {"Name": "session", "Type": "string"},
                            {"Name": "query_group_id", "Type": "string"},
                            {"Name": "action_id", "Type": "int"},
                            {"Name": "rank_function", "Type": "string"},
                            {"Name": "is_prime_customer", "Type": "int"},
                            {"Name": "total_found", "Type": "int"},
                            {"Name": "weblabs", "Type": "string"},
                            {"Name": "total_displayed", "Type": "int"},
                            {"Name": "page", "Type": "int"},
                            {"Name": "browse_node", "Type": "string"},
                            {"Name": "spelling_action", "Type": "string"},
                            {"Name": "main_request_id", "Type": "string"},
                        ],
                        "Location": "s3://tommy-datasets-test/tommy-searches-parquet",
                        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                        "Compressed": False,
                        "NumberOfBuckets": 0,
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                            "Parameters": {"serialization.format": "1"},
                        },
                        "BucketColumns": [],
                        "SortColumns": [],
                        "StoredAsSubDirectories": False,
                    },
                    "PartitionKeys": [{"Name": "org", "Type": "string"}, {"Name": "partition_date", "Type": "string"}],
                    "TableType": "EXTERNAL_TABLE",
                    "Parameters": {"EXTERNAL": "TRUE", "has_encrypted_data": "true"},
                    "IsRegisteredWithLakeFormation": False,
                    "CatalogId": "111222333444",
                },
                "ResponseMetadata": {
                    "RequestId": "b17e8ccc-d5b2-46db-bceb-fe73c739c97c",
                    "HTTPStatusCode": 200,
                    "HTTPHeaders": {
                        "date": "Thu, 23 Sep 2021 03:01:04 GMT",
                        "content-type": "application/x-amz-json-1.1",
                        "content-length": "12252",
                        "connection": "keep-alive",
                        "x-amzn-requestid": "b17e8ccc-d5b2-46db-bceb-fe73c739c97c",
                    },
                    "RetryAttempts": 0,
                },
            }
        elif DatabaseName == "dexbi" and Name == "d_ship_option":
            return {
                "Table": {
                    "Name": "d_ship_option",
                    "DatabaseName": "dexbi",
                    "CreateTime": datetime.datetime(2021, 2, 6, 20, 15, 42, tzinfo=tzlocal()),
                    "UpdateTime": datetime.datetime(2021, 2, 6, 20, 15, 42, tzinfo=tzlocal()),
                    "LastAccessTime": datetime.datetime(2021, 2, 6, 20, 15, 42, tzinfo=tzlocal()),
                    "Retention": 0,
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "region_id", "Type": "decimal(38,0)", "Comment": ""},
                            {"Name": "marketplace_id", "Type": "decimal(38,0)", "Comment": ""},
                            {"Name": "legal_entity_id", "Type": "decimal(38,0)", "Comment": ""},
                            {"Name": "ship_option", "Type": "string", "Comment": ""},
                            {"Name": "ship_option_group", "Type": "string", "Comment": ""},
                            {"Name": "ship_option_group2", "Type": "string", "Comment": ""},
                            {"Name": "intl_sub_group", "Type": "string", "Comment": ""},
                            {"Name": "perf_ship_option_group", "Type": "string", "Comment": ""},
                        ],
                        "Location": "s3://foo-subscriptions/111222333444/DEXBI/.db/D_SHIP_OPTION",
                        "InputFormat": "amazon.conexio.hive.EDXManifestHiveInputFormat",
                        "OutputFormat": "amazon.conexio.hive.EDXManifestHiveOutputFormat",
                        "Compressed": False,
                        "NumberOfBuckets": 0,
                        "SerdeInfo": {
                            "Name": "edx_serde",
                            "SerializationLibrary": "amazon.conexio.hive.serde.edx.GenericEDXSerDe",
                            "Parameters": {},
                        },
                        "SortColumns": [],
                        "StoredAsSubDirectories": False,
                    },
                    "PartitionKeys": [],
                    "TableType": "EXTERNAL_TABLE",
                    "Parameters": {},
                    "CreatedBy": "arn:aws:sts::111222333444:assumed-role/foo-glue-service-role-us-east-1/Synchronizer",
                    "IsRegisteredWithLakeFormation": False,
                    "CatalogId": "111222333444",
                },
                "ResponseMetadata": {
                    "RequestId": "9a428327-d970-4533-b532-222a0e83ca35",
                    "HTTPStatusCode": 200,
                    "HTTPHeaders": {
                        "date": "Thu, 07 Oct 2021 04:01:55 GMT",
                        "content-type": "application/x-amz-json-1.1",
                        "content-length": "2660",
                        "connection": "keep-alive",
                        "x-amzn-requestid": "9a428327-d970-4533-b532-222a0e83ca35",
                    },
                    "RetryAttempts": 0,
                },
            }
        else:
            return None

    # overrides
    def get_partitions(self, DatabaseName: str, TableName: str, MaxResults: int) -> Dict[str, Any]:
        if DatabaseName == "booker" and TableName == "d_unified_cust_shipment_items":
            return {
                "Partitions": [
                    {
                        "Values": ["1", "1995-12-06 00:00:00", "1588942061259", "1588942061259", "SNAPSHOT"],
                        "DatabaseName": "booker",
                        "TableName": "d_unified_cust_shipment_items",
                        "CreationTime": datetime.datetime(2020, 7, 24, 7, 45, 5, tzinfo=tzlocal()),
                        "LastAccessTime": datetime.datetime(2020, 7, 24, 7, 44, 50, tzinfo=tzlocal()),
                        "StorageDescriptor": {
                            "Columns": [],
                            "Location": "s3:///foo-subscriptions/222333444555/BOOKER/D_UNIFIED_CUST_SHIPMENT_ITEMS/895a31f0-f3fc-482a-a7aa-bc740149ae2b/1/1995-12-06 00:00:00/1588942061259/1588942061259/SNAPSHOT/manifest.ion",
                            "InputFormat": "amazon.conexio.hive.EDXManifestHiveInputFormat",
                            "OutputFormat": "amazon.conexio.hive.EDXManifestHiveOutputFormat",
                            "Compressed": False,
                            "NumberOfBuckets": 0,
                            "SerdeInfo": {
                                "Name": "edx_serde",
                                "SerializationLibrary": "amazon.conexio.hive.serde.edx.GenericEDXSerDe",
                                "Parameters": {},
                            },
                            "SortColumns": [],
                            "StoredAsSubDirectories": False,
                        },
                        "Parameters": {},
                        "CatalogId": "222333444555",
                    }
                ],
                "NextToken": "eyJsYXN0RXZhbHVhdGVkS2V5Ijp7IkhBU0hfS0VZIjp7InMiOiJwLjVhYmMyMjY5ZTRkMzRjMmVhM2JhMWRmZTIyYmRiNDRkLjAifSwiUkFOR0VfS0VZIjp7InMiOiIxLDE5OTUtMTItMDYgMDA6MDA6MDAsMTU4ODk0MjA2MTI1OSwxNTg4OTQyMDYxMjU5LFNOQVBTSE9UIn19LCJleHBpcmF0aW9uIjp7InNlY29uZHMiOjE2MzI0NTA3OTEsIm5hbm9zIjo1NDYwMDAwMDB9LCJwYWdlU2l6ZSI6MX0=",
                "ResponseMetadata": {
                    "RequestId": "31dcd83c-8063-4c8c-984b-64d7ba772e52",
                    "HTTPStatusCode": 200,
                    "HTTPHeaders": {
                        "date": "Thu, 23 Sep 2021 02:33:11 GMT",
                        "content-type": "application/x-amz-json-1.1",
                        "content-length": "1197",
                        "connection": "keep-alive",
                        "x-amzn-requestid": "31dcd83c-8063-4c8c-984b-64d7ba772e52",
                    },
                    "RetryAttempts": 0,
                },
            }
        elif DatabaseName == "dex_ml_catalog" and TableName == "d_ad_orders_na":
            return {
                "Partitions": [
                    {
                        "Values": ["2021-05-09 00:00:00"],
                        "DatabaseName": "dex_ml_catalog",
                        "TableName": "d_ad_orders_na",
                        "CreationTime": datetime.datetime(2021, 5, 11, 0, 44, 30, tzinfo=tzlocal()),
                        "LastAccessTime": datetime.datetime(2021, 5, 16, 0, 39, 57, tzinfo=tzlocal()),
                        "StorageDescriptor": {
                            "Columns": [
                                {"Name": "customer_id", "Type": "decimal(38,0)", "Comment": ""},
                                {"Name": "order_day", "Type": "timestamp", "Comment": ""},
                                {"Name": "completed_count", "Type": "bigint", "Comment": ""},
                                {"Name": "uncompleted_count", "Type": "bigint", "Comment": ""},
                            ],
                            "Location": "s3://11111111-2222-3333-4444-555555555555/a07ec394-114a-48f6-9e7a-1c8daa8cee68/ea265ae3-860e-4552-9b35-363e5951b138/1621125220487",
                            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                            "Compressed": False,
                            "NumberOfBuckets": 0,
                            "SerdeInfo": {
                                "Name": "cairns_serde",
                                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                                "Parameters": {},
                            },
                            "SortColumns": [],
                            "StoredAsSubDirectories": False,
                        },
                        "Parameters": {"classification": "parquet"},
                        "CatalogId": "222333444555",
                    }
                ],
                "NextToken": "eyJsYXN0RXZhbHVhdGVkS2V5Ijp7IkhBU0hfS0VZIjp7InMiOiJwLmFlZjRmMTA5NDA0NDQxMjFhZDBkMGZmNjMyYTg0Nzc2LjAifSwiUkFOR0VfS0VZIjp7InMiOiIyMDIxLTA1LTA5IDAwOjAwOjAwIn19LCJleHBpcmF0aW9uIjp7InNlY29uZHMiOjE2MzI0NTEyMDcsIm5hbm9zIjozNjcwMDAwMDB9LCJwYWdlU2l6ZSI6MX0=",
                "ResponseMetadata": {
                    "RequestId": "b3cdc751-1da5-4d39-92f1-ceeed7d1f779",
                    "HTTPStatusCode": 200,
                    "HTTPHeaders": {
                        "date": "Thu, 23 Sep 2021 02:40:07 GMT",
                        "content-type": "application/x-amz-json-1.1",
                        "content-length": "1296",
                        "connection": "keep-alive",
                        "x-amzn-requestid": "b3cdc751-1da5-4d39-92f1-ceeed7d1f779",
                    },
                    "RetryAttempts": 0,
                },
            }
        elif DatabaseName == "atrops_ddl" and TableName == "o_slam_packages":
            return {
                "Partitions": [
                    {
                        "Values": ["1", "2018-12-28 00:00:00", "1551299222452", "1551299222452", "SNAPSHOT"],
                        "DatabaseName": "atrops_ddl",
                        "TableName": "o_slam_packages",
                        "CreationTime": datetime.datetime(2021, 4, 26, 23, 2, 25, tzinfo=tzlocal()),
                        "LastAccessTime": datetime.datetime(2021, 4, 26, 23, 2, 25, tzinfo=tzlocal()),
                        "StorageDescriptor": {
                            "Columns": [],
                            "Location": "s3://foo-subscriptions/111222333444/ATROPS_DDL/O_SLAM_PACKAGES/c76a0751-3c0c-43a6-b8ef-524c0a88dbb5/1/2018-12-28 00:00:00/1551299222452/1551299222452/SNAPSHOT/manifest.ion",
                            "InputFormat": "amazon.conexio.hive.EDXManifestHiveInputFormat",
                            "OutputFormat": "amazon.conexio.hive.EDXManifestHiveOutputFormat",
                            "Compressed": False,
                            "NumberOfBuckets": 0,
                            "SerdeInfo": {
                                "Name": "edx_serde",
                                "SerializationLibrary": "amazon.conexio.hive.serde.edx.GenericEDXSerDe",
                                "Parameters": {},
                            },
                            "SortColumns": [],
                            "StoredAsSubDirectories": False,
                        },
                        "Parameters": {},
                        "CatalogId": "111222333444",
                    }
                ],
                "NextToken": "eyJsYXN0RXZhbHVhdGVkS2V5Ijp7IkhBU0hfS0VZIjp7InMiOiJwLmU2NTUyZmUyYjdjZDQ5ODg5MTY1YjFhOTIzODFiYTBmLjAifSwiUkFOR0VfS0VZIjp7InMiOiIxLDIwMTgtMTItMjggMDA6MDA6MDAsMTU1MTI5OTIyMjQ1MiwxNTUxMjk5MjIyNDUyLFNOQVBTSE9UIn19LCJleHBpcmF0aW9uIjp7InNlY29uZHMiOjE2MzI0NTIyMTcsIm5hbm9zIjo4NDYwMDAwMDB9LCJwYWdlU2l6ZSI6MX0=",
                "ResponseMetadata": {
                    "RequestId": "58963277-47c8-4bc8-b3f0-746dfd229aa3",
                    "HTTPStatusCode": 200,
                    "HTTPHeaders": {
                        "date": "Thu, 23 Sep 2021 02:56:57 GMT",
                        "content-type": "application/x-amz-json-1.1",
                        "content-length": "1178",
                        "connection": "keep-alive",
                        "x-amzn-requestid": "58963277-47c8-4bc8-b3f0-746dfd229aa3",
                    },
                    "RetryAttempts": 0,
                },
            }
        elif DatabaseName == "searchdata" and TableName == "tommy_searches":
            return {
                "Partitions": [
                    {
                        "Values": ["AIWNA", "20191127"],
                        "DatabaseName": "searchdata",
                        "TableName": "tommy_searches",
                        "CreationTime": datetime.datetime(2019, 11, 28, 9, 9, 52, tzinfo=tzlocal()),
                        "StorageDescriptor": {
                            "Columns": [
                                {"Name": "gmt_time", "Type": "string"},
                                {"Name": "marketplace_id", "Type": "int"},
                                {"Name": "alias", "Type": "string"},
                                {"Name": "site_variant", "Type": "string"},
                                {"Name": "user_agent_info_type", "Type": "string"},
                                {"Name": "user_agent_id", "Type": "bigint"},
                                {"Name": "search_type", "Type": "string"},
                                {"Name": "is_spam", "Type": "int"},
                                {"Name": "is_trusted_customer", "Type": "int"},
                                {"Name": "is_trusted_user", "Type": "int"},
                                {"Name": "is_spam_or_untrusted", "Type": "int"},
                                {"Name": "session", "Type": "string"},
                                {"Name": "query_group_id", "Type": "string"},
                                {"Name": "action_id", "Type": "int"},
                                {"Name": "rank_function", "Type": "string"},
                                {"Name": "is_prime_customer", "Type": "int"},
                                {"Name": "customer_id", "Type": "string"},
                                {"Name": "total_found", "Type": "int"},
                                {"Name": "weblabs", "Type": "string"},
                                {"Name": "legal_entity_id", "Type": "int"},
                                {"Name": "total_displayed", "Type": "int"},
                                {"Name": "page", "Type": "int"},
                                {"Name": "browse_node", "Type": "string"},
                                {"Name": "ref_marker", "Type": "string"},
                                {"Name": "domain", "Type": "string"},
                                {"Name": "is_amazon_domain", "Type": "int"},
                                {"Name": "is_from_external_ad", "Type": "int"},
                                {"Name": "session_type", "Type": "int"},
                                {"Name": "is_recognized_session", "Type": "int"},
                                {"Name": "is_suspect_session", "Type": "int"},
                                {"Name": "is_editorial_query_group", "Type": "int"},
                                {"Name": "is_externally_referred_query_group", "Type": "int"},
                                {"Name": "user_agent_info_family", "Type": "string"},
                                {"Name": "page_type", "Type": "string"},
                                {"Name": "sub_page_type", "Type": "string"},
                                {"Name": "http_referer", "Type": "string"},
                                {"Name": "path_info", "Type": "string"},
                                {"Name": "query_string", "Type": "string"},
                                {"Name": "page_action", "Type": "string"},
                                {"Name": "hit_type", "Type": "string"},
                                {"Name": "application_name", "Type": "string"},
                                {"Name": "application_version", "Type": "string"},
                                {"Name": "operating_system_name", "Type": "string"},
                                {"Name": "operating_system_version", "Type": "string"},
                                {"Name": "device_type", "Type": "string"},
                                {"Name": "device_type_id", "Type": "string"},
                                {"Name": "secondary_search", "Type": "string"},
                                {"Name": "spelling_correction", "Type": "string"},
                                {"Name": "refinement_used", "Type": "string"},
                                {"Name": "original_path", "Type": "string"},
                                {"Name": "country_code", "Type": "string"},
                                {"Name": "action", "Type": "string"},
                                {"Name": "request_id", "Type": "string"},
                                {"Name": "record_type", "Type": "string"},
                                {"Name": "browse_node_name", "Type": "string"},
                                {"Name": "device_name", "Type": "string"},
                                {"Name": "qid", "Type": "string"},
                                {"Name": "search_source", "Type": "string"},
                                {"Name": "is_internal", "Type": "int"},
                                {"Name": "is_robot", "Type": "int"},
                                {"Name": "is_robot_ip", "Type": "int"},
                                {"Name": "client_ip", "Type": "string"},
                                {"Name": "spam_reason", "Type": "string"},
                                {"Name": "keywords", "Type": "string"},
                                {"Name": "is_business_customer", "Type": "int"},
                                {"Name": "program_region_id", "Type": "string"},
                                {"Name": "is_auto_spell_corrected", "Type": "int"},
                                {"Name": "iss_query_prefix", "Type": "string"},
                                {"Name": "iss_alias", "Type": "string"},
                                {"Name": "iss_clientside_latency", "Type": "int"},
                                {"Name": "date", "Type": "string"},
                                {"Name": "language", "Type": "string"},
                                {"Name": "sle_language_from", "Type": "string"},
                                {"Name": "sle_language_to", "Type": "string"},
                                {"Name": "sle_mlt_keywords", "Type": "string"},
                                {"Name": "server", "Type": "string"},
                                {"Name": "is_fresh_customer", "Type": "int"},
                                {"Name": "is_music_subscription_customer", "Type": "int"},
                                {"Name": "tommylabs", "Type": "string"},
                                {"Name": "is_spam_or_untrusted_lab", "Type": "string"},
                                {"Name": "ue_critical_feature", "Type": "bigint"},
                                {"Name": "ue_above_the_fold", "Type": "bigint"},
                                {"Name": "ue_page_loaded", "Type": "bigint"},
                                {"Name": "active_refinements", "Type": "string"},
                                {"Name": "active_refinement_count", "Type": "int"},
                                {"Name": "spam_reasons", "Type": "string"},
                                {"Name": "spam_reason_count", "Type": "string"},
                                {"Name": "has_search_results", "Type": "int"},
                                {"Name": "rr_first_click", "Type": "double"},
                                {"Name": "rr_first_add", "Type": "double"},
                                {"Name": "rr_first_purchase", "Type": "double"},
                                {"Name": "rr_first_consume", "Type": "double"},
                                {"Name": "first_clicked_asin", "Type": "string"},
                                {"Name": "first_added_asin", "Type": "string"},
                                {"Name": "first_click_depth", "Type": "int"},
                                {"Name": "clicks", "Type": "int"},
                                {"Name": "adds", "Type": "int"},
                                {"Name": "total_orders", "Type": "int"},
                                {"Name": "paid_orders", "Type": "int"},
                                {"Name": "free_orders", "Type": "int"},
                                {"Name": "total_units_ordered", "Type": "int"},
                                {"Name": "paid_units_ordered", "Type": "int"},
                                {"Name": "free_units_ordered", "Type": "int"},
                                {"Name": "max_purchase_price", "Type": "double"},
                                {"Name": "ops", "Type": "double"},
                                {"Name": "search_clicked", "Type": "int"},
                                {"Name": "search_added", "Type": "int"},
                                {"Name": "search_purchased", "Type": "int"},
                                {"Name": "search_consumed", "Type": "int"},
                                {"Name": "seconds_until_first_add", "Type": "int"},
                                {"Name": "first_purchased_asin", "Type": "string"},
                                {"Name": "borrows", "Type": "int"},
                                {"Name": "search_borrowed", "Type": "int"},
                                {"Name": "query_length", "Type": "int"},
                                {"Name": "secondary_search_spelling_correction_offered", "Type": "int"},
                                {"Name": "secondary_search_spelling_correction_explicitly_accepted", "Type": "int"},
                                {"Name": "primary_spelling_correction_offered", "Type": "int"},
                                {"Name": "primary_spelling_correction_explicitly_accepted", "Type": "int"},
                                {"Name": "secondary_search_spelling_correction_implicitly_accepted", "Type": "int"},
                                {"Name": "primary_spelling_correction_implicitly_accepted", "Type": "int"},
                                {"Name": "is_first_clicked_search_of_query_group", "Type": "int"},
                                {"Name": "is_first_added_search_of_query_group", "Type": "int"},
                                {"Name": "is_first_purchased_search_of_query_group", "Type": "int"},
                                {"Name": "is_first_consumed_search_of_query_group", "Type": "int"},
                                {"Name": "is_first_search_of_query_group", "Type": "int"},
                                {"Name": "sparkle_clicks", "Type": "int"},
                                {"Name": "sparkle_clicked", "Type": "int"},
                                {"Name": "clicks_to_asin_page", "Type": "int"},
                                {"Name": "clicks_to_discovery_page", "Type": "int"},
                                {"Name": "engaged", "Type": "int"},
                                {"Name": "keyword_reformulated", "Type": "int"},
                                {"Name": "browse_reformulated", "Type": "int"},
                                {"Name": "first_hit_slot_name", "Type": "string"},
                                {"Name": "first_hit_amabot_placement_id", "Type": "string"},
                                {"Name": "first_hit_amabot_creative_id", "Type": "string"},
                                {"Name": "slot_names", "Type": "string"},
                                {"Name": "signpost_clicks", "Type": "int"},
                                {"Name": "signpost_clicked", "Type": "int"},
                                {"Name": "whole_page_destination_clicks", "Type": "int"},
                                {"Name": "whole_page_exploration_actions", "Type": "int"},
                                {"Name": "whole_page_no_action", "Type": "int"},
                                {"Name": "next_ref_marker", "Type": "string"},
                                {"Name": "next_request_id", "Type": "string"},
                                {"Name": "next_alias", "Type": "string"},
                                {"Name": "next_keywords", "Type": "string"},
                                {"Name": "next_browse_node", "Type": "string"},
                                {"Name": "next_active_refinements", "Type": "string"},
                                {"Name": "next_refinement_used", "Type": "string"},
                                {"Name": "next_page", "Type": "int"},
                                {"Name": "next_rank_function", "Type": "string"},
                                {"Name": "next_asin", "Type": "string"},
                                {"Name": "next_http_referer", "Type": "string"},
                                {"Name": "outgoing_transition_type", "Type": "string"},
                                {"Name": "prev_request_id", "Type": "string"},
                                {"Name": "prev_alias", "Type": "string"},
                                {"Name": "prev_keywords", "Type": "string"},
                                {"Name": "prev_browse_node", "Type": "string"},
                                {"Name": "prev_active_refinements", "Type": "string"},
                                {"Name": "prev_refinement_used", "Type": "string"},
                                {"Name": "prev_page", "Type": "int"},
                                {"Name": "prev_rank_function", "Type": "string"},
                                {"Name": "prev_asin", "Type": "string"},
                                {"Name": "incoming_transition_type", "Type": "string"},
                                {"Name": "next_page_type", "Type": "string"},
                                {"Name": "next_sub_page_type", "Type": "string"},
                                {"Name": "prev_page_type", "Type": "string"},
                                {"Name": "prev_sub_page_type", "Type": "string"},
                                {"Name": "exit", "Type": "int"},
                                {"Name": "transit_ops_indicator", "Type": "int"},
                                {"Name": "transit_order_indicator", "Type": "int"},
                                {"Name": "transit_total_ops", "Type": "double"},
                                {"Name": "downstream_total_ops", "Type": "double"},
                                {"Name": "is_first_browse_hit_on_this_browse_node", "Type": "int"},
                                {"Name": "is_first_hit_with_this_keyword", "Type": "int"},
                                {"Name": "ops_indicator", "Type": "int"},
                                {"Name": "order_indicator", "Type": "int"},
                                {"Name": "search_result_lists", "Type": "string"},
                                {"Name": "srp_widget_ids", "Type": "string"},
                                {"Name": "search_result_list_count", "Type": "string"},
                                {"Name": "first_click_list_name", "Type": "string"},
                                {"Name": "sponsored_result_click_count", "Type": "int"},
                                {"Name": "sponsored_result_add_count", "Type": "int"},
                                {"Name": "sponsored_result_order_count", "Type": "int"},
                                {"Name": "sponsored_result_impression_count", "Type": "int"},
                                {"Name": "sponsored_result_ops", "Type": "double"},
                                {"Name": "organic_result_click_count", "Type": "int"},
                                {"Name": "organic_result_add_count", "Type": "int"},
                                {"Name": "organic_result_order_count", "Type": "int"},
                                {"Name": "organic_result_impression_count", "Type": "int"},
                                {"Name": "organic_result_ops", "Type": "double"},
                                {"Name": "sparkle_ad_impression_count", "Type": "int"},
                                {"Name": "total_impression_count", "Type": "int"},
                                {"Name": "amabot_impression_slot_names", "Type": "string"},
                                {"Name": "amabot_impression_creative_ids", "Type": "string"},
                                {"Name": "amabot_impression_slot_count", "Type": "int"},
                                {"Name": "auto_scoped", "Type": "int"},
                                {"Name": "scoping_node", "Type": "string"},
                                {"Name": "active_refinement_pickers", "Type": "string"},
                                {"Name": "refinement_pickers_shown", "Type": "string"},
                                {"Name": "cross_session_order_count", "Type": "int"},
                                {"Name": "paid_cross_session_order_count", "Type": "int"},
                                {"Name": "free_cross_session_order_count", "Type": "int"},
                                {"Name": "cross_session_units_ordered", "Type": "int"},
                                {"Name": "paid_cross_session_units_ordered", "Type": "int"},
                                {"Name": "free_cross_session_units_ordered", "Type": "int"},
                                {"Name": "cross_session_ops", "Type": "double"},
                                {"Name": "iss_explicit_acceptance", "Type": "int"},
                                {"Name": "iss_explicit_acceptance_position", "Type": "int"},
                                {"Name": "iss_explicit_accept_on_word_break", "Type": "int"},
                                {"Name": "iss_explicit_category_acceptance", "Type": "int"},
                                {"Name": "iss_implicit_acceptance", "Type": "int"},
                                {"Name": "iss_query_prefix_length", "Type": "int"},
                                {"Name": "iss_query_prefix_tokens", "Type": "int"},
                                {"Name": "down_session_100_minute_overall_search_count", "Type": "int"},
                                {"Name": "down_session_100_minute_overall_click_count", "Type": "int"},
                                {"Name": "down_session_100_minute_overall_add_count", "Type": "int"},
                                {"Name": "down_session_100_minute_overall_paid_unit_count", "Type": "int"},
                                {"Name": "down_session_100_minute_overall_ops", "Type": "double"},
                                {"Name": "down_session_100_minute_sa_search_count", "Type": "int"},
                                {"Name": "down_session_100_minute_sa_click_count", "Type": "int"},
                                {"Name": "down_session_100_minute_sa_add_count", "Type": "int"},
                                {"Name": "down_session_100_minute_sa_paid_unit_count", "Type": "int"},
                                {"Name": "down_session_100_minute_sa_ops", "Type": "double"},
                                {"Name": "is_fresh_store", "Type": "int"},
                                {"Name": "post_tp_keywords", "Type": "string"},
                                {"Name": "post_tp_phrasedoc_keywords", "Type": "string"},
                                {"Name": "customer_delivery_address", "Type": "string"},
                                {"Name": "spotcp_value", "Type": "double"},
                                {"Name": "spotcp_currency_code", "Type": "string"},
                                {"Name": "transit_first_record_request_id", "Type": "string"},
                                {"Name": "lkci", "Type": "string"},
                                {"Name": "down_session_100_minute_overall_paid_purchase_count", "Type": "int"},
                                {"Name": "is_optin_auto_correction", "Type": "int"},
                                {"Name": "is_suspected_inorganic_search", "Type": "int"},
                                {"Name": "down_session_100_minute_sa_purchase_count", "Type": "int"},
                                {"Name": "down_session_100_minute_sa_purchased_asins", "Type": "string"},
                                {"Name": "is_optout_auto_correction", "Type": "int"},
                                {"Name": "is_optin_offer_suggestion", "Type": "int"},
                                {"Name": "is_offer_spelling_correction", "Type": "int"},
                                {"Name": "down_session_100_minute_overall_purchase_count", "Type": "int"},
                                {"Name": "spelling_action", "Type": "string"},
                                {"Name": "down_session_100_minute_overall_purchased_asins", "Type": "string"},
                            ],
                            "Location": "s3://tommy-datasets-test/tommy-searches-parquet/aiwna/20191127",
                            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                            "Compressed": False,
                            "NumberOfBuckets": 0,
                            "SerdeInfo": {
                                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                                "Parameters": {"serialization.format": "1"},
                            },
                            "BucketColumns": [],
                            "SortColumns": [],
                            "StoredAsSubDirectories": False,
                        },
                        "Parameters": {"EXTERNAL": "TRUE", "has_encrypted_data": "true"},
                        "CatalogId": "111222333444",
                    }
                ],
                "NextToken": "eyJsYXN0RXZhbHVhdGVkS2V5Ijp7IkhBU0hfS0VZIjp7InMiOiJwLmY1MThkMDI2N2Y5NjQxYmFhM2Y3ZDlhZjRkOWEzY2I2LjAifSwiUkFOR0VfS0VZIjp7InMiOiJBSVdOQSwyMDE5MTEyNyJ9fSwiZXhwaXJhdGlvbiI6eyJzZWNvbmRzIjoxNjMyNDUyNDY0LCJuYW5vcyI6ODc1MDAwMDAwfSwicGFnZVNpemUiOjF9",
                "ResponseMetadata": {
                    "RequestId": "cbdad626-4387-44a0-903c-d95dbc278aca",
                    "HTTPStatusCode": 200,
                    "HTTPHeaders": {
                        "date": "Thu, 23 Sep 2021 03:01:04 GMT",
                        "content-type": "application/x-amz-json-1.1",
                        "content-length": "12236",
                        "connection": "keep-alive",
                        "x-amzn-requestid": "cbdad626-4387-44a0-903c-d95dbc278aca",
                    },
                    "RetryAttempts": 0,
                },
            }
        elif DatabaseName == "dexbi" and TableName == "d_ship_option":
            return {
                "Partitions": [
                    {
                        "Values": ["1623655171208", "1623655171208", "SNAPSHOT"],
                        "DatabaseName": "dexbi",
                        "TableName": "d_ship_option",
                        "CreationTime": datetime.datetime(2021, 6, 14, 7, 25, 35, tzinfo=tzlocal()),
                        "LastAccessTime": datetime.datetime(2021, 6, 14, 7, 25, 35, tzinfo=tzlocal()),
                        "StorageDescriptor": {
                            "Columns": [],
                            "Location": "s3://foo-subscriptions/111222333444/DEXBI/D_SHIP_OPTION/D_SHIP_OPTION/1623655171208/1623655171208/SNAPSHOT/manifest.ion",
                            "InputFormat": "amazon.conexio.hive.EDXManifestHiveInputFormat",
                            "OutputFormat": "amazon.conexio.hive.EDXManifestHiveOutputFormat",
                            "Compressed": False,
                            "NumberOfBuckets": 0,
                            "SerdeInfo": {
                                "Name": "edx_serde",
                                "SerializationLibrary": "amazon.conexio.hive.serde.edx.GenericEDXSerDe",
                                "Parameters": {},
                            },
                            "SortColumns": [],
                            "StoredAsSubDirectories": False,
                        },
                        "Parameters": {},
                        "CatalogId": "111222333444",
                    }
                ],
                "NextToken": "eyJsYXN0RXZhbHVhdGVkS2V5Ijp7IkhBU0hfS0VZIjp7InMiOiJwLjUzZmMxZWNjODRkNzQwMjJhYjJkNThkYjFhYmMwODVjLjMifSwiUkFOR0VfS0VZIjp7InMiOiIxNjIzNjU1MTcxMjA4LDE2MjM2NTUxNzEyMDgsU05BUFNIT1QifX0sImV4cGlyYXRpb24iOnsic2Vjb25kcyI6MTYzMzY2NTkxMiwibmFub3MiOjI4MDAwMDAwfSwicGFnZVNpemUiOjF9",
                "ResponseMetadata": {
                    "RequestId": "13c3c6c9-e27f-4083-8cfb-b4a0841ebc7d",
                    "HTTPStatusCode": 200,
                    "HTTPHeaders": {
                        "date": "Thu, 07 Oct 2021 04:05:12 GMT",
                        "content-type": "application/x-amz-json-1.1",
                        "content-length": "1061",
                        "connection": "keep-alive",
                        "x-amzn-requestid": "13c3c6c9-e27f-4083-8cfb-b4a0841ebc7d",
                    },
                    "RetryAttempts": 0,
                },
            }
        else:
            return {}

    def get_partition(self, DatabaseName: str, TableName: str, PartitionValues: List[str]) -> Dict[str, Any]:
        partition = self.get_partitions(DatabaseName, TableName, 1)["Partitions"][0]
        return {"Partition": partition} if partition["Values"] == PartitionValues else None

    def get_tables(self, DatabaseName: str) -> Dict[str, Any]:
        if DatabaseName == "booker":
            return {"TableList": [{"Name": "d_unified_cust_shipment_items"}, {"Name": "d_unified_cust_shipment_items_PARQUET"}]}
        elif DatabaseName == "dex_ml_catalog":
            return {"TableList": [{"Name": "d_ad_orders_na"}]}
        elif DatabaseName == "atrops_ddl":
            return {"TableList": [{"Name": "o_slam_packages"}]}
        elif DatabaseName == "searchdata":
            return {"TableList": [{"Name": "tommy_searches"}]}
        elif DatabaseName == "dexbi":
            return {"TableList": [{"Name": "d_ship_option"}]}
        else:
            return {}

    def test_application_external_data_glue_table_1(self):
        self.patch_aws_start(glue_catalog_has_all_tables=False, glue_catalog=self)

        app = AWSApplication("andes-test", self.region)

        ducsi_data = app.marshal_external_data(external_data_desc=GlueTable("booker", "d_unified_cust_shipment_items"))
        assert ducsi_data.bound.data_id == "d_unified_cust_shipment_items"

        ducsi_data_explicit_import = app.marshal_external_data(
            GlueTable("booker", "d_unified_cust_shipment_items", partition_keys=["region_id", "ship_day"]),
            "DEXML_DUCSI2",
            {"region_id": {"type": DimensionType.LONG, "ship_day": {"format": "%Y-%m-%d", "type": DimensionType.DATETIME}}},
            {
                "1": {
                    "*": {
                        "timezone": "PST",
                    }
                }
            },
            SignalIntegrityProtocol("FILE_CHECK", {"file": ["SNAPSHOT"]}),
        )
        assert ducsi_data_explicit_import.bound.data_id == "DEXML_DUCSI2"

        try:
            app.marshal_external_data(GlueTable("dex_ml_catalog", "d_ad_orders_na", partition_keys=[]))
            assert False, "Fail due to empty partition keys (the table actually has one: 'order_day')"
        except ValueError:
            pass

        try:
            app.marshal_external_data(GlueTable("dex_ml_catalog", "d_ad_orders_na", partition_keys=["WRONGorder_day"]))
            assert False, "Fail due to wrong partition key"
        except ValueError:
            pass

        try:
            app.marshal_external_data(GlueTable("dex_ml_catalog", "d_ad_orders_na", primary_keys=["WRONG_customer_id"]))
            assert False, "Fail due to wrong primary key"
        except ValueError:
            pass

        d_ad_orders_na = app.marshal_external_data(GlueTable("dex_ml_catalog", "d_ad_orders_na"))

        # import a table type table with explicit decl (for documentation, strictly-
        d_ad_orders_na_explicit_decl = app.marshal_external_data(
            GlueTable("dex_ml_catalog", "d_ad_orders_na", partition_keys=["order_day"]),
            "d_ad_orders_na2",
            dimension_spec={"order_day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d", "timezone": "PST"}},
            dimension_filter={"*": {}},
        )

        # EDGE-CASES
        # SHOULD SUCCEED
        app.marshal_external_data(
            GlueTable("dexbi", "d_ship_option", partition_keys=[], **{"metadata_field_1": "value"}),
            "ship_options",
            {},
            {},
            SignalIntegrityProtocol("FILE_CHECK", {"file": ["DELTA", "SNAPSHOT"]}),
        )

        # BEGIN ETLs
        repeat_ducsi = app.create_data(
            id="REPEAT_DUCSI", inputs={"DEXML_DUCSI": ducsi_data}, compute_targets="output=DEXML_DUCSI.limit(100)"
        )

        repeat_d_ad_orders_na = app.create_data(
            id="REPEAT_AD_ORDERS",
            inputs=[d_ad_orders_na],
            compute_targets=[
                BatchCompute(
                    "output=d_ad_orders_na.limit(100)", WorkerType=GlueWorkerType.G_1X.value, NumberOfWorkers=50, GlueVersion="2.0"
                )
            ],
        )

        ducsi_with_AD_orders_NA = app.create_data(
            id="DUCSI_WITH_AD_ORDERS_NA",
            inputs=[
                d_ad_orders_na,
                # keep it as the first input to make this node adapt its dimension spec (due to default behaviour in AWSApplication::create_data)
                ducsi_data["1"]["*"],
            ],
            input_dim_links=[(ducsi_data("ship_day"), lambda dim: dim, d_ad_orders_na("order_day"))],
            compute_targets=[
                BatchCompute(
                    """
output=DEXML_DUCSI.limit(100).join(d_ad_orders_na.limit(100), ['customer_id']).limit(10).drop(*('customer_id', 'order_day'))
                    """,
                    GlueVersion="2.0",
                    WorkerType=GlueWorkerType.G_1X.value,
                    NumberOfWorkers=100,
                    Timeout=3 * 60,  # 3 hours
                )
            ],
        )

        # experiment early (warning: will activate the application with the nodes added so far).
        # materialized_output_path = app.execute(repeat_ducsi, ducsi_data[1]['2020-12-01'])

        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        app._dev_context = dev_context
        #

        app.activate(allow_concurrent_executions=False)

        self.patch_aws_stop()

    def test_application_external_data_glue_table_dependency_check(self):
        self.patch_aws_start(glue_catalog_has_all_tables=False, glue_catalog=self)

        app = AWSApplication("andes-test", self.region)

        ducsi_data = app.marshal_external_data(external_data_desc=GlueTable("booker", "d_unified_cust_shipment_items"))

        d_ad_orders_na = app.marshal_external_data(GlueTable("dex_ml_catalog", "d_ad_orders_na"))

        # 1- now setup nodes to test dependency check on glue table data
        #    use glue table as a reference table (no interest in its incoming events) but enable range
        #    check to make sure that the execution will happen once its range (all of the partitions) are ready.
        ducsi_with_ad_orders = app.create_data(
            id="DUCSI_with_AD_ORDERS",
            inputs=[ducsi_data, d_ad_orders_na.ref.range_check(True)],  # only one partition
            compute_targets=[NOOPCompute],
            input_dim_links=[(ducsi_data("ship_day"), EQUALS, d_ad_orders_na("order_day"))],
        )

        ducsi_with_ad_orders_2 = app.create_data(
            id="DUCSI_with_AD_ORDERS_2",
            inputs=[ducsi_data, d_ad_orders_na[:-2].ref.range_check(True)],  # two partitions
            compute_targets=[NOOPCompute],
            input_dim_links=[(ducsi_data("ship_day"), EQUALS, d_ad_orders_na("order_day"))],
        )

        app.activate(allow_concurrent_executions=False)

        # 3- Inject and Execute
        # from get_partition impl above it is been configured that
        # d_ad_orders_na has 2021-05-09 00:00:00 partition
        # INJECT DUCSI event into the system (emulate runtime behaviour)
        app.process(ducsi_data[1]["2021-05-09"])
        # so it must have caused an execution
        path, _ = app.poll(ducsi_with_ad_orders[1]["2021-05-09"])
        assert path

        # there should not be an execution on the second node since it uses two partitions but 2021-05-08 does not exist in the system
        path, _ = app.poll(ducsi_with_ad_orders_2[1]["2021-05-09"])
        assert not path

        # use a date (tomorrow) that does not have d_ad_orders_na data
        app.process(ducsi_data[1]["2021-05-10"])
        path, _ = app.poll(ducsi_with_ad_orders[1]["2021-05-10"])
        # no exeuctions! due to missing d_ad_orders_na
        assert not path

        # NOW EMULATE generation of 2021-05-08 data on d_ad_orders_na by patching get_partition
        get_partition_ORG = self.get_partition

        def get_partition(DatabaseName: str, TableName: str, PartitionValues: List[str]) -> Dict[str, Any]:
            if DatabaseName == "dex_ml_catalog" and TableName == "d_ad_orders_na":
                if PartitionValues in [["2021-05-08 00:00:00"], ["2021-05-09 00:00:00"]]:
                    return {"Partition": {"Values": PartitionValues}}

        self.get_partition = get_partition

        # cause orhestration to do 'next cycle' locally (will check pending nodes and do range analysis)
        app.update_active_routes_status()

        # now this should succeed because '2021-05-08 00:00:00' partition is ready
        path, _ = app.poll(ducsi_with_ad_orders_2[1]["2021-05-09"])
        assert path

        # and now emulate a scenario where '2021-05-10' partition event on d_ad_orders_na is received
        # even before range analysis detects it. This will satisfy range analysis for trigger group on 2021-05-10
        # created above after injecting ducsi (first input) via app.process(ducsi_data[1]['2021-05-10'])
        app.process(d_ad_orders_na["2021-05-10"])
        path, _ = app.poll(ducsi_with_ad_orders[1]["2021-05-10"])
        assert path
        path, _ = app.poll(ducsi_with_ad_orders_2[1]["2021-05-10"])
        assert path
        # end 3

        # 4- Reset the application topology and do the test by swapping inputs: making parquet version of DUCSI data
        # as a reference input. application will have one node only for this simple/final case
        app = AWSApplication("andes-test", self.region)
        #  dimensions/partition keys: 'region_id' (LONG), 'order_day' (TIMESTAMP)
        ducsi_data_PARQUET = app.glue_table("booker", "d_unified_cust_shipment_items_PARQUET")

        #    This case aims to cover dependency check on multiple dimensions one of which is LONG (non-string: to
        #    capture the edge-case of coverting everything to Glue catalog expected partition value type string).
        ad_orders_with_ducsi_reference = app.create_data(
            id="AD_ORDERS_with_DUCSI",
            inputs=[d_ad_orders_na, ducsi_data_PARQUET[1]["*"].ref.range_check(True)],
            compute_targets=[NOOPCompute],
        )
        app.activate()

        app.process(d_ad_orders_na["2021-05-20"])
        path, _ = app.poll(ad_orders_with_ducsi_reference["2021-05-20"])
        # must NOT have executed since the most recent version of get_partition yields None for any ducsi query
        assert not path

        # now modify get_partition to serve requests for ducsi
        def get_partition(DatabaseName: str, TableName: str, PartitionValues: List[str]) -> Dict[str, Any]:
            if DatabaseName == "booker" and TableName == "d_unified_cust_shipment_items_PARQUET":
                # please note that 1 -> '1' is also tested to conform with glue:get_partition. normally that partition
                # value is LONG (and in Python int type) internally within the framework.
                if PartitionValues in [["1", "2021-05-20 00:00:00"]]:
                    return {"Partition": {"Values": PartitionValues}}

        self.get_partition = get_partition

        # cause orhestration to do 'next cycle' locally (will check pending nodes and do range analysis)
        app.update_active_routes_status()

        # should succeed now thanks to dependency check
        path, _ = app.poll(ad_orders_with_ducsi_reference["2021-05-20"])
        assert path
        # end 4

        # restore
        self.get_partition = get_partition_ORG

        self.patch_aws_stop()

    def test_application_external_data_glue_table_2(self):
        self.patch_aws_start(glue_catalog_has_all_tables=False, glue_catalog=self)

        app = AWSApplication("gluetable-test", self.region)

        try:
            app.marshal_external_data(GlueTable("dex_ml_catalog", "d_ad_orders_na", partition_keys=[]))
            assert False, "Fail due to empty partition keys (the table actually has one: 'order_day')"
        except ValueError:
            pass

        atrops_o_slam_packages = app.marshal_external_data(
            GlueTable(database="atrops_ddl", table_name="o_slam_packages", table_type="REPLACE")
        )
        # RheocerOS internal node_id is 'table_name' by default (since 'id' param to marshal_external_data is left None).
        assert atrops_o_slam_packages.bound.data_id == "o_slam_packages"

        encryption_key = "arn:aws:kms:us-east-1:111222333444:key/aaaaaaaa-bbbb-cccc-dddd-1112223334ab"

        # will fail due to missing encryption_key
        try:
            app.marshal_external_data(
                external_data_desc=GlueTable(database="searchdata", table_name="tommy_searches"), id="my_tommy_searches"
            )
            assert False, "shoul fail due to missing 'encryption_key'"
        except ValueError:
            pass

        tommy_searches = app.marshal_external_data(
            external_data_desc=GlueTable(database="searchdata", table_name="tommy_searches", encryption_key=encryption_key),
            id="my_tommy_searches",
        )

        assert tommy_searches.bound.data_id == "my_tommy_searches"

        tommy_searches = app.marshal_external_data(
            # always checks the catalog
            external_data_desc=GlueTable(database="searchdata", table_name="tommy_searches", encryption_key=encryption_key),
            dimension_filter={  # carries the type info as well (partition names will be from catalog)
                "*": {
                    "type": DimensionType.STRING,
                    "format": lambda dim: dim.lower(),
                    # their Glue events and S3 path uses org partition in a case insensitive way (declare here so that IF knows about it).
                    "insensitive": True,
                    "*": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d"},  # internally convert search partition format %Y%m%d
                }
            },
        )

        tommy_searches2 = app.marshal_external_data(
            # always checks the catalog
            external_data_desc=GlueTable(database="searchdata", table_name="tommy_searches", encryption_key=encryption_key),
            id="tommy_searches2",
            dimension_filter={  # missing first partition name ('org') will be fetched from catalog
                "*": {
                    "type": DimensionType.STRING,
                    "format": lambda dim: dim.lower(),
                    # their Glue events and S3 path uses org partition in a case insensitive way (declare here so that IF knows about it).
                    "insensitive": True,
                    "*": {
                        "name": "part_date",
                        # catalog has this as 'partition_date' but user's choice will be honored here
                        "type": DimensionType.DATETIME,
                        "format": "%Y-%m-%d",  # internally convert search partition format %Y%m%d
                    },
                }
            },
        )

        try:
            app.marshal_external_data(
                # always checks the catalog
                external_data_desc=GlueTable(database="searchdata", table_name="tommy_searches", encryption_key=encryption_key),
                id="tommy_searches3",
                dimension_spec={  # missing first partition type (STRING) will be fetched from catalog
                    "org": {
                        "part_date": {
                            "type": DimensionType.DATETIME,
                            # this will be honored (catalog as this dimension as STRING)
                            "format": "%Y-%m-%d",
                        }
                    }
                },
            )
            assert False, "should fail due to missing 'type' for 'org' dimension!"
        except KeyError:
            pass

        # this dataset is important, since Glue Table proxy should be used at runtime. publisher adds new partitions
        tommy_searches4 = app.marshal_external_data(
            # always checks the catalog
            external_data_desc=GlueTable(database="searchdata", table_name="tommy_searches", encryption_key=encryption_key),
            id="tommy_searches4",
            dimension_spec={
                "org": {
                    "type": DimensionType.STRING,
                    "format": lambda dim: dim.lower(),
                    # their Glue events and S3 path uses org partition in a case insensitive way (declare here so that IF knows about it).
                    "insensitive": True,
                    "partition_date": {
                        "type": DimensionType.DATETIME,
                        "format": "%Y-%m-%d",  # internally convert search partition format %Y%m%d
                    },
                }
            },
        )

        ducsi_data = app.marshal_external_data(
            # checks the catalog and tries to do validation and compensate missing data.
            # different than GlueTable, will be OK if everything is given and also domainspec is there (dim spec).
            external_data_desc=GlueTable(
                "booker",
                "d_unified_cust_shipment_items",
            ),
            id="DEXML_DUCSI",
            # dimension spec will be auto-generated based on the names and types of partition keys from the catalog
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": ["SNAPSHOT"]}),
        )

        d_ad_orders_na = app.marshal_external_data(
            external_data_desc=GlueTable("dex_ml_catalog", "d_ad_orders_na")
            # this spec will be auto-generated using info from Glue catalog
            # {
            #    'order_day': {
            #        'type': DimensionType.DATETIME,
            #    }
            # },
        )

        tommy_searches_filtered = app.create_data(
            id="TOMMY_SEARCHES_FILTERED",
            inputs=[tommy_searches],
            compute_targets=[
                BatchCompute(
                    "output = tommy_searches.limit(5)", WorkerType=GlueWorkerType.G_1X.value, NumberOfWorkers=20, GlueVersion="2.0"
                )
            ],
        )

        repeat_d_ad_orders_na = app.create_data(
            id="REPEAT_AD_ORDERS",
            inputs=[d_ad_orders_na],
            compute_targets=[
                BatchCompute(
                    "output=d_ad_orders_na.limit(100)", WorkerType=GlueWorkerType.G_1X.value, NumberOfWorkers=50, GlueVersion="2.0"
                )
            ],
        )

        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        app._dev_context = dev_context
        #

        app.activate()

        self.patch_aws_stop()

    def test_application_external_data_glue_table_with_convenience_api(self):
        self.patch_aws_start(glue_catalog_has_all_tables=False, glue_catalog=self)

        app = AWSApplication("gluetable-test", self.region)

        atrops_o_slam_packages = app.glue_table("atrops_ddl", "o_slam_packages")
        # RheocerOS internal node_id is 'table_name' by default (since 'id' param to marshal_external_data is left None).
        assert atrops_o_slam_packages.bound.data_id == "o_slam_packages"

        encryption_key = "arn:aws:kms:us-east-1:111122223333:key/aaaaaaaa-bbbb-cccc-dddd-cccafabddeee"

        # will fail due to missing encryption_key
        try:
            app.glue_table(database="searchdata", table_name="tommy_searches", id="my_tommy_searches")
            assert False, "shoul fail due to missing 'encryption_key'"
        except ValueError:
            pass

        tommy_searches = app.glue_table(
            database="searchdata", table_name="tommy_searches", encryption_key=encryption_key, id="my_tommy_searches"
        )

        assert tommy_searches.bound.data_id == "my_tommy_searches"

        tommy_searches = app.glue_table(
            database="searchdata",
            table_name="tommy_searches",
            encryption_key=encryption_key,
            dimension_filter={  # carries the type info as well (partition names will be from catalog)
                "*": {
                    "type": DimensionType.STRING,
                    "format": lambda dim: dim.lower(),
                    # their Glue events and S3 path uses org partition in a case insensitive way (declare here so that IF knows about it).
                    "insensitive": True,
                    "*": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d"},  # internally convert search partition format %Y%m%d
                }
            },
        )

        tommy_searches2 = app.glue_table(
            database="searchdata",
            table_name="tommy_searches",
            encryption_key=encryption_key,
            id="tommy_searches2",
            dimension_filter={  # missing first partition name ('org') will be fetched from catalog
                "*": {
                    "type": DimensionType.STRING,
                    "format": lambda dim: dim.lower(),
                    # their Glue events and S3 path uses org partition in a case insensitive way (declare here so that IF knows about it).
                    "insensitive": True,
                    "*": {
                        "name": "part_date",
                        # catalog has this as 'partition_date' but user's choice will be honored here
                        "type": DimensionType.DATETIME,
                        "format": "%Y-%m-%d",  # internally convert search partition format %Y%m%d
                    },
                }
            },
        )

        try:
            app.glue_table(
                database="searchdata",
                table_name="tommy_searches",
                encryption_key=encryption_key,
                id="tommy_searches3",
                dimension_spec={  # missing first partition type (STRING) will be fetched from catalog
                    "org": {
                        "part_date": {
                            "type": DimensionType.DATETIME,
                            # this will be honored (catalog as this dimension as STRING)
                            "format": "%Y-%m-%d",
                        }
                    }
                },
            )
            assert False, "should fail due to missing 'type' for 'org' dimension!"
        except KeyError:
            pass

        # this dataset is important, since Glue Table proxy should be used at runtime. publisher adds new partitions
        tommy_searches4 = app.glue_table(
            database="searchdata",
            table_name="tommy_searches",
            encryption_key=encryption_key,
            id="tommy_searches4",
            dimension_spec={
                "org": {
                    "type": DimensionType.STRING,
                    "format": lambda dim: dim.lower(),
                    # their Glue events and S3 path uses org partition in a case insensitive way (declare here so that IF knows about it).
                    "insensitive": True,
                    "partition_date": {
                        "type": DimensionType.DATETIME,
                        "format": "%Y-%m-%d",  # internally convert search partition format %Y%m%d
                    },
                }
            },
        )

        ducsi_data = app.glue_table(
            # checks the catalog and tries to do validation and compensate missing data.
            # different than GlueTable, will be OK if everything is given and also domainspec is there (dim spec).
            "booker",
            "d_unified_cust_shipment_items",
            # primary_keys will be auto-retrieved from the catalog
            # primary_keys=['customer_order_item_id', 'customer_shipment_item_id'],
            table_type="APPEND",
            id="DEXML_DUCSI",
            # dimension spec will be auto-generated based on the names and types of partition keys from the catalog
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": ["SNAPSHOT"]}),
        )

        d_ad_orders_na = app.glue_table(
            "dex_ml_catalog",
            "d_ad_orders_na"
            # this spec will be auto-generated using info from Glue catalog
            # {
            #    'order_day': {
            #        'type': DimensionType.DATETIME,
            #    }
            # },
        )

        tommy_searches_filtered = app.create_data(
            id="TOMMY_SEARCHES_FILTERED",
            inputs=[tommy_searches],
            compute_targets=[
                BatchCompute(
                    "output = tommy_searches.limit(5)", WorkerType=GlueWorkerType.G_1X.value, NumberOfWorkers=20, GlueVersion="2.0"
                )
            ],
        )

        repeat_d_ad_orders_na = app.create_data(
            id="REPEAT_AD_ORDERS",
            inputs=[d_ad_orders_na],
            compute_targets=[
                BatchCompute(
                    "output=d_ad_orders_na.limit(100)", WorkerType=GlueWorkerType.G_1X.value, NumberOfWorkers=50, GlueVersion="2.0"
                )
            ],
        )

        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        app._dev_context = dev_context
        #

        app.activate()

        self.patch_aws_stop()

    def test_application_external_data_s3(self):
        self.patch_aws_start(
            glue_catalog_has_all_tables=False,
            # explicitly set to None to check if catalog use is somehow in question, it should not!
            glue_catalog=None,
        )

        app = AWSApplication("s3-test", self.region)

        try:
            app.marshal_external_data(
                S3Dataset(
                    "427809481713",
                    "if-adpd-training-427809481713-us-east-1",
                    "datanet_data/transit_time_map",
                    dataset_format=DataFormat.CSV,
                    delimiter="|",
                ),
                "transit_time_map_2020",
            )
            assert False, "should fail due to missing dimension_spec"
        except ValueError:
            pass

        # show that import will succeed, but we won't use this external data in this test
        app.marshal_external_data(
            S3Dataset(
                "427809481713",
                "if-adpd-training-427809481713-us-east-1",
                "datanet_data/transit_time_map",
                dataset_format=DataFormat.CSV,
                delimiter="|",
            ),
            "transit_time_map_2020",
            dimension_spec={},  # no partitions/dimensions
        )

        # Import the following external, raw datasource
        # Example partition path:
        # https://s3.console.aws.amazon.com/s3/buckets/adpd-prod?region=us-east-1&prefix=adpds-output-data/1/2021/09/15/&showversions=false
        adpd_shadow_json_AS_SIGNAL = app.marshal_external_data(
            external_data_desc=S3Dataset(
                "427809481713", "adpd-prod", "adpds-output-data", "{}", "{}", "{}", "{}", dataset_format=DataFormat.JSON
            )
            # in spark code, this will be the default DataFrame name/alias (but you can overwrite it in 'inputs' if you use map instead of list)
            ,
            id="adpd_shadow_data",
            dimension_spec={
                "region": {
                    "type": DimensionType.LONG,  # 1, 3 , 5
                    "year": {
                        "type": DimensionType.DATETIME,
                        "granularity": DatetimeGranularity.YEAR,
                        # important if you do range operations in 'inputs' (like read multiple years)
                        "format": "%Y",  # year, remember from %Y%m%d, this should output 2021
                        "month": {
                            # unfortunately we cannot use DATETIME with this since we avoid inferring a two digit as datetime representing month at runtime.
                            # IF will infer '09' as 9th day of month.
                            "type": DimensionType.STRING,
                            "day": {
                                "type": DimensionType.DATETIME,
                                "granularity": DatetimeGranularity.DAY,
                                "format": "%d",  # day, remember from %Y%m%d, this should output 29
                            },
                        },
                    },
                }
            },
            dimension_filter={
                "*": {
                    # if you want to pre-filter (so that in this notebook anything else than '1' should not be used, then add it here)
                    "*": {"*": {"*": {}}}  # any year  # any month  # any day
                }
            },
            # ADPD Prod data currently does not use any 'completion' file.
            # But declaring this will at least eliminat any update from the upstream partition causing event processing/trigger in this pipeline.
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        adpd_shadow_json_processed = app.create_data(
            "adpd_shadow_json2",
            inputs=[adpd_shadow_json_AS_SIGNAL],
            compute_targets=[
                BatchCompute(
                    # adpd_shadow_data alias comes from signal decl( the 'id'), you can overwrite it above using a map instead of an array
                    # 'adpd_shadow_data' will contain daily adpd shadow prod data only because no relative range has defined as the last dimension.
                    code=f"""
        output = spark.sql("select * from adpd_shadow_data").limit(1)
                                                 """,
                    lang=Lang.PYTHON,
                    GlueVersion="2.0",
                    WorkerType=GlueWorkerType.G_1X.value,
                    NumberOfWorkers=50,
                    Timeout=10 * 60,  # 10 hours
                )
            ],
        )

        daily_timer = app.add_timer("daily_timer", "rate(1 day)", time_dimension_id="day")

        adpd_shadow_json_processed_SCHEDULED = app.create_data(
            "adpd_shadow_json3",
            inputs=[
                daily_timer,
                # read last 14 days from NOW (trigger [event] date)
                adpd_shadow_json_AS_SIGNAL[1]["*"]["*"][:-14],
            ],
            input_dim_links=[
                (adpd_shadow_json_AS_SIGNAL("year"), EQUALS, daily_timer("day")),
                # e.g '2021-09-12 00:00'<datetime> -> '09'<string>
                (adpd_shadow_json_AS_SIGNAL("month"), lambda day: day.strftime("%m"), daily_timer("day")),
                # the following is actually redundant since IF auto-links same dim names
                (adpd_shadow_json_AS_SIGNAL("day"), EQUALS, daily_timer("day")),
            ],
            compute_targets=[NOOPCompute],
        )

        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        app._dev_context = dev_context
        #

        app.execute(adpd_shadow_json_processed_SCHEDULED["2021-09-21"], wait=True)

        path, compute_records = app.poll(adpd_shadow_json_processed[1]["2021"]["09"]["15"])
        assert not path, "Unexpected execution!"

        self.patch_aws_stop()
