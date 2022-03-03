# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from intelliflow.core.platform.definitions.aws.glue import catalog as glue_catalog
from intelliflow.core.platform.definitions.aws.glue.catalog import GlueTableDesc


class AWSTestGlueCatalog(ABC):
    """Can be implemented in test modules and passed in to AWSTestBase::patch_aws_start for a full-stack emulation of
    AWS Glue Catalog in RheocerOS based end-to-end unit-tests.
    See
       ./test/intelliflow/core/application/test_aws_application_external_data module for a sample impl.
    """

    def create_signal(self, glue_client, database_name: str, table_name: str) -> Optional[GlueTableDesc]:
        # use the actual 'glue/catalog' module
        # use this object in place of 'glue_client' so that get_table and get_partitions will be redirected here.
        return glue_catalog.create_signal(self, database_name, table_name)

    def is_partition_present(self, glue_client, database_name: str, table_name: str, values: List[str]) -> bool:
        return glue_catalog.is_partition_present(self, database_name, table_name, values)

    def check_table(self, session, region: str, database: str, table_name: str) -> bool:
        response_get_tables = self.get_tables(DatabaseName=database)
        if response_get_tables:
            table_list = response_get_tables["TableList"]
            for table in table_list:
                if table["Name"] == table_name:
                    return True
        return False

    @abstractmethod
    def get_table(self, DatabaseName: str, Name: str) -> Dict[str, Any]:
        ...

    @abstractmethod
    def get_partitions(self, DatabaseName: str, TableName: str, MaxResults: int) -> Dict[str, Any]:
        ...

    @abstractmethod
    def get_partition(self, DatabaseName: str, TableName: str, PartitionValues: List[str]) -> Dict[str, Any]:
        ...

    @abstractmethod
    def get_tables(self, DatabaseName: str) -> Dict[str, Any]:
        ...


class AllPassTestCatalog(AWSTestGlueCatalog):
    def create_signal(self, glue_client, database_name: str, table_name: str) -> Optional[GlueTableDesc]:
        return GlueTableDesc(signal=None, default_compute_params=None, _test_bypass_checks=True)

    def is_partition_present(self, glue_client, database_name: str, table_name: str, values: List[str]) -> bool:
        return True

    def check_table(self, session, region: str, database: str, table: str) -> bool:
        return True

    def get_table(self, DatabaseName: str, Name: str) -> Dict[str, Any]:
        pass

    def get_partitions(self, DatabaseName: str, TableName: str, MaxResults: int) -> Dict[str, Any]:
        pass

    def get_partition(self, DatabaseName: str, TableName: str, PartitionValues: List[str]) -> Dict[str, Any]:
        pass

    def get_tables(self, DatabaseName: str) -> Dict[str, Any]:
        pass
