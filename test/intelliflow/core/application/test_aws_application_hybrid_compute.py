# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import threading
import time
from datetime import datetime
from unittest.mock import MagicMock

import pytest

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
from intelliflow.core.application.application import Application
from intelliflow.core.platform.constructs import CompositeBatchCompute
from intelliflow.core.platform.definitions.compute import (
    ComputeFailedSessionState,
    ComputeFailedSessionStateType,
    ComputeResourceDesc,
    ComputeResponse,
    ComputeSessionDesc,
    ComputeSessionState,
    ComputeSuccessfulResponse,
    ComputeSuccessfulResponseType,
)
from intelliflow.core.platform.drivers.compute.aws import AWSGlueBatchComputeBasic
from intelliflow.core.platform.drivers.compute.aws_athena import AWSAthenaBatchCompute
from intelliflow.core.platform.drivers.compute.aws_emr import AWSEMRBatchCompute, InstanceConfig, RuntimeConfig
from intelliflow.core.signal_processing import Slot
from intelliflow.core.signal_processing.signal import *
from intelliflow.core.signal_processing.signal_source import (
    DATASET_COMPRESSION_DEFAULT_VALUE,
    DATASET_DELIMITER_DEFAULT_VALUE,
    DATASET_FOLDER_DEFAULT_VALUE,
    DatasetSchemaType,
    DatasetType,
)
from intelliflow.mixins.aws.test import AWSTestBase
from intelliflow.utils.test.inlined_compute import NOOPCompute


class TestAWSApplicationHybridCompute(AWSTestBase):
    def _create_test_application(self, id_or_app: Union[str, Application]):
        if isinstance(id_or_app, str):
            id = id_or_app
            app = AWSApplication(id, self.region)
        else:
            app = id_or_app
            id = app.id

        # first S3 signal
        app.add_external_data(
            data_id="eureka_training_data",
            s3_dataset=S3(
                "427809481713",
                "dex-ml-eureka-model-training-data",
                "cradle_eureka_p3/v8_00/training-data",
                StringVariant("NA", "region"),
                AnyDate("day", {"format": "%Y-%m-%d"}),
                data_folder="sub_folder",
            ),
        )

        # second S3 signal
        app.marshal_external_data(
            S3Dataset(
                "427809481713",
                "dex-ml-eureka-model-training-data",
                "cradle_eureka_p3/v8_00/all-data-prod",
                "partition_day={}",
                dataset_format=DataFormat.CSV,
            ),
            "eureka_training_all_data",
            {"day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d"}},
            {"*": {}},
            SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        return app

    def test_application_hybrid_compute_params(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = self._create_test_application("hybrid_compute")

        eureka_offline_training_data = app.get_data("eureka_training_data", context=Application.QueryContext.DEV_CONTEXT)[0]
        eureka_offline_all_data = app.get_data("eureka_training_all_data", context=Application.QueryContext.DEV_CONTEXT)[0]

        assert eureka_offline_training_data.signal().resource_access_spec.data_folder == "sub_folder"
        assert eureka_offline_all_data.signal().resource_access_spec.data_folder == DATASET_FOLDER_DEFAULT_VALUE

        # test when slot / compute cannot be mapped to any of the underlying BatchCompute drivers
        # here the problem is we are using generic BatchCompute slot type but not providing GlueVersion for example
        # the underlying Glue or Emr drivers will require either GlueVersion or RuntimeConfig to be defined
        # (refer their driver_spec impls for more details)
        with pytest.raises(ValueError):
            app.create_data(
                id="eureka_default",  # data_id
                inputs={"offline_training_data": eureka_offline_training_data, "offline_data": eureka_offline_all_data[:-2].no_event},
                compute_targets=[
                    BatchCompute(
                        "output = offline_data.unionAll(offline_training_data).limit(10)",
                        # MISSING!
                        # GlueVersion="2.0",
                        WorkerType=GlueWorkerType.G_1X.value,
                        NumberOfWorkers=50,
                    )
                ],
            )

        # check the total state of compute params from Glue driver
        default_selection_features_SPARK = app.create_data(
            id="eureka_default",  # data_id
            inputs={"offline_training_data": eureka_offline_training_data, "offline_data": eureka_offline_all_data[:-2].no_event},
            compute_targets=[
                Glue(
                    "output = offline_data.unionAll(offline_training_data).limit(10)",
                    GlueVersion="2.0",
                    WorkerType=GlueWorkerType.G_1X.value,
                    NumberOfWorkers=50,
                )
            ],
        )
        signal = default_selection_features_SPARK.signal()
        assert signal.resource_access_spec.data_format == DatasetSignalSourceFormat.CSV
        assert signal.resource_access_spec.data_delimiter == DATASET_DELIMITER_DEFAULT_VALUE
        assert signal.resource_access_spec.dataset_type.value == DatasetType.REPLACE
        assert signal.resource_access_spec.data_compression is DATASET_COMPRESSION_DEFAULT_VALUE
        assert signal.resource_access_spec.data_folder is DATASET_FOLDER_DEFAULT_VALUE
        assert signal.resource_access_spec.data_header_exists  # determined by the driver!
        assert signal.resource_access_spec.data_schema_type == DatasetSchemaType.SPARK_SCHEMA_JSON  # determined by the driver!

        default_selection_features_PRESTOSQL = app.create_data(
            id="eureka_default_presto",
            inputs={"offline_training_data": eureka_offline_training_data, "offline_data": eureka_offline_all_data[:-2].no_event},
            compute_targets=[
                PrestoSQL(
                    """
                                                                       SELECT * FROM offline_training_data
                                                                       UNION ALL
                                                                       SELECT *, '1' as region FROM offline_data
                                                                       """,
                )
            ],
        )

        signal = default_selection_features_PRESTOSQL.signal()
        assert signal.resource_access_spec.data_format == DatasetSignalSourceFormat.PARQUET  # determined/overwritten by the driver
        assert (
            signal.resource_access_spec.data_delimiter == DATASET_DELIMITER_DEFAULT_VALUE
        )  # not effective but still there since driver does not change it
        assert (
            signal.resource_access_spec.dataset_type.value == DatasetType.REPLACE
        )  # not effective but still there since driver does not change it
        assert signal.resource_access_spec.data_compression is DATASET_COMPRESSION_DEFAULT_VALUE
        assert signal.resource_access_spec.data_folder is DATASET_FOLDER_DEFAULT_VALUE
        # TODO Athena CTAS uses GZIP by default (and uses .gz as the suffix on partitions) and it is seamless for downstream consumption (Spark or Athena again)
        #  but the driver should still this keep the signal properties 'metadata' consistent (particularly for another app in collaboration mode)
        # refer
        # https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html for PARQUET default compression
        # assert signal.resource_access_spec.data_compression == "GZIP"
        assert signal.resource_access_spec.data_header_exists  # determined by the driver!
        assert signal.resource_access_spec.data_schema_type == DatasetSchemaType.ATHENA_CTAS_SCHEMA_JSON  # determined by the driver!

        # show how Athena driver rejects header=True when CSV is selected (to keep metadata consistent with runtime behaviour from AWS Athena)
        with pytest.raises(ValueError):
            app.create_data(
                id="should_fail",
                inputs={"offline_training_data": eureka_offline_training_data},
                compute_targets=[
                    PrestoSQL(
                        """
                                   SELECT * FROM offline_training_data
                                   """,
                    )
                ],
                # a dataset cannot have CSV output and header set with Athena (driver will raise)
                data_format=DatasetSignalSourceFormat.CSV,
                header=True,
            )

        # however it is OK if the compute is going to be handled by Glue/EMR for example
        app.create_data(
            id="should_NOT_fail",
            inputs={"offline_training_data": eureka_offline_training_data},
            compute_targets=[
                Glue(
                    """
            output=spark.sql('SELECT * FROM offline_training_data')
            """
                )
            ],
            # a dataset cannot have CSV output and header set with Athena (driver will raise)
            data_format=DatasetSignalSourceFormat.CSV,
            header=True,
        )

        # update with diff params
        default_selection_features_PRESTOSQL = app.update_data(
            id="eureka_default_presto",
            inputs={"offline_training_data": eureka_offline_training_data, "offline_data": eureka_offline_all_data[:-2].no_event},
            compute_targets=[
                PrestoSQL(
                    """
                                                                       SELECT * FROM offline_training_data
                                                                       UNION ALL
                                                                       SELECT *, '1' as region FROM offline_data
                                                                       """,
                )
            ],
            enforce_referential_integrity=False,
            data_format=DatasetSignalSourceFormat.CSV,
            header=False,
            delimiter=",",
        )

        signal = default_selection_features_PRESTOSQL.signal()
        assert signal.resource_access_spec.data_format == DatasetSignalSourceFormat.CSV  # driver honors user pref
        assert signal.resource_access_spec.data_delimiter == ","  # driver honors user pref
        assert (
            signal.resource_access_spec.dataset_type.value == DatasetType.REPLACE
        )  # not effective by still there since driver does not change it
        # TODO Athena CTAS uses GZIP by default (and uses .gz as the suffix on partitions) and it is seamless for downstream consumption (Spark or Athena again)
        #  but the driver should still this keep the signal properties 'metadata' consistent (particularly for another app in collaboration mode)
        assert signal.resource_access_spec.data_compression is DATASET_COMPRESSION_DEFAULT_VALUE
        assert not signal.resource_access_spec.data_header_exists
        assert signal.resource_access_spec.data_schema_type == DatasetSchemaType.ATHENA_CTAS_SCHEMA_JSON

        default_selection_features_PRESTOSQL = app.update_data(
            id="eureka_default_presto",
            inputs={"offline_training_data": eureka_offline_training_data, "offline_data": eureka_offline_all_data[:-2].no_event},
            compute_targets=[
                PrestoSQL(
                    """
                                                                       SELECT * FROM offline_training_data
                                                                       UNION ALL
                                                                       SELECT *, '1' as region FROM offline_data
                                                                       """,
                )
            ],
            enforce_referential_integrity=False,
            data_format=DatasetSignalSourceFormat.ORC,
            # TODO raise error from Athena driver when PARQUET or ORC and header is set False by user
            # header=False
        )

        signal = default_selection_features_PRESTOSQL.signal()
        assert signal.resource_access_spec.data_format == DatasetSignalSourceFormat.ORC
        assert signal.resource_access_spec.data_delimiter == DATASET_DELIMITER_DEFAULT_VALUE
        # TODO Athena uses GZIP by default and it is seamless for downstream consumption (Spark or Athena again)
        #  but the driver should still this keep the signal properties 'metadata' consistent (particularly for another app in collaboration mode)
        assert signal.resource_access_spec.data_compression is DATASET_COMPRESSION_DEFAULT_VALUE
        assert signal.resource_access_spec.data_folder is DATASET_FOLDER_DEFAULT_VALUE
        #  refer
        # https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html for ORC default compression
        # assert signal.resource_access_spec.data_compression == "GZIP"  # Athena default compression for ORC
        assert signal.resource_access_spec.data_header_exists  # determined by the driver!
        assert signal.resource_access_spec.data_schema_type == DatasetSchemaType.ATHENA_CTAS_SCHEMA_JSON  # determined by the driver!

        # and test a different compression with ORC
        default_selection_features_PRESTOSQL = app.update_data(
            id="eureka_default_presto",
            inputs={
                "offline_training_data": eureka_offline_training_data,
            },
            compute_targets=[
                PrestoSQL(
                    """
                                                                       SELECT * FROM offline_training_data
                                                                       """,
                )
            ],
            enforce_referential_integrity=False,
            data_format=DatasetSignalSourceFormat.ORC,
            compression="ZLIB",
        )

        signal = default_selection_features_PRESTOSQL.signal()
        assert signal.resource_access_spec.data_compression == "ZLIB"

        self.patch_aws_stop()

    def test_application_hybrid_compute_execution(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = self._create_test_application("hybrid_compute")

        eureka_offline_training_data = app.get_data("eureka_training_data", context=Application.QueryContext.DEV_CONTEXT)[0]
        eureka_offline_all_data = app.get_data("eureka_training_all_data", context=Application.QueryContext.DEV_CONTEXT)[0]

        default_selection_features_SPARK = app.create_data(
            id="eureka_default",  # data_id
            inputs={"offline_training_data": eureka_offline_training_data, "offline_data": eureka_offline_all_data[:-2].no_event},
            compute_targets=[
                # when inputs are ready, trigger the following
                Glue(
                    "output = offline_data.unionAll(offline_training_data).limit(10)",
                    # intentionally leave undefined to capture defaulting
                    # on the related parameter.
                    # GlueVersion="2.0",
                    WorkerType=GlueWorkerType.G_1X.value,
                    NumberOfWorkers=50,
                )
            ],
        )

        default_selection_features_PRESTOSQL = app.create_data(
            id="eureka_default_presto",
            inputs={"offline_training_data": eureka_offline_training_data, "offline_data": eureka_offline_all_data[:-2].no_event},
            compute_targets=[
                PrestoSQL(
                    """
                                                                       SELECT * FROM offline_training_data
                                                                       UNION ALL
                                                                       SELECT *, '1' as region FROM offline_data
                                                                       """,
                )
            ],
        )


        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        app._dev_context = dev_context
        #

        app.activate()

        def glue_driver_compute_method_impl(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            return ComputeSuccessfulResponse(
                ComputeSuccessfulResponseType.PROCESSING,
                ComputeSessionDesc(
                    "glue_JOB", ComputeResourceDesc("job_name", "job_arn", AWSGlueBatchComputeBasic)
                ),  # driver_type not mandatory but let's use it for extra code-path testing.
            )

        def glue_driver_get_session_state_impl(
            session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord"
        ) -> ComputeSessionState:
            # make sure that CompositeBatchDriver does not mess up with slot -> BatchCompute driver routing
            assert session_desc.session_id == "glue_JOB"
            return ComputeSessionState(
                (
                    ComputeSessionDesc("job_id", ComputeResourceDesc("job_name", "job_arn", driver=AWSGlueBatchComputeBasic))
                    if not session_desc
                    else session_desc
                ),
                ComputeSessionStateType.PROCESSING,
                [ComputeExecutionDetails("<start_time>", "<end_time>", dict({"param1": 1, "param2": 2}))],
            )

        # mock the second driver from CompositeBatchCompute which is the Glue driver in the default setting.
        # for more deterministic testing, we can set driver priority list during app init (but just avoiding it now for the sake of simplicity).
        app.platform.batch_compute._drivers[1].compute = MagicMock(side_effect=glue_driver_compute_method_impl)
        app.platform.batch_compute._drivers[1].get_session_state = MagicMock(side_effect=glue_driver_get_session_state_impl)

        def athena_driver_compute_method_impl(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            return ComputeSuccessfulResponse(
                ComputeSuccessfulResponseType.PROCESSING,
                ComputeSessionDesc(
                    "athena_JOB", ComputeResourceDesc("job_name", "job_arn", driver=AWSAthenaBatchCompute)
                ),  # driver_type not mandatory but let's use it for extra code-path testing.
            )

        def athena_driver_get_session_state_impl(
            session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord"
        ) -> ComputeSessionState:
            # make sure that CompositeBatchDriver does not mess up with slot -> BatchCompute driver routing
            assert session_desc.session_id == "athena_JOB"
            return ComputeSessionState(
                ComputeSessionDesc("job_id", ComputeResourceDesc("job_name", "job_arn", driver=None)) if not session_desc else session_desc,
                ComputeSessionStateType.PROCESSING,
                [ComputeExecutionDetails("<start_time>", "<end_time>", dict({"param1": 1, "param2": 2}))],
            )

        # mock the first driver from CompositeBatchCompute which is the 'Athena driver' in the default setting.
        # for more deterministic testing, we can set driver priority list during app init (but just avoiding it now for the sake of simplicity).
        app.platform.batch_compute._drivers[0].compute = MagicMock(side_effect=athena_driver_compute_method_impl)
        app.platform.batch_compute._drivers[0].get_session_state = MagicMock(side_effect=athena_driver_get_session_state_impl)

        # now trigger both nodes at the same time!
        app.process(eureka_offline_training_data["NA"]["2021-03-18"], with_activated_processor=False)

        # Athena must have been called once
        assert app.platform.batch_compute._drivers[0].compute.call_count == 1
        assert app.platform.batch_compute._drivers[0].get_session_state.call_count == 1
        # Glue must have been called once
        assert app.platform.batch_compute._drivers[1].compute.call_count == 1
        assert app.platform.batch_compute._drivers[1].get_session_state.call_count == 1

        # two nodes/routes should be active
        assert len(app.get_active_routes()) == 3

        # Now let's complete those executions by causing get_session_state to return COMPLETED
        def glue_driver_get_session_state_impl(
            session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord"
        ) -> ComputeSessionState:
            # make sure that CompositeBatchDriver does not mess up with slot -> BatchCompute driver routing
            assert session_desc.session_id == "glue_JOB"
            return ComputeSessionState(
                (
                    ComputeSessionDesc("job_run_id", ComputeResourceDesc("job_name", "job_arn", driver=AWSGlueBatchComputeBasic))
                    if not session_desc
                    else session_desc
                ),
                # COMPLETE!
                ComputeSessionStateType.COMPLETED,
                [
                    ComputeExecutionDetails(
                        "<start_time>",
                        "<end_time>",
                        dict({"Id": "job_run_id", "StartedOn": datetime.now(), "CompletedOn": datetime.now(), "param1": 1, "param2": 2}),
                    )
                ],
            )

        app.platform.batch_compute._drivers[1].get_session_state = MagicMock(side_effect=glue_driver_get_session_state_impl)

        def athena_driver_get_session_state_impl(
            session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord"
        ) -> ComputeSessionState:
            # make sure that CompositeBatchDriver does not mess up with slot -> BatchCompute driver routing
            assert session_desc.session_id == "athena_JOB"
            return ComputeSessionState(
                ComputeSessionDesc("job_id", ComputeResourceDesc("job_name", "job_arn", driver=None)) if not session_desc else session_desc,
                ComputeSessionStateType.COMPLETED,
                [
                    ComputeExecutionDetails(
                        "<start_time>",
                        "<end_time>",
                        dict(
                            {
                                "QueryExecution": {
                                    "QueryExecutionId": "test_exec_id",
                                    "Status": {"State": "SUCCEEDED"},
                                    "param1": 1,
                                    "param2": 2,
                                }
                            }
                        ),
                    )
                ],
            )

        app.platform.batch_compute._drivers[0].get_session_state = MagicMock(side_effect=athena_driver_get_session_state_impl)

        # cause orchestration to 'next-cycle' which scans active routes (implicitly calls BatchCompute driver) and updates the states.
        app.update_active_routes_status()

        # they hould be complete!
        assert len(app.get_active_routes()) == 0

        # and successful!
        path, glue_compute_records = app.poll(default_selection_features_SPARK["NA"]["2021-03-18"])
        assert path
        path, athena_compute_records = app.poll(default_selection_features_PRESTOSQL["NA"]["2021-03-18"])
        assert path

        # Debugging
        # patch Glue driver's remote call
        def glue_driver_filter_log_events(**query):
            return {
                "events": [
                    {"logStreamName": "job_run_id", "timestamp": 123, "message": "string", "ingestionTime": 123, "eventId": "string"},
                ],
                "searchedLogStreams": [
                    {"logStreamName": "job_run_id", "searchedCompletely": True},
                ],
                "nextToken": None,
            }

        app.platform.batch_compute._drivers[1]._filter_log_events = MagicMock(side_effect=glue_driver_filter_log_events)

        glue_compute_log_query = app.get_compute_record_logs(glue_compute_records[0])[0]
        assert glue_compute_log_query
        assert len(glue_compute_log_query.records) == 2
        assert glue_compute_log_query.records[0]["message"] == "string"
        # do the same again with the materialized view this time (rather than using the compute record directly)
        glue_compute_log_query = app.get_compute_record_logs(default_selection_features_SPARK["NA"]["2021-03-18"])[0]
        assert glue_compute_log_query
        assert len(glue_compute_log_query.records) == 2
        assert glue_compute_log_query.records[0]["message"] == "string"

        athena_compute_log_query = app.get_compute_record_logs(athena_compute_records[0])[0]
        assert athena_compute_log_query
        assert not athena_compute_log_query.records
        # do the same again with the materialized view this time (rather than using the compute record directly)
        athena_compute_log_query = app.get_compute_record_logs(default_selection_features_PRESTOSQL["NA"]["2021-03-18"])[0]
        assert athena_compute_log_query
        assert not athena_compute_log_query.records

        # verify cannot retrieve information for non-existent executions
        with pytest.raises(ValueError):
            app.get_compute_record_logs(default_selection_features_SPARK["NA"]["2022-09-12"])

        with pytest.raises(ValueError):
            app.get_compute_record_logs(default_selection_features_PRESTOSQL["NA"]["2022-09-12"])

        self.patch_aws_stop()

    def test_application_hybrid_compute_resolve_extra_params(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        # configure platform to use two overlapping BatchCompute drivers that would require extra param resolving.
        app = AWSApplication(
            "hybrid_compute",
            HostPlatform(
                AWSConfiguration.builder()
                .with_region(self.region)
                .with_param(
                    CompositeBatchCompute.BATCH_COMPUTE_DRIVERS_PARAM,
                    [AWSGlueBatchComputeBasic, AWSEMRBatchCompute],
                )
                .build()
            ),
        )

        should_run_on_glue = app.create_data(
            id="glue_node",
            compute_targets=[
                BatchCompute(
                    "<SPARK CODE>",
                    # will pick Glue thanks to order in the list above
                    GlueVersion="2.0",
                    WorkerType=GlueWorkerType.G_1X.value,
                    NumberOfWorkers=50,
                )
            ],
        )

        should_run_on_EMR = app.create_data(
            id="emr_node",
            compute_targets=[
                BatchCompute(
                    "<SPARK CODE>",
                    # will pick EMR over the existence of InstanceConfig
                    GlueVersion="2.0",
                    InstanceConfig=InstanceConfig(20),
                )
            ],
        )

        should_run_on_EMR2 = app.create_data(
            id="emr_node2",
            compute_targets=[
                BatchCompute(
                    "<SPARK CODE>",
                    # will pick EMR over the existence of RuntimeConfig
                    RuntimeConfig=RuntimeConfig.GlueVersion_2_0,
                )
            ],
        )

        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        app._dev_context = dev_context
        #

        app.activate()

        def glue_driver_compute_method_impl(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            return ComputeSuccessfulResponse(
                ComputeSuccessfulResponseType.PROCESSING,
                ComputeSessionDesc(
                    "glue_JOB", ComputeResourceDesc("job_name", "job_arn", AWSGlueBatchComputeBasic)
                ),  # driver_type not mandatory but let's use it for extra code-path testing.
            )

        def glue_driver_get_session_state_impl(
            session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord"
        ) -> ComputeSessionState:
            # make sure that CompositeBatchDriver does not mess up with slot -> BatchCompute driver routing
            assert session_desc.session_id == "glue_JOB"
            return ComputeSessionState(
                (
                    ComputeSessionDesc("job_id", ComputeResourceDesc("job_name", "job_arn", driver=AWSGlueBatchComputeBasic))
                    if not session_desc
                    else session_desc
                ),
                ComputeSessionStateType.PROCESSING,
                [ComputeExecutionDetails("<start_time>", "<end_time>", dict({"param1": 1, "param2": 2}))],
            )

        # mock the first driver from CompositeBatchCompute which is the Glue driver in the list above
        app.platform.batch_compute._drivers[0].compute = MagicMock(side_effect=glue_driver_compute_method_impl)
        app.platform.batch_compute._drivers[0].get_session_state = MagicMock(side_effect=glue_driver_get_session_state_impl)

        def emr_driver_compute_method_impl(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            return ComputeSuccessfulResponse(
                ComputeSuccessfulResponseType.PROCESSING,
                ComputeSessionDesc(
                    "athena_JOB", ComputeResourceDesc("job_name", "job_arn", driver=AWSEMRBatchCompute)
                ),  # driver_type not mandatory but let's use it for extra code-path testing.
            )

        def emr_driver_get_session_state_impl(
            session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord"
        ) -> ComputeSessionState:
            # make sure that CompositeBatchDriver does not mess up with slot -> BatchCompute driver routing
            assert session_desc.session_id == "emr_JOB"
            return ComputeSessionState(
                ComputeSessionDesc("job_id", ComputeResourceDesc("job_name", "job_arn", driver=None)) if not session_desc else session_desc,
                ComputeSessionStateType.PROCESSING,
                [ComputeExecutionDetails("<start_time>", "<end_time>", dict({"param1": 1, "param2": 2}))],
            )

        # mock the second driver from CompositeBatchCompute which is the 'EMR driver' in the current setting.
        app.platform.batch_compute._drivers[1].compute = MagicMock(side_effect=emr_driver_compute_method_impl)
        app.platform.batch_compute._drivers[1].get_session_state = MagicMock(side_effect=emr_driver_get_session_state_impl)

        # now trigger both nodes at the same time!
        app.execute(should_run_on_glue, wait=False)

        # Glue must have been called once
        assert app.platform.batch_compute._drivers[0].compute.call_count == 1
        assert app.platform.batch_compute._drivers[0].get_session_state.call_count == 1

        # No EMR execution yet
        assert app.platform.batch_compute._drivers[1].compute.call_count == 0
        assert app.platform.batch_compute._drivers[1].get_session_state.call_count == 0

        # 1 node should be active
        assert len(app.get_active_routes()) == 1

        app.execute(should_run_on_EMR, wait=False)
        assert app.platform.batch_compute._drivers[1].compute.call_count == 1
        assert app.platform.batch_compute._drivers[1].get_session_state.call_count == 1

        assert len(app.get_active_routes()) == 2

        app.execute(should_run_on_EMR2, wait=False)
        assert app.platform.batch_compute._drivers[1].compute.call_count == 2
        assert app.platform.batch_compute._drivers[1].get_session_state.call_count == 2

        assert len(app.get_active_routes()) == 3

        self.patch_aws_stop()

    def test_application_hybrid_compute_driver_lifecycle_management(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        # advanced configuration to control underlying BatchCompute driver types and their "order of precedence"
        app = AWSApplication(
            "hybrid_compute2",
            HostPlatform(
                AWSConfiguration.builder()
                .with_region(self.region)
                .with_param(CompositeBatchCompute.BATCH_COMPUTE_DRIVERS_PARAM, [AWSAthenaBatchCompute, AWSGlueBatchComputeBasic])
                .build()
            ),
        )

        # create a node to check the effect of driver removal on ongoing executions
        presto_node = app.create_data(id="arrivederci_a_presto", compute_targets=[PrestoSQL("select * from DUMMY")])

        app.activate()

        # create an execution on the presto node and keep it in PROCESSING state,
        # later on we will check the state of this node after the removal of Presto driver from the system
        #
        # BEGIN typical test time BatchCompute driver patching sequence:
        def athena_driver_compute_method_impl(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            return ComputeSuccessfulResponse(
                ComputeSuccessfulResponseType.PROCESSING,
                ComputeSessionDesc(
                    "athena_JOB", ComputeResourceDesc("job_name", "job_arn", driver=AWSAthenaBatchCompute)
                ),  # driver_type not mandatory but let's use it for extra code-path testing.
            )

        def athena_driver_get_session_state_impl(
            session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord"
        ) -> ComputeSessionState:
            # make sure that CompositeBatchDriver does not mess up with slot -> BatchCompute driver routing
            assert session_desc.session_id == "athena_JOB"
            return ComputeSessionState(
                ComputeSessionDesc("job_id", ComputeResourceDesc("job_name", "job_arn", driver=None)) if not session_desc else session_desc,
                ComputeSessionStateType.PROCESSING,
                [ComputeExecutionDetails("<start_time>", "<end_time>", dict({"param1": 1, "param2": 2}))],
            )

        # mock the first driver from CompositeBatchCompute which is the 'Athena driver' in the default setting.
        # for more deterministic testing, we can set driver priority list during app init (but just avoiding it now for the sake of simplicity).
        app.platform.batch_compute._drivers[0].compute = MagicMock(side_effect=athena_driver_compute_method_impl)
        app.platform.batch_compute._drivers[0].get_session_state = MagicMock(side_effect=athena_driver_get_session_state_impl)
        # END patch

        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        app._dev_context = dev_context
        #

        app.execute(presto_node, wait=False)

        # verify that there is an execution and an active compute record for the node
        # this check is logically important because at the end of the test we will directly check the failure on this
        # execution. so here we are verifying that the application still regards this execution as ongoing (PROCESSING).
        assert len(app.get_active_route(presto_node).active_compute_records) == 1

        # change supported drivers and their "order of precedence"
        # what will happen under the hood?
        # - during the deserialization of the app, platform and underlying drivers CompositeBatchCompute will detect
        # the removal of drivers (AWSAthenaBatchCompute) and an addition of new (AWSEMRBatchCompute)
        # - for the new development session Presto nodes won't be allowed
        # - and finally during the activation, removed drivers will be terminated (AWSAthenaBatchCompute::terminate)
        # - in the first orchestration cycle following the activation, ongoing executions (created by the removed driver)
        # will automatically fail at runtime. They will be orphaned and CompositeBatchCompute::get_session_state will not
        # match their <Slot> information to any of the new drivers. They will be marked as FAILED and moved to inactive.
        app = AWSApplication(
            "hybrid_compute2",
            HostPlatform(
                AWSConfiguration.builder()
                .with_region(self.region)
                .with_param(CompositeBatchCompute.BATCH_COMPUTE_DRIVERS_PARAM, [AWSGlueBatchComputeBasic, AWSEMRBatchCompute])
                .with_param(AWSCommonParams.DEFAULT_CREDENTIALS_AS_ADMIN, True)
                .build()
            ),
        )
        assert [type(driver) for driver in app.platform.batch_compute._drivers] == [AWSGlueBatchComputeBasic, AWSEMRBatchCompute]
        assert [type(driver) for driver in app.platform.batch_compute._removed_drivers] == [AWSAthenaBatchCompute]

        # change the compute target for the node so that it runs on Glue now (why glue? it is the firt in the two eligible driver list above)
        presto_node = app.create_data(id="arrivederci_a_presto", compute_targets=[SparkSQL("select * from DUMMY")])

        # during the activation orchestration will detect compute change and force move PrestoSQL based orphaned compute
        # to inactive state (as is).
        app.activate()

        assert [type(driver) for driver in app.platform.batch_compute._drivers] == [AWSGlueBatchComputeBasic, AWSEMRBatchCompute]
        assert not app.platform.batch_compute._removed_drivers

        path, compute_records = app.poll(presto_node)
        assert path is None
        # due to forced state-change into inactive, orphaned record will be in PROCESSING state permanently.
        assert compute_records[0].session_state.state_type == ComputeSessionStateType.PROCESSING

        self.patch_aws_stop()
