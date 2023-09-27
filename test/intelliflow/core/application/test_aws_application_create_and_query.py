# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest

import intelliflow.api_ext as flow
from intelliflow.api import DataFormat, S3Dataset
from intelliflow.api_ext import *
from intelliflow.core.application.application import Application
from intelliflow.core.application.context.node.base import DataNode, TimerNode
from intelliflow.core.application.context.node.internal.nodes import InternalDataNode
from intelliflow.core.signal_processing.definitions.dimension_defs import Type
from intelliflow.core.signal_processing.dimension_constructs import StringVariant
from intelliflow.core.signal_processing.signal import *
from intelliflow.mixins.aws.test import AWSTestBase
from intelliflow.utils.test.inlined_compute import FailureCompute, NOOPCompute


class TestAWSApplicationBuild(AWSTestBase):
    def _create_test_application(self, id_or_app: str):
        if isinstance(id_or_app, str):
            id = id_or_app
            app = AWSApplication(id, self.region)
        else:
            app = id_or_app
            id = app.id

        app.add_timer(
            "pipeline_monthly_timer"
            # see https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/ScheduledEvents.html
            ,
            "cron(0 0 1 * ? *)",  # Run at 00:00 am (UTC) every 1st day of the month
            "day",
        )

        eureka_offline_training_data = app.add_external_data(
            "eureka_training_data",
            S3(
                "321129071393",
                "dex-olaf-eureka-model-training-data",
                "cradle_eureka_p3/v8_00/training-data-prod",
                StringVariant("NA", "region"),
                AnyDate("my_day_my_way", {"format": "%Y-%m-%d"}),
                **{"key1": 1, "key2": {}}
            ),
        )

        eureka_offline_all_data = app.marshal_external_data(
            S3Dataset(
                "321129071393",
                "dex-olaf-eureka-model-training-data",
                "cradle_eureka_p3/v8_00/all-data-prod",
                "partition_day={}",
                dataset_format=DataFormat.CSV,
                **{"key1": 1, "key2": {}}
            ),
            "eureka_training_all_data",
            {"day": {"type": DimensionType.DATETIME}},
            {
                "*": {
                    "format": "%Y-%m-%d",
                    "timezone": "PST",
                },
            },
            SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        default_selection_features = app.create_data(
            "eureka_default_selection_data_over_two_days",  # data_id
            {"offline_data": eureka_offline_all_data[:-2], "offline_training_data": eureka_offline_training_data["NA"][:-2]},
            [(eureka_offline_all_data("day"), EQUALS, eureka_offline_training_data("my_day_my_way"))],  # dim link matrix for inputs
            {
                "reg": {
                    "type": DimensionType.STRING,
                    "day": {
                        "type": DimensionType.DATETIME,
                        "format": "%d-%m-%Y",
                    },
                }
            },
            [
                # using EQUALS lambda from core framework otherwise lambdas
                # declared here will cause 'test.intelliflow' module not found
                # serialization error at runtime (in integ-tests).
                ("reg", EQUALS, eureka_offline_training_data("region")),
                ("day", EQUALS, eureka_offline_all_data("day")),
            ],
            [  # when inputs are ready, trigger the following
                BatchCompute(
                    "output = offline_data.subtract(offline_training_data).limit(100)",
                    Lang.PYTHON,
                    ABI.GLUE_EMBEDDED,
                    # extra_params
                    spark_ver="2.4",
                    # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.start_job_run
                    MaxCapacity=60,
                    Timeout=60,  # in minutes
                    GlueVersion="1.0",
                )
            ],
            auto_backfilling_enabled=False,
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_FILE"}),
            **{"foo": "i am DEFAULT_DATAset"}
        )

        app.create_data(
            "compute_stub",
            compute_targets="print('hello world')",
            # no completion protocol, success file or whatsoever
            protocol=None,
        )

        default_completed_selection_features = app.create_data(
            "eureka_default_completed_features",  # data_id
            [default_selection_features],  # equivalent to ['*']['*']
            [],  # dim link matrix for inputs
            {
                "reg": {
                    "type": DimensionType.STRING,
                    "day": {
                        "type": DimensionType.DATETIME,
                        "format": "%d-%m-%Y",
                    },
                }
            },
            [("reg", EQUALS, default_selection_features("reg")), ("day", EQUALS, default_selection_features("day"))],
            [  # when inputs are ready, trigger the following
                BatchCompute(
                    python_module("test.intelliflow.core.application.glue_spark_python_etl"),
                    Lang.PYTHON,
                    ABI.GLUE_EMBEDDED,
                    spark_ver="2.4",
                    MaxCapacity=5,
                    Timeout=30,  # in minutes
                    GlueVersion="1.0",
                )
            ],
            auto_backfilling_enabled=False,
            **{"foo": "i am also default_dataset"}
        )

        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        app._dev_context = dev_context
        #

        return app

    def test_application_basic_query(self):
        self.patch_aws_start()
        app = self._create_test_application("sample_pipeline")
        app.activate()

        all_nodes = app.list()
        assert len(all_nodes) == 6  # 5 data nodes + 1 timer node

        timer_nodes = app.list(TimerNode)
        assert len(timer_nodes) == 1
        assert all([isinstance(node, TimerNode) for node in timer_nodes])

        external_nodes = app.list(ExternalDataNode)
        assert len(external_nodes) == 2
        assert all([isinstance(node, ExternalDataNode) for node in external_nodes])

        internal_nodes = app.list(InternalDataNode)
        assert len(internal_nodes) == 3
        assert all([isinstance(node, InternalDataNode) for node in internal_nodes])

        all_data_nodes = app.list_data()
        assert len(all_data_nodes) == 5
        assert all([isinstance(node, MarshalerNode) for node in all_data_nodes])

        eureka_nodes = app.query([DataNode.QueryVisitor("eureka_")])
        assert len(eureka_nodes) == 4
        assert all([isinstance(node, DataNode) for id, node in eureka_nodes.items()])

        eureka_nodes = app.query([DataNode.QueryVisitor("eureka_training")])
        assert len(eureka_nodes) == 2
        assert all([isinstance(node, ExternalDataNode) for id, node in eureka_nodes.items()])

        eureka_nodes = app.query([DataNode.QueryVisitor("eureka_default")])
        assert len(eureka_nodes) == 2
        assert all([isinstance(node, InternalDataNode) for id, node in eureka_nodes.items()])

        timer_nodes = app.query([TimerNode.QueryVisitor("timer")])
        assert len(timer_nodes) == 1
        assert all([isinstance(node, TimerNode) for id, node in timer_nodes.items()])

        eureka_nodes = app.query_data("eureka_")
        assert len(eureka_nodes) == 4
        assert all([isinstance(node, MarshalerNode) for id, node in eureka_nodes.items()])

        search_results = app.search_data("eureka_")
        assert len(search_results) == 4

        search_results = app.search_data("default_dataSet")
        assert len(search_results) == 2

        assert not app.search_data("gibberish")

        with pytest.raises(ValueError):
            gibb_data_node = app["gibberish"]

        monthly_timer = app.timer("pipeline_monthly_timer")
        monthly_timer2 = app["pipeline_monthly_timer"]
        assert monthly_timer is monthly_timer2
        assert isinstance(monthly_timer, MarshalerNode)

        eureka_training_data = app.data("eureka_training_data")
        assert isinstance(eureka_training_data, MarshalerNode)
        eureka_training_all_data = app["eureka_training_all_data"]
        assert isinstance(eureka_training_all_data, MarshalerNode)
        assert isinstance(app["eureka_default_selection_data_over_two_days"], MarshalerNode)
        assert isinstance(app["eureka_default_completed_features"], MarshalerNode)
        self.patch_aws_stop()

    def test_application_basic_exploration(self):
        self.patch_aws_start()
        app = self._create_test_application("sample_pipeline")
        app.activate()

        eureka_training_data = app["eureka_training_data"]
        assert eureka_training_data.attributes()["key1"] == 1
        assert eureka_training_data.attributes()["key2"] == {}
        import json

        assert eureka_training_data.metadata() == json.dumps(eureka_training_data.attributes(), indent=6)
        # assert eureka_training_data.key1 == 1
        # assert eureka_training_data.key2 == {}
        with pytest.raises(AttributeError):
            key3 = eureka_training_data.key3
        assert eureka_training_data.paths()
        assert eureka_training_data.path_format()
        assert eureka_training_data.dimensions()
        assert eureka_training_data.partitions()
        assert eureka_training_data.dimension_values()
        assert eureka_training_data.partition_values()
        assert eureka_training_data.partitions()
        eureka_training_all_data = app["eureka_training_all_data"]
        # assert eureka_training_all_data.key1 == 1
        # assert eureka_training_all_data.key2 == {}
        assert eureka_training_all_data.paths()
        assert eureka_training_all_data.path_format()
        assert eureka_training_all_data.dimensions()
        assert eureka_training_all_data.partitions()
        assert eureka_training_all_data.dimension_values()
        assert eureka_training_all_data.partition_values()
        assert eureka_training_all_data.partitions()
        internal_data_1 = app["eureka_default_selection_data_over_two_days"]
        assert internal_data_1.metadata() == json.dumps(internal_data_1.attributes(), indent=6, default=repr)
        # assert internal_data_1.foo == 'i am DEFAULT_DATAset'
        assert internal_data_1.paths()
        assert internal_data_1.path_format()
        assert internal_data_1.dimensions()
        assert internal_data_1.partitions()
        assert internal_data_1.dimension_values()
        assert internal_data_1.partition_values()
        assert internal_data_1.partitions()

        self.patch_aws_stop()

    def test_application_uses_active_context_by_default(self):
        """This unit-test is more about education for other engineers since its intention to;
        - clarify the default 'brand new scratchpad' behaviour of instantiating an Application
        - and also the fact that actions on an application are not persisted without an activation and
        all of the nodes that belong to the dev-context will be lost if the app object is out of scope.
        """
        self.patch_aws_start()
        app = self._create_test_application("sample_pipeline")
        # not activated yet, default mode is ACTIVE in queries
        assert len(app.list()) == 0
        with pytest.raises(ValueError):
            eureka_training_data = app["eureka_training_data"]

        # we have to indicate DEV_CONTEXT if we want to use query APIs to access nodes.
        timer_nodes = app.list(TimerNode, context=Application.QueryContext.DEV_CONTEXT)
        assert len(timer_nodes) == 1

        external_nodes = app.list(ExternalDataNode, context=Application.QueryContext.DEV_CONTEXT)
        assert len(external_nodes) == 2

        internal_nodes = app.list(InternalDataNode, context=Application.QueryContext.DEV_CONTEXT)
        assert len(internal_nodes) == 3

        all_data_nodes = app.list_data(context=Application.QueryContext.DEV_CONTEXT)
        assert len(all_data_nodes) == 5

        eureka_nodes = app.query([DataNode.QueryVisitor("eureka_")], context=Application.QueryContext.DEV_CONTEXT)
        assert len(eureka_nodes) == 4

        eureka_nodes = app.query([DataNode.QueryVisitor("eureka_training")], context=Application.QueryContext.DEV_CONTEXT)
        assert len(eureka_nodes) == 2

        eureka_nodes = app.query([DataNode.QueryVisitor("eureka_default")], context=Application.QueryContext.DEV_CONTEXT)
        assert len(eureka_nodes) == 2

        timer_nodes = app.query([TimerNode.QueryVisitor("timer")], context=Application.QueryContext.DEV_CONTEXT)
        assert len(timer_nodes) == 1

        self.patch_aws_stop()

    def test_application_node_signal_slots(self):
        """Check the nodes from signal/slot perspective"""
        self.patch_aws_start()
        app = self._create_test_application("sample_pipeline")

        eureka_training_data = app.get_data("eureka_training_data", context=Application.QueryContext.DEV_CONTEXT)[0]
        assert eureka_training_data.signal().alias == "eureka_training_data"
        assert DimensionFilter.check_equivalence(
            eureka_training_data.signal().domain_spec.dimension_filter_spec, DimensionFilter.load_raw({"NA": {"*": {type: Type.DATETIME}}})
        )
        eureka_training_all_data = app.get_data("eureka_training_all_data", context=Application.QueryContext.DEV_CONTEXT)[0]
        assert eureka_training_all_data.signal().alias == "eureka_training_all_data"
        assert DimensionFilter.check_equivalence(
            eureka_training_all_data.signal().domain_spec.dimension_filter_spec, DimensionFilter.load_raw({"*": {type: Type.DATETIME}})
        )
        default_completed_features = app.get_data("eureka_default_completed_features", context=Application.QueryContext.DEV_CONTEXT)[0]
        assert default_completed_features.signal().domain_spec.integrity_check_protocol
        assert default_completed_features.signal().domain_spec.integrity_check_protocol == InternalDataNode.DEFAULT_DATA_COMPLETION_PROTOCOL

        default_selection_features = app.get_data(
            "eureka_default_selection_data_over_two_days", context=Application.QueryContext.DEV_CONTEXT
        )[0]
        default_sel_fea_filter = default_selection_features.signal().domain_spec.dimension_filter_spec
        assert not DimensionFilter.check_equivalence(default_sel_fea_filter, DimensionFilter.load_raw({"EU": {"*": {type: Type.DATETIME}}}))
        assert DimensionFilter.check_equivalence(default_sel_fea_filter, DimensionFilter.load_raw({"NA": {"*": {type: Type.DATETIME}}}))
        assert default_selection_features.signal().domain_spec.integrity_check_protocol
        assert default_selection_features.signal().domain_spec.integrity_check_protocol == SignalIntegrityProtocol(
            "FILE_CHECK", {"file": "_FILE"}
        )

        dummy_compute_stub = app.get_data("compute_stub", context=Application.QueryContext.DEV_CONTEXT)[0]
        assert not dummy_compute_stub.signal().domain_spec.integrity_check_protocol

        default_selection_features_internal_node = cast("InternalDataNode", default_selection_features.bound)
        assert len(default_selection_features_internal_node.signal_link_node.signals) == 2
        assert len(default_selection_features_internal_node.signal_link_node.link_matrix) == 1
        assert len(default_selection_features_internal_node.output_dim_matrix) == 2

        slots = default_selection_features_internal_node._slots()
        assert len(slots) == 1
        assert slots[0].code == "output = offline_data.subtract(offline_training_data).limit(100)"

        monthly_timer_signal = app.get_timer("pipeline_monthly_timer", context=Application.QueryContext.DEV_CONTEXT)[0]
        assert DimensionFilter.check_equivalence(
            monthly_timer_signal.signal().domain_spec.dimension_filter_spec, DimensionFilter.load_raw({"*": {type: Type.DATETIME}})
        )
        time_dimension = monthly_timer_signal.signal().domain_spec.dimension_spec.find_dimension_by_name("day")
        assert time_dimension.params[DateVariant.FORMAT_PARAM] == "%Y-%m-%d"
        assert time_dimension.params[DateVariant.TIMEZONE_PARAM] == "UTC"

        temp_timer = app.add_timer("temp_daily_timer", "rate(1 day)", time_dimension_granularity=DatetimeGranularity.DAY)
        assert isinstance(temp_timer, MarshalerNode)
        # if nothing is specified, default dimension name is 'time'
        time_dimension = temp_timer.signal().domain_spec.dimension_spec.find_dimension_by_name("time")
        assert time_dimension.params[DateVariant.GRANULARITY_PARAM] == DatetimeGranularity.DAY

        self.patch_aws_stop()

    def test_application_create_update_data_defaults(self):
        self.patch_aws_start()
        app = self._create_test_application("sample_pipeline")

        # add a new node using timer as input
        monthly_timer_signal = app.get_timer("pipeline_monthly_timer", context=Application.QueryContext.DEV_CONTEXT)[0]
        default_selection_data = app.get_data("eureka_default_selection_data_over_two_days", context=Application.QueryContext.DEV_CONTEXT)[
            0
        ]

        spark_code = """output=spark.sql('
                              select * from eureka_default_selection_data_over_two_days
                                      order_type = 4
                     ')"""
        monthly_etl = app.create_data(
            "monthly_etl_NA", inputs=[monthly_timer_signal, default_selection_data["NA"][:-30]], compute_targets=spark_code
        )

        # dimension spec will be the same as first input (monthly_timer_signal)
        assert DimensionFilter.check_equivalence(
            monthly_etl.signal().domain_spec.dimension_filter_spec, DimensionFilter.load_raw({"*": {type: Type.DATETIME}})
        )
        monthly_etl_internal_node = cast("InternalDataNode", monthly_etl.bound)
        assert len(monthly_etl_internal_node.signal_link_node.signals) == 2
        # inputs will automatically be bound over the common dimension 'day'
        assert len(monthly_etl_internal_node.signal_link_node.link_matrix) == 1
        # output dimension link will bind over 'day' with any of the inputs
        assert len(monthly_etl_internal_node.output_dim_matrix) == 1

        slots = monthly_etl_internal_node._slots()
        assert len(slots) == 1
        assert slots[0].code == spark_code

        # check update now, same checks should still succeed.
        monthly_etl = app.update_data(
            "monthly_etl_NA", inputs=[monthly_timer_signal, default_selection_data["NA"][:-30]], compute_targets=spark_code
        )

        # dimension spec will be the same as first input (monthly_timer_signal)
        assert DimensionFilter.check_equivalence(
            monthly_etl.signal().domain_spec.dimension_filter_spec, DimensionFilter.load_raw({"*": {type: Type.DATETIME}})
        )
        monthly_etl_internal_node = cast("InternalDataNode", monthly_etl.bound)
        assert len(monthly_etl_internal_node.signal_link_node.signals) == 2
        # inputs will automatically be bound over the common dimension 'day'
        assert len(monthly_etl_internal_node.signal_link_node.link_matrix) == 1
        # output dimension link will bind over 'day' with any of the inputs
        assert len(monthly_etl_internal_node.output_dim_matrix) == 1

        slots = monthly_etl_internal_node._slots()
        assert len(slots) == 1
        assert slots[0].code == spark_code

        # check patch_data now, same checks should still succeed except the patched compute_targets.
        monthly_etl = app.patch_data(monthly_etl, compute_targets=[NOOPCompute, FailureCompute])
        assert DimensionFilter.check_equivalence(
            monthly_etl.signal().domain_spec.dimension_filter_spec, DimensionFilter.load_raw({"*": {type: Type.DATETIME}})
        )
        monthly_etl_internal_node = cast("InternalDataNode", monthly_etl.bound)
        slots = monthly_etl_internal_node._slots()
        assert len(slots) == 2
        assert slots[0].code != spark_code

        self.patch_aws_stop()

    def test_application_create_data_use_python_module(self):
        self.patch_aws_start()
        app = self._create_test_application("sample_pipeline")

        default_selection_data = app.get_data("eureka_default_selection_data_over_two_days", context=Application.QueryContext.DEV_CONTEXT)[
            0
        ]
        monthly_etl = app.create_data(
            "monthly_etl_NA",
            inputs=[default_selection_data["NA"][:-30]],
            compute_targets=[GlueBatchCompute(python_module("test.intelliflow.core.application.glue_spark_python_etl"))],
        )

        # dimension spec will be the same as first input (monthly_timer_signal)
        assert monthly_etl.dimension_spec().check_compatibility(default_selection_data.dimension_spec())
        assert DimensionFilter.check_equivalence(
            monthly_etl.signal().domain_spec.dimension_filter_spec, DimensionFilter.load_raw({"NA": {"*": {type: Type.DATETIME}}})
        )

        monthly_etl_internal_node = cast("InternalDataNode", monthly_etl.bound)
        assert len(monthly_etl_internal_node.signal_link_node.signals) == 1
        assert len(monthly_etl_internal_node.signal_link_node.link_matrix) == 0
        # output dimension link will bind over 'region' and 'day' with the input
        assert len(monthly_etl_internal_node.output_dim_matrix) == 2

        slots = monthly_etl_internal_node._slots()
        assert len(slots) == 1
        assert slots[0].code_lang == Lang.PYTHON
        assert slots[0].code_abi == ABI.GLUE_EMBEDDED
        assert slots[0].code.find("output = input0")
        self.patch_aws_stop()

    def test_application_create_data_input_aliasing(self):
        self.patch_aws_start()
        app = AWSApplication("input-aliasing", self.region)

        daily_timer = app.add_timer("daily_timer", "rate(1 day)", time_dimension_id="day")
        external_data = app.add_external_data("ext_data", S3("111222333444", "bucket", "folder", AnyDate("day", {"format": "%Y-%m-%d"})))

        internal_data = app.create_data(
            id="internal_data",
            inputs={
                "ext_data_alias_0": external_data,
                "ext_data_alias_1": external_data.ref,
                "ext_data_alias_2": external_data.ref.range_check(True),
                "ext_data_alias_3": external_data[:-7],
                "timer_alias": daily_timer,
            },
            compute_targets=[
                BatchCompute("output=ad_orders.limit(100)", WorkerType=GlueWorkerType.G_1X.value, NumberOfWorkers=50, GlueVersion="3.0")
            ],
        )

        internal_data_node = cast("InternalDataNode", internal_data.bound)
        assert len(internal_data_node.signal_link_node.signals) == 5
        assert internal_data_node.signal_link_node.signals[0].alias == "ext_data_alias_0"
        assert internal_data_node.signal_link_node.signals[1].alias == "ext_data_alias_1"
        assert internal_data_node.signal_link_node.signals[2].alias == "ext_data_alias_2"
        assert internal_data_node.signal_link_node.signals[3].alias == "ext_data_alias_3"
        assert internal_data_node.signal_link_node.signals[4].alias == "timer_alias"

        self.patch_aws_stop()

    def test_application_update_data_on_nonexistent_data(self):
        self.patch_aws_start()
        app = self._create_test_application("sample_pipeline")

        eureka_offline_all_data = app.get_data("eureka_training_all_data", context=Application.QueryContext.DEV_CONTEXT)[0]
        eureka_offline_training_data = app.get_data("eureka_training_data", context=Application.QueryContext.DEV_CONTEXT)[0]
        with pytest.raises(ValueError):
            app.update_data(
                "DONT_EXISTS_eureka_default_selection_data_over_two_days",
                {"offline_data": eureka_offline_all_data[:-2], "offline_training_data": eureka_offline_training_data["NA"][:-2]},
                [
                    (eureka_offline_all_data("day"), lambda dim: dim, eureka_offline_training_data("my_day_my_way"))
                ],  # dim link matrix for inputs
                {
                    "reg": {
                        "type": DimensionType.STRING,
                        "day": {
                            "type": DimensionType.DATETIME,
                            "format": "%d-%m-%Y",
                        },
                    }
                },
                [
                    ("reg", lambda dim: dim, eureka_offline_training_data("region")),
                    ("day", lambda dim: dim.strftime("%d-%m-%Y"), eureka_offline_all_data("day")),
                ],
                [  # when inputs are ready, trigger the following
                    BatchCompute(
                        "output = offline_data.subtract(offline_training_data).limit(100)",
                        Lang.PYTHON,
                        ABI.GLUE_EMBEDDED,
                        # extra_params
                        spark_ver="2.4",
                        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.start_job_run
                        MaxCapacity=5,
                        Timeout=30,  # in minutes
                        GlueVersion="1.0",
                    )
                ],
                auto_backfilling_enabled=False,
                **{"foo": "i am DEFAULT_DATAset"}
            )

        with pytest.raises(ValueError):
            app.patch_data(
                "DONT_EXISTS_eureka_default_selection_data_over_two_days2", {"offline_data": eureka_offline_all_data[:-2]}, [NOOPCompute]
            )

        with pytest.raises(ValueError):
            # wrong input type (for id_or_node param)
            app.patch_data(app.add_timer("timer", "rate(1 day)"), [eureka_offline_all_data], [NOOPCompute])

        self.patch_aws_stop()

    def test_application_update_data_with_changing_signalling_properties(self):
        self.patch_aws_start()
        app = self._create_test_application("sample_pipeline")

        eureka_offline_all_data = app.get_data("eureka_training_all_data", context=Application.QueryContext.DEV_CONTEXT)[0]
        eureka_offline_training_data = app.get_data("eureka_training_data", context=Application.QueryContext.DEV_CONTEXT)[0]

        # just update with the same params (irrespective of 'enforce_referential_integrity'
        # no matter what it should always succeed
        app.update_data(
            "eureka_default_selection_data_over_two_days",  # data_id
            {"offline_data": eureka_offline_all_data[:-2], "offline_training_data": eureka_offline_training_data["NA"][:-2]},
            [
                (eureka_offline_all_data("day"), lambda dim: dim, eureka_offline_training_data("my_day_my_way"))
            ],  # dim link matrix for inputs
            {
                "reg": {
                    "type": DimensionType.STRING,
                    "day": {
                        "type": DimensionType.DATETIME,
                        "format": "%d-%m-%Y",
                    },
                }
            },
            [
                ("reg", lambda dim: dim, eureka_offline_training_data("region")),
                ("day", lambda dim: dim.strftime("%d-%m-%Y"), eureka_offline_all_data("day")),
            ],
            [  # when inputs are ready, trigger the following
                BatchCompute(
                    "output = offline_data.subtract(offline_training_data).limit(100)",
                    Lang.PYTHON,
                    ABI.GLUE_EMBEDDED,
                    # extra_params
                    spark_ver="2.4",
                    # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.start_job_run
                    MaxCapacity=5,
                    Timeout=30,  # in minutes
                    GlueVersion="1.0",
                )
            ],
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_FILE"}),
            enforce_referential_integrity=True,
            **{"foo": "i am DEFAULT_DATAset"}
        )

        # change the compute, remove metadata, it should still succeed
        app.update_data(
            "eureka_default_selection_data_over_two_days",  # data_id
            {"offline_data": eureka_offline_all_data[:-2], "offline_training_data": eureka_offline_training_data["NA"][:-2]},
            [
                (eureka_offline_all_data("day"), lambda dim: dim, eureka_offline_training_data("my_day_my_way"))
            ],  # dim link matrix for inputs
            {
                "reg": {
                    "type": DimensionType.STRING,
                    "day": {
                        "type": DimensionType.DATETIME,
                        "format": "%d-%m-%Y",
                    },
                }
            },
            [
                ("reg", lambda dim: dim, eureka_offline_training_data("region")),
                ("day", lambda dim: dim.strftime("%d-%m-%Y"), eureka_offline_all_data("day")),
            ],
            [GlueBatchCompute(code="output=offline_data")],  # when inputs are ready, trigger the following
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_FILE"}),
            enforce_referential_integrity=True,
        )

        # change the inputs but keep the output signal same
        app.update_data(
            "eureka_default_selection_data_over_two_days",  # data_id
            [eureka_offline_training_data["NA"][:-2]],
            [],
            {
                "reg": {
                    "type": DimensionType.STRING,
                    "day": {
                        "type": DimensionType.DATETIME,
                        "format": "%d-%m-%Y",
                    },
                }
            },
            [
                ("reg", lambda dim: dim, eureka_offline_training_data("region")),
                ("day", lambda dim: dim.strftime("%d-%m-%Y"), eureka_offline_training_data("my_day_my_way")),
            ],
            [GlueBatchCompute(code="output=offline_data")],  # when inputs are ready, trigger the following
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_FILE"}),
            enforce_referential_integrity=True,
        )

        # change the output signal!
        with pytest.raises(ValueError):
            app.update_data(
                "eureka_default_selection_data_over_two_days",  # data_id
                [eureka_offline_training_data["NA"][:-2]],
                [],
                {"reg": {"type": DimensionType.STRING}},
                [
                    ("reg", lambda dim: dim, eureka_offline_training_data("region")),
                ],
                [GlueBatchCompute(code="output=offline_data")],  # when inputs are ready, trigger the following
                protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_FILE"}),
                enforce_referential_integrity=True,
            )

        # user disables referential integrity check and update is allowed.
        default_selection_features = app.update_data(
            "eureka_default_selection_data_over_two_days",  # data_id
            [eureka_offline_training_data["NA"][:-2]],
            [],
            {"reg": {"type": DimensionType.STRING}},
            [
                ("reg", lambda dim: dim, eureka_offline_training_data("region")),
            ],
            [GlueBatchCompute(code="output=offline_data")],  # when inputs are ready, trigger the following
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_FILE"}),
            enforce_referential_integrity=False,
        )

        # now finally show that with a leaf (final) node, no matter what the case is update is always allowed.
        # change the output signal but it should succeed!
        app.update_data(
            "eureka_default_completed_features",
            [default_selection_features],
            [],  # dim link matrix for inputs
            {"reg": {"type": DimensionType.STRING}},
            [("reg", lambda dim: dim, default_selection_features("reg"))],
            [  # when inputs are ready, trigger the following
                BatchCompute(
                    python_module("test.intelliflow.core.application.glue_spark_python_etl"),
                    Lang.PYTHON,
                    ABI.GLUE_EMBEDDED,
                    spark_ver="2.4",
                    MaxCapacity=5,
                    Timeout=30,  # in minutes
                    GlueVersion="1.0",
                )
            ],
            auto_backfilling_enabled=False,
            enforce_referential_integrity=True,  # Enforce
            **{"foo": "i am also default_dataset"}
        )

        self.patch_aws_stop()

    def test_application_update_data_referential_integrity_error(self):
        self.patch_aws_start()
        app = AWSApplication("update-data-err", self.region)

        root_node = app.create_data(
            "root",
            output_dimension_spec={"dim": {type: Type.STRING}},
            compute_targets=[NOOPCompute],
        )

        child_1 = app.create_data("child1", inputs={"root_alias1": root_node, "root_alias2": root_node}, compute_targets=[NOOPCompute])

        child_2 = app.create_data("child2", inputs=[root_node], compute_targets=[NOOPCompute])

        with pytest.raises(ValueError):
            app.patch_data(
                root_node,
                output_dimension_spec={"dim_new": {type: Type.STRING}},  # new dimension changes signalling properties and causes error
                enforce_referential_integrity=True,
            )

        # force
        app.patch_data(
            root_node,
            output_dimension_spec={"dim_new": {type: Type.STRING}},
            # user must set this to False if dependent nodes (child_1 and child_2) will also be updated next
            enforce_referential_integrity=False,
        )

        # BUT user forgets to update child_1 and child_2 leaving the app topology in a bad state:
        #  - child_1 and child_2 have not been updated with the new signalling property of their into root_node
        with pytest.raises(TypeError):
            app.activate()

        app.patch_data(child_1, inputs={"root_alias1": root_node, "root_alias2": root_node})

        app.patch_data(child_2, inputs=[root_node])

        # now child instruction references must have been refreshed with new 'root_node' reference
        app.activate()

        self.patch_aws_stop()
