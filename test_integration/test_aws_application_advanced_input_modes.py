# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import time
from datetime import datetime

import pytest
from mock import MagicMock

from intelliflow.api_ext import *
from intelliflow.core.application.core_application import ApplicationState
from intelliflow.core.platform.constructs import FeedBackSignalProcessingMode
from intelliflow.core.signal_processing.dimension_constructs import StringVariant
from intelliflow.mixins.aws.integ_test import AWSIntegTestMixin


class TestAWSApplicationAdvancedInputModes(AWSIntegTestMixin):
    """Integ-test coverage for 'test.TestAWSApplicationAdvancedInputModes'

    This example covers advanced input modes such as 'reference' and 'range_check'.

    'reference' inputs dont mandate event-polling on them and can be used on data which just needs to be inputted
    to the compute side.

    'range_check' controls whether a 'ranged' input (with multiple materialized paths, as in a range of data partitions)
    will checked for the existence of data spawning its entire domain. For example, if you want to make sure that a
    trigger will happen only all of the data partitions from a range of a dataset are ready then you use this flag on
    an input in Application::create_data.
    """

    def setup(self):
        super().setup("IntelliFlow")

    def _create_app(self) -> AWSApplication:
        app = self._create_test_application("adv-ins")
        app.set_security_conf(
            Storage,
            ConstructSecurityConf(
                persisting=ConstructPersistenceSecurityDef(
                    ConstructEncryption(
                        key_allocation_level=EncryptionKeyAllocationLevel.HIGH,
                        key_rotation_cycle_in_days=365,
                        is_hard_rotation=False,
                        reencrypt_old_data_during_hard_rotation=False,
                        trust_access_from_same_root=True,
                    )
                ),
                passing=None,
                processing=None,
            ),
        )
        return app

    def test_application_reference_inputs_in_different_layers(self):
        app = self._create_app()
        eureka_offline_training_data = app.add_external_data(
            data_id="eureka_training_data",
            s3_dataset=S3(
                "427809481713",
                "dex-ml-eureka-model-training-data",
                "cradle_eureka_p3/v8_00/training-data",
                StringVariant("NA", "region"),
                AnyDate("day", {"format": "%Y-%m-%d"}),
            ),
        )

        eureka_offline_all_data = app.marshal_external_data(
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

        default_selection_features = app.create_data(
            id="eureka_default",  # data_id
            inputs={
                "offline_training_data": eureka_offline_training_data,
                # REFERENCE! Routing runtime won't wait for an event on this one as long as
                # it can be mapped from the dimensions of the other non-reference input.
                "offline_data": eureka_offline_all_data[:-2].no_event,
            },
            compute_targets=[  # when inputs are ready, trigger the following
                BatchCompute(
                    "output = offline_data.unionAll(offline_training_data).limit(10)",
                    Lang.PYTHON,
                    ABI.GLUE_EMBEDDED,
                    GlueVersion="2.0",
                    WorkerType=GlueWorkerType.G_1X.value,
                    NumberOfWorkers=50,
                )
            ],
        )

        d_ad_orders_na = app.marshal_external_data(
            GlueTable("dex_ml_catalog", "d_ad_orders_na"),
            "d_ad_orders_na",
            {"order_day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d", "timezone": "PST"}},
            {"*": {}},
        )

        merge_eureka_default_with_AD = app.create_data(
            id="REPEAT_AD_ORDERS",
            # please note that inputs won't be linked since their dimension names are diff
            # and also there is no input_dim_links provided. this enables the following weird
            # join of partitions from different dates.
            inputs=[default_selection_features, d_ad_orders_na["2021-01-13"].as_reference()],  # or no_event or ref or reference()
            compute_targets=[
                BatchCompute(
                    "output=eureka_default.join(d_ad_orders_na, ['customer_id'], how='left').drop('order_day').limit(100)",
                    WorkerType=GlueWorkerType.G_1X.value,
                    NumberOfWorkers=50,
                    GlueVersion="2.0",
                )
            ],
        )

        app.activate(allow_concurrent_executions=False)

        # now this should create an avalanche in the pipeline down to the final node
        app.process(eureka_offline_training_data["NA"]["2021-03-18"], with_activated_processor=True)

        # 1- check the first node and poll till it ends (check the console for updates on execution state).
        self.poll(app, default_selection_features["NA"]["2021-03-18"], expect_failure=False)

        # 2- now the second node must have been started
        self.poll(app, merge_eureka_default_with_AD["NA"]["2021-03-18"], expect_failure=False)

        app.terminate()
        app.delete()

    def test_application_nearest_input(self):
        app = self._create_app()

        eureka_offline_training_data = app.add_external_data(
            data_id="eureka_training_data",
            s3_dataset=S3(
                "427809481713",
                "dex-ml-eureka-model-training-data",
                "cradle_eureka_p3/v8_00/training-data",
                StringVariant("NA", "region"),
                AnyDate("day", {"format": "%Y-%m-%d"}),
            ),
        )

        eureka_offline_all_data = app.marshal_external_data(
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

        pick_latest_from_eureka = app.create_data(
            id="latest_eureka",
            inputs={
                "independent_signal": eureka_offline_all_data,
                "offline_training_data": eureka_offline_training_data["*"][:-14].nearest(),  # or latest()
            },
            compute_targets=[  # when inputs are ready, trigger the following
                BatchCompute(
                    """
from pyspark.sql.functions import col
from datetime import datetime

output = offline_training_data.limit(10)
# most recent partition of this dataset is from 2021-03-18 so as long as the test range (of 14 days) covers this, then
# the following assertion should be ok.
output_order_day = output.collect()[0][4]
output_order_day_str = output_order_day if isinstance(output_order_day, str) else output_order_day.strftime('%Y-%m-%d')
assert output_order_day_str.startswith('2021-03-18')  # 'latest' order_day
                                                          """,
                    Lang.PYTHON,
                    ABI.GLUE_EMBEDDED,
                    GlueVersion="2.0",
                    WorkerType=GlueWorkerType.G_1X.value,
                    NumberOfWorkers=30,
                )
            ],
        )

        # should succeed since 2021-03-18 is within 14 days of range from 2021-03-20
        path = app.execute(pick_latest_from_eureka["2021-03-20"])
        assert path  # should succeed
        try:
            # execute is different than runtime behaviour, it won't keep waiting for the 'nearest=True' signal.
            # because it will artificially feed the incoming signal for 'offline_training_data' here (via process API).
            # relies on compute failure if no data is not found within the range.
            # (however in normal execution, orchestration waits for the completion of a partition within the user provided range).
            app.execute(pick_latest_from_eureka["2021-04-20"])
            assert False, "this execution must have failed since nearest input range actually does not have any data."
        except:
            pass

        app.terminate()
        app.delete()
