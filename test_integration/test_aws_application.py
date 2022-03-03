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
from intelliflow.utils.test.inlined_compute import NOOPCompute


class TestAWSApplication(AWSIntegTestMixin):
    """Integ-test coverage for 'test.TestAWSApplication'"""

    def setup(self):
        super().setup("IntelliFlow")

    def test_application_check_backwards_compatibility(self):
        """Create and continously update/activate a full-fledged app (in each integ-test run).

        By full-fledged, we mean an app that would contain almost all of the high-level features, node types possible.

        Framework developers are expected to decorate this app with new features or modifications to guarantee
        backwards compatibility check via this test.
        """
        # loads implicitly,
        app = self._create_test_application("test-perm-app")
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

        # S3 external data
        # TODO add external data management test util and read the target account from the config.
        eureka_offline_training_data = app.add_external_data(
            "eureka_training_data",
            S3(
                "427809481713",
                "dex-ml-eureka-model-training-data",
                "cradle_eureka_p3/v8_00/training-data",
                StringVariant("NA", "region"),
                AnyDate("my_day_my_way", {"format": "%Y-%m-%d"}),
            ),
        )

        # glue table external data
        ship_options = app.marshal_external_data(
            GlueTable("dexbi", "d_ship_option", partition_keys=[], **{"metadata_field_1": "value"}),
            "ship_options",
            {},
            {},
            SignalIntegrityProtocol("FILE_CHECK", {"file": ["DELTA", "SNAPSHOT"]}),
        )

        monthly_timer_signal = app.add_timer(
            id="monthly_timer"
            # see https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/ScheduledEvents.html
            ,
            schedule_expression="cron(0 0 1 * ? *)",  # Run at 00:00 am (UTC) every 1st day of the month
            time_dimension_id="day",
        )

        default_selection_features = app.create_data(
            id="monthly_default_selection",  # data_id
            inputs={"offline_training_data": eureka_offline_training_data["NA"][:-30], "timer": monthly_timer_signal},
            compute_targets=[  # when inputs are ready, trigger the following
                BatchCompute(
                    "output = offline_data.subtract(offline_training_data)",
                    Lang.PYTHON,
                    ABI.GLUE_EMBEDDED,
                    # extra_params
                    # interpreted by the underlying Compute (Glue)
                    # and also passed in as job run args (irrespective of compute).
                    spark_ver="2.4",
                    # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.start_job_run
                    MaxCapacity=5,
                    Timeout=30,  # in minutes
                    GlueVersion="1.0",
                )
            ],
        )
        monthly_ship_options_snapshot = app.create_data(
            id="monthly_SO_snapshot", inputs=[monthly_timer_signal, ship_options], compute_targets="output=d_ship_option"
        )

        monthly_impressions_shiptions_joined = app.create_data(
            id="default_with_SOs",
            inputs=[default_selection_features, monthly_ship_options_snapshot],
            compute_targets="output=monthly_default_selection.join(monthly_SO_snapshot, ['ship_option'])",
        )
        # activate
        app.activate()
        # since this app is permanent, block signal propagation in the background to avoid IMR costs, etc.
        # the intention in this test is to check the compatibility in object model, etc.
        app.pause()

        # DO NOT TERMINATE/DELETE

    def test_application_terminate_after_activation(self):
        app = self._create_test_application("test-app")
        app.activate()

        with pytest.raises(Exception):
            app.delete()

        app.terminate()

        with pytest.raises(Exception):
            app.terminate()

        assert app.state == ApplicationState.INACTIVE

        app.delete()

        assert app.state == ApplicationState.DELETED

    def test_application_termination_resilience(self):
        app = self._create_test_application("test-app")
        # external_data = app.marshal_external_data(
        #    S3Dataset("111222333444", "bucke", "prefix", "{}", dataset_format=DataFormat.CSV)
        #    , "test_ext_data"
        #    , { 'region': { 'type': Type.STRING } }
        #    , {
        #        "NA": {
        #        },
        #    },
        #    SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"})
        # )

        timer = app.add_timer("timer_input", "rate(5 minutes)")

        app.create_data(
            id="test_internal_data",
            inputs=[timer],
            input_dim_links=[],
            output_dimension_spec={},
            output_dim_links={},
            compute_targets=[NOOPCompute],
        )

        app.activate()

        # make termination fail half-way through (check resilience in against different modules)
        # 1- processor fails and then termination is resumed manually
        original_terminate = app.platform.processor.terminate
        app.platform.processor.terminate = MagicMock(side_effect=RuntimeError())
        try:
            app.terminate()
        except RuntimeError:
            pass
        assert app.state == ApplicationState.TERMINATING
        app.platform.processor.terminate = original_terminate
        app.terminate()
        assert app.state == ApplicationState.INACTIVE

        # 2- processor_queue
        app.activate()
        original_terminate = app.platform.processor_queue.terminate
        app.platform.processor_queue.terminate = MagicMock(side_effect=RuntimeError())
        try:
            app.terminate()
        except RuntimeError:
            pass
        assert app.state == ApplicationState.TERMINATING
        app.platform.processor_queue.terminate = original_terminate
        app.terminate()
        assert app.state == ApplicationState.INACTIVE

        # 3- batch_compute
        app.activate()
        original_terminate = app.platform.batch_compute.terminate
        app.platform.batch_compute.terminate = MagicMock(side_effect=RuntimeError())
        try:
            app.terminate()
        except RuntimeError:
            pass
        assert app.state == ApplicationState.TERMINATING
        app.platform.batch_compute.terminate = original_terminate
        app.terminate()
        assert app.state == ApplicationState.INACTIVE

        # 4- routing_table
        app.activate()
        original_terminate = app.platform.routing_table.terminate
        app.platform.routing_table.terminate = MagicMock(side_effect=RuntimeError())
        try:
            app.terminate()
        except RuntimeError:
            pass
        assert app.state == ApplicationState.TERMINATING
        app.platform.routing_table.terminate = original_terminate
        app.terminate()
        assert app.state == ApplicationState.INACTIVE

        # and finally test that a TERMINATING application can be restored back to active state
        app.activate()
        original_terminate = app.platform.processor.terminate
        app.platform.processor.terminate = MagicMock(side_effect=RuntimeError())
        try:
            app.terminate()
        except RuntimeError:
            pass
        app.platform.processor.terminate = original_terminate
        app.activate()
        assert app.state == ApplicationState.ACTIVE

        # clean up
        app.terminate()
        app.delete()

    def test_application_context_management(self):
        app = self._create_test_application("test-app", reset_if_exists=True)
        external_trigger = app.add_timer("trigger_timer", "rate(1 day)")

        app.create_data(
            id="test_internal_data",
            inputs=[external_trigger],
            input_dim_links=[],
            output_dimension_spec={},
            output_dim_links={},
            compute_targets=[NOOPCompute],
        )
        app.save_dev_state()

        # reload
        app = self._create_test_application("test-app")

        assert not app.get_timer("trigger_timer", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert not app.get_timer("trigger_timer", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)
        assert not app.get_data(
            "test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT
        )
        assert not app.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)

        app.load_dev_state()
        assert not app.get_timer("trigger_timer", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert app.get_timer("trigger_timer", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)
        assert not app.get_data(
            "test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT
        )
        assert app.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)

        app.activate()
        assert app.state == ApplicationState.ACTIVE
        assert app.get_timer("trigger_timer", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert app.get_timer("trigger_timer", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)
        assert app.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert app.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)

        app = self._create_test_application("test-app")
        assert app.get_timer("trigger_timer", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert not app.get_timer("trigger_timer", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)
        assert app.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert not app.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)

        app.attach()
        assert app.get_timer("trigger_timer", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert app.get_timer("trigger_timer", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)
        assert app.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert app.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)

        app.terminate()
        assert not app.get_timer("trigger_timer", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert app.get_timer("trigger_timer", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)
        assert not app.get_data(
            "test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT
        )
        assert app.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)

        assert app.state == ApplicationState.INACTIVE

        app.activate()
        assert app.state == ApplicationState.ACTIVE
        assert app.get_timer("trigger_timer", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert app.get_timer("trigger_timer", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)
        assert app.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert app.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)

        app.terminate()
        # as tested above, now active context is empty but we still have dev context intact. However, the following
        # reload will invalidate dev context.
        app = self._create_test_application("test-app")
        assert app.state == ApplicationState.INACTIVE
        # as explained above, both active and dev contexts are gone at this point.
        assert not app.get_timer("trigger_timer", Application.QueryApplicationScope.ALL, Application.QueryContext.ALL)

        app.delete()

    def test_application_pause_resume(self):
        app = self._create_test_application("pause-resume", reset_if_exists=True)
        # no-op
        app.pause()
        assert app.state == ApplicationState.INACTIVE

        # no-op
        app.resume()
        assert app.state == ApplicationState.INACTIVE

        app = self._create_test_application("pause-resume")
        external_trigger = app.add_timer("trigger_timer", "rate(1 day)")
        internal_data = app.create_data(id="test_internal_data", inputs=[external_trigger], compute_targets=[NOOPCompute])
        app.activate()
        assert app.state == ApplicationState.ACTIVE
        app.pause()
        assert app.state == ApplicationState.PAUSED

        # now trigger and see no action will be taken
        test_time = datetime.now()
        """
        Cannot use Application::process since it does not allow ASYNC event/signal ingestion in PAUSED mode.
        app.process(external_trigger[test_time],
                     # use remote Processor (Lambda) for the async logic to test Pause state
                     with_activated_processor=True,
                     processing_mode=FeedBackSignalProcessingMode.ONLY_HEAD,
                     target_route_id=internal_data.route_id)
        """
        app.platform.processor.process(
            app._check_upstream_signal(app._get_input_signal(external_trigger[test_time])),
            use_activated_instance=True,
            processing_mode=FeedBackSignalProcessingMode.ONLY_HEAD,
            target_route_id=internal_data.bound.route_id,
        )

        # give enough time to check the result
        time.sleep(30)

        # nothing has happened and won't happen due to PAUSE state.
        assert app.poll(internal_data[test_time]) == (None, None)

        # reload
        app = self._create_test_application("pause-resume")
        assert app.state == ApplicationState.PAUSED

        app.resume()

        # Currently RheocerOS does not buffer events received in PAUSED state.
        # (AWS Lambda driver relies on async event queue and as of 09/2021 Lambda change has changed the behaviour
        #  with concurreny=0 case, started dropping events rather than queueing up.)
        # So the goal here is to show that event is actually dropped while the app was paused and there is no trace of
        # an execution event after 5 mins.
        # with pytest.raises(AssertionError):
        #    self.poll(app=app, materialized_node=internal_data[test_time], expect_failure=False, duration=5 * 60)

        # clean up
        app.terminate()
        app.delete()
