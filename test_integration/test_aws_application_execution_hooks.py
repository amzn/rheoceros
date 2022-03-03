# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import time

from intelliflow.api_ext import *
from intelliflow.core.application.core_application import ApplicationState
from intelliflow.mixins.aws.integ_test import AWSIntegTestMixin
from intelliflow.utils.test.hook import GenericRoutingHookImpl, OnExecBeginHookImpl


class TestAWSApplicationExecutionHooks(AWSIntegTestMixin):
    def setup(self):
        super().setup("IntelliFlow")

    def test_application_hooks(self):
        from test.intelliflow.core.application.test_aws_application_execution_hooks import TestAWSApplicationExecutionHooks

        app = super()._create_test_application("hook-app")
        if app.state != ApplicationState.INACTIVE:
            # make re-testing hassle-free following a failed test.
            # the sequence here relies on a brand-new app.
            app.terminate()
            app.delete()
            app = super()._create_test_application("hook-app")
        app = TestAWSApplicationExecutionHooks._create_test_application(self, app)

        ducsi_data = app.get_data("DEXML_DUCSI", context=Application.QueryContext.DEV_CONTEXT)[0]
        ship_options = app.get_data("ship_options", context=Application.QueryContext.DEV_CONTEXT)[0]

        on_exec_begin_hook = OnExecBeginHookImpl()
        on_exec_skipped_hook = GenericRoutingHookImpl()
        on_compute_success_hook = GenericRoutingHookImpl()
        on_success_hook = GenericRoutingHookImpl()
        exec_checkpoints = [
            RouteCheckpoint(checkpoint_in_secs=60, slot=GenericRoutingHookImpl()),
            RouteCheckpoint(checkpoint_in_secs=4 * 60, slot=GenericRoutingHookImpl()),
        ]

        repeat_ducsi = app.create_data(
            id="REPEAT_DUCSI",
            inputs={
                "DEXML_DUCSI": ducsi_data,
            },
            compute_targets="output=DEXML_DUCSI.limit(100)",
            execution_hook=RouteExecutionHook(
                on_exec_begin=on_exec_begin_hook,
                on_exec_skipped=on_exec_skipped_hook,
                on_compute_success=on_compute_success_hook,
                on_success=on_success_hook,
                checkpoints=exec_checkpoints,
            ),
        )

        on_compute_failure_hook = GenericRoutingHookImpl()
        on_compute_retry_hook = GenericRoutingHookImpl()
        on_failure_hook = GenericRoutingHookImpl()

        # we will be using this second node for failure checks
        failed_ducsi = app.create_data(
            id="FAIL_DUCSI",
            inputs={"DEXML_DUCSI": ducsi_data},
            # bad Glue ETL code
            compute_targets=[GlueBatchCompute(code="boot me, boot me, boot me", retry_count=1)],
            execution_hook=RouteExecutionHook(
                on_compute_failure=on_compute_failure_hook, on_compute_retry=on_compute_retry_hook, on_failure=on_failure_hook
            ),
        )

        on_pending_node_created_hook = GenericRoutingHookImpl()
        on_expiration_hook = GenericRoutingHookImpl()
        pending_node_checkpoints = [RouteCheckpoint(checkpoint_in_secs=60, slot=GenericRoutingHookImpl())]

        # we will be using this third node for Pending Node checks mostly
        app.create_data(
            id="DUCSI_WITH_SO",
            inputs={"DEXML_DUCSI": ducsi_data, "SHIP_OPTIONS": ship_options},
            compute_targets="output=DEXML_DUCSI.limit(100).join(SHIP_OPTIONS, DEXML_DUCSI.customer_ship_option == SHIP_OPTIONS.ship_option)",
            pending_node_hook=RoutePendingNodeHook(
                on_pending_node_created=on_pending_node_created_hook, on_expiration=on_expiration_hook, checkpoints=pending_node_checkpoints
            ),
            pending_node_expiration_ttl_in_secs=3 * 60,
        )

        app.activate()

        start = time.time()
        # 1- Inject DUCSI event to trigger execution on the first node/route and create a pending node on the second.
        app.process(
            ducsi_data[1]["2020-12-25"],
            # use the remote processor)
            with_activated_processor=True,
            # make it sync so that the following assertions won't fail due to the delay in event propagation.
            is_async=False,
        )
        time.sleep(5)

        # check if the first exec hook has been hit and done with its own logic
        assert on_exec_begin_hook.verify(app)
        assert not any([c.slot.verify(app) for c in exec_checkpoints])
        assert not any([c.slot.verify(app) for c in pending_node_checkpoints])
        assert not on_exec_skipped_hook.verify(app)
        assert not on_compute_failure_hook.verify(app)
        assert not on_compute_retry_hook.verify(app)
        assert not on_compute_success_hook.verify(app)
        assert not on_success_hook.verify(app)
        assert not on_failure_hook.verify(app)

        # check the pending node hooks registered on the second route.
        assert on_pending_node_created_hook.verify(app)
        assert not on_expiration_hook.verify(app)
        # check idempotency
        app.process(ducsi_data[1]["2020-12-25"], with_activated_processor=True, is_async=False)
        time.sleep(5)
        # now we can check the skipped hook due to idempotency related call above
        assert on_exec_skipped_hook.verify(app)
        # wait till first execution succeeds
        self.poll(app, repeat_ducsi[1]["2020-12-25"])
        # wait till second execution on failed_ducsi fails
        self.poll(app, failed_ducsi[1]["2020-12-25"], expect_failure=True)

        assert on_compute_success_hook.verify(app)
        assert on_success_hook.verify(app)
        # elapsed = time.time() - start
        # if elapsed < 4 * 60:
        #    # not very likely but just make sure that executions did not take less than last checkpoint's mark.
        #    time.sleep((4 * 60) - elapsed)
        #    app.update_active_routes_status()
        assert all([c.slot.verify(app) for c in exec_checkpoints])
        assert on_compute_failure_hook.verify(app)
        assert on_compute_retry_hook.verify(app)
        assert on_failure_hook.verify(app)

        # we now only have pending node and it must have checked all of its checkpoints and finally gotten expired.
        assert on_expiration_hook.verify(app)
        assert all([c.slot.verify(app) for c in pending_node_checkpoints])

        app.terminate()
        app.delete()
