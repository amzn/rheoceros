# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import time

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
import logging

from intelliflow.core.application.core_application import ApplicationState
from intelliflow.utils.test.hook import GenericRoutingHookImpl, OnExecBeginHookImpl

flow.init_basic_logging()

from pyodinhttp import odin_material_retrieve

def poll(app, materialized_node, expect_failure=False, duration=3600):
    node = None
    if isinstance(materialized_node, MarshalingView):
        node = materialized_node.marshaler_node
    elif isinstance(materialized_node, MarshalerNode):
        node = materialized_node
    start = time.time()
    while True:
        path, records = app.poll(materialized_node)
        if records is not None:
            if expect_failure:
                assert not path, f"Expected failure but the node {node.bound!r} yielded success."
            else:
                assert path, f"Expected success but the node {node.bound!r} yielded failure."
            return path, records
        time.sleep(10)
        elapsed_time_in_secs = time.time() - start
        assert elapsed_time_in_secs < duration, f"Test failed due to timeout while polling on {node.bound!r}"


odin_ms = "com.amazon.access.DEXMLAWSDevAccount-IntelliFlowAdmin-1"
app = AWSApplication(app_name="hook-app-dev",
                      region="us-east-1",
                      access_id=odin_material_retrieve(odin_ms, 'Principal').decode('utf-8'),
                      access_key=odin_material_retrieve(odin_ms, 'Credential').decode('utf-8'))

if app.state != ApplicationState.INACTIVE:
    app.terminate()
    app = AWSApplication(app_name="hook-app-dev",
                         region="us-east-1",
                         access_id=odin_material_retrieve(odin_ms, 'Principal').decode('utf-8'),
                         access_key=odin_material_retrieve(odin_ms, 'Credential').decode('utf-8'))

ducsi_data = app.marshal_external_data(
    GlueTable("booker", "d_unified_cust_shipment_items", partition_keys=["region_id", "ship_day"])
    , "DEXML_DUCSI"
    , {
        'region_id': {
            'type': DimensionType.LONG,
            'ship_day': {
                'type': DimensionType.DATETIME,
                'format': '%Y-%m-%d'
            }
        }
    }
    , {
        '1': {
            '*': {
                'timezone': 'PST'
            }
        }
    },
    SignalIntegrityProtocol("FILE_CHECK", {"file": ["SNAPSHOT"]})
)

# add a dimensionless table (important corner-case)
ship_options = app.glue_table(database="dexbi", table_name="d_ship_option")

on_exec_begin_hook = OnExecBeginHookImpl()
on_exec_skipped_hook = GenericRoutingHookImpl()
on_compute_success_hook = GenericRoutingHookImpl()
on_success_hook = GenericRoutingHookImpl()
exec_checkpoints = [RouteCheckpoint(checkpoint_in_secs=60, slot=GenericRoutingHookImpl()),
                    RouteCheckpoint(checkpoint_in_secs=4 * 60, slot=GenericRoutingHookImpl())]

repeat_ducsi = app.create_data(id="REPEAT_DUCSI",
                               inputs={
                                   "DEXML_DUCSI": ducsi_data,
                               },
                               compute_targets="output=DEXML_DUCSI.limit(100)",
                               execution_hook=RouteExecutionHook(on_exec_begin=on_exec_begin_hook,
                                                                 on_exec_skipped=on_exec_skipped_hook,
                                                                 on_compute_success=on_compute_success_hook,
                                                                 on_success=on_success_hook,
                                                                 checkpoints=exec_checkpoints)
                               )

on_compute_failure_hook = GenericRoutingHookImpl()
on_failure_hook = GenericRoutingHookImpl()

# we will be using this second node for failure checks
failed_ducsi = app.create_data(id="FAIL_DUCSI",
                               inputs={
                                   "DEXML_DUCSI": ducsi_data
                               },
                               # bad Glue ETL code
                               compute_targets="boot me, boot me, boot me",
                               execution_hook=RouteExecutionHook(on_compute_failure=on_compute_failure_hook,
                                                                 on_failure=on_failure_hook),
                               )

on_pending_node_created_hook = GenericRoutingHookImpl()
on_expiration_hook = GenericRoutingHookImpl()
pending_node_checkpoints = [RouteCheckpoint(checkpoint_in_secs=60, slot=GenericRoutingHookImpl())]

# we will be using this third node for Pending Node checks mostly
app.create_data(id="DUCSI_WITH_SO",
                inputs={
                    "DEXML_DUCSI": ducsi_data,
                    "SHIP_OPTIONS": ship_options
                },
                compute_targets="output=DEXML_DUCSI.limit(100).join(SHIP_OPTIONS, DEXML_DUCSI.customer_ship_option == SHIP_OPTIONS.ship_option)",
                pending_node_hook=RoutePendingNodeHook(on_pending_node_created=on_pending_node_created_hook,
                                                       on_expiration=on_expiration_hook,
                                                       checkpoints=pending_node_checkpoints),
                pending_node_expiration_ttl_in_secs=3 * 60
                )

app.activate()

start = time.time()
# 1- Inject DUCSI event to trigger execution on the first node/route and create a pending node on the second.
app.process(ducsi_data[1]['2020-12-25'],
            # use the remote processor)
            with_activated_processor=True,
            # make it sync so that the following assertions won't fail due to the delay in event propagation.
            is_async=False)
time.sleep(5)
# check if the first exec hook has been hit and done with its own logic
assert on_exec_begin_hook.verify(app)
assert not any([c.slot.verify(app) for c in exec_checkpoints])
assert not any([c.slot.verify(app) for c in pending_node_checkpoints])
assert not on_exec_skipped_hook.verify(app)
assert not on_compute_failure_hook.verify(app)
assert not on_compute_success_hook.verify(app)
assert not on_success_hook.verify(app)
assert not on_failure_hook.verify(app)

# check the pending node hooks registered on the second route.
assert on_pending_node_created_hook.verify(app)
assert not on_expiration_hook.verify(app)
# check idempotency
app.process(ducsi_data[1]['2020-12-25'], with_activated_processor=True, is_async=False)
time.sleep(5)
# now we can check the skipped hook due to idempotency related call above
assert on_exec_skipped_hook.verify(app)
# wait till first execution succeeds
poll(app, repeat_ducsi[1]['2020-12-25'])
# wait till second execution on failed_ducsi fails
poll(app, failed_ducsi[1]['2020-12-25'], expect_failure=True)

assert on_compute_success_hook.verify(app)
assert on_success_hook.verify(app)
elapsed = time.time() - start
if elapsed < 4 * 60:
   # not very likely but just make sure that executions did not take less than last checkpoint's mark.
   time.sleep((4 * 60) - elapsed)
   app.update_active_routes_status()
assert all([c.slot.verify(app) for c in exec_checkpoints])
assert on_compute_failure_hook.verify(app)
assert on_failure_hook.verify(app)

# we now only have pending node and it must have checked all of its checkpoints and finally gotten expired.
assert on_expiration_hook.verify(app)
assert all([c.slot.verify(app) for c in pending_node_checkpoints])

app.terminate()
app.delete()

