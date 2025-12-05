import pytest

from intelliflow.api_ext import *
from intelliflow.mixins.aws.test import AWSTestBase
from intelliflow.plugins.diagnostics.availability_alarm import create_availability_alarm, create_availability_alarm_extended
from intelliflow.plugins.diagnostics.orchestration_alarms import create_orchestration_alarms
from intelliflow.plugins.diagnostics.system_alarms import create_system_alarms
from intelliflow.utils.test.inlined_compute import NOOPCompute


class TestAWSApplicationDiagnosticsPlugins(AWSTestBase):
    """
    This test suite was created to cover plugins/diagnostics against an AWSApplication
    """

    @pytest.mark.skip(reason="TODO temporarily due to moto upgrade. getting serialization error in remote builds during merge/build.")
    def test_application_diagnostics_plugins_creation(self):
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True)

        app = AWSApplication("tst_sys_alarms", self.region)

        node = app.create_data(
            id="node_1",
            compute_targets=[NOOPCompute],  # do nothing
        )

        cwa_cti = CWAInternal.CTI(category="Delivery Experience", type="Machine Learning", item="IntelliFlow")

        create_system_alarms(app, cwa_cti, ticketing_enabled=True)

        create_orchestration_alarms(app, cwa_cti, ticketing_enabled=True)

        create_availability_alarm(app, node, cwa_cti, ticketing_enabled=True)

        # Extended Alarm
        daily_timer = app.add_timer("availability_check_timer", "rate(1 day)")
        node2 = app.create_data(
            id="node_2",
            inputs=[daily_timer],
            compute_targets=[NOOPCompute],  # do nothing
        )
        ALARM_MONITORING_PERIOD_IN_DAYS = 7
        create_availability_alarm_extended(
            app,
            daily_timer,
            node2[:-ALARM_MONITORING_PERIOD_IN_DAYS],
            alarm_period_in_hours=ALARM_MONITORING_PERIOD_IN_DAYS * 24,
            # any compute target can be used as an alarm_action (Slack, Email, ComputeTarget impl)
            alarm_action=NOOPCompute,
        )

        self.patch_aws_stop()
