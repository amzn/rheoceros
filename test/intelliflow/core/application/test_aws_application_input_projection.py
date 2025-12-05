import time

from intelliflow.api_ext import *
from intelliflow.mixins.aws.test import AWSTestBase
from intelliflow.utils.test.inlined_compute import NOOPCompute


class TestAWSApplicationProject(AWSTestBase):
    """
    This module tests Application::project API
    """

    def test_application_project_timer_fanout(self):
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True)

        app = AWSApplication("project-timer", self.region)

        daily_timer = app.add_timer(
            "DAILY_TIMER",
            schedule_expression="cron(0 0 1 * ? *)",
            # Run at 00:00 am (UTC) every 1st day of the month
            time_dimension_id="date",
        )

        regionalized_timer = app.project(
            "REGIONAL_DAILY_TIMER",
            input=daily_timer,
            output_dimension_spec={
                "region_id": {
                    type: DimensionType.LONG,
                    "marketplace_id": {type: DimensionType.LONG, "date": {type: DimensionType.DATETIME, "format": "%Y-%m-%d"}},
                }
            },
            output_dimension_filter={
                1: {1: {"*": {}}, 771770: {"*": {}}},  # NA  # US  # will be retrieved from daily_timer("date") at runtime  # MEX
                2: {3: {"*": {}}, 4: {"*": {}}},  # EU  # UK  # DE
            },
        )

        regional_daily_job = app.create_data("REGIONAL_JOB", inputs=[regionalized_timer], compute_targets=[NOOPCompute])
        app.activate()

        # make sure orchestration loop is active locally
        # otherwise propagation from timer projection node ("REGIONAL_DAILY_TIMER") to "REGIONAL_JOB" would not work.
        orchestration_cycle_time_in_secs = 5
        self.activate_event_propagation(app, cycle_time_in_secs=orchestration_cycle_time_in_secs)

        # first verify non-existence of partitions
        assert app.poll(regional_daily_job[1][1]["2023-10-17"]) == (None, None)
        assert app.poll(regional_daily_job[1][771770]["2023-10-17"]) == (None, None)
        assert app.poll(regional_daily_job[2][3]["2023-10-17"]) == (None, None)
        assert app.poll(regional_daily_job[2][4]["2023-10-17"]) == (None, None)

        # now simulate timer trigger
        app.process(daily_timer["2023-10-17"])

        time.sleep(2 * orchestration_cycle_time_in_secs)

        path, _ = app.poll(regional_daily_job[1][1]["2023-10-17"])
        assert path.endswith("REGIONAL_JOB/1/1/2023-10-17")

        path, _ = app.poll(regional_daily_job[1][771770]["2023-10-17"])
        assert path.endswith("REGIONAL_JOB/1/771770/2023-10-17")

        path, _ = app.poll(regional_daily_job[2][3]["2023-10-17"])
        assert path.endswith("REGIONAL_JOB/2/3/2023-10-17")

        path, _ = app.poll(regional_daily_job[2][4]["2023-10-17"])
        assert path.endswith("REGIONAL_JOB/2/4/2023-10-17")

        # now check recursive execution, it should backfill only for the requested (inputted) dimensions here
        app.execute(regional_daily_job[2][4]["2023-10-15"], recursive=True)

        path, _ = app.poll(regional_daily_job[2][4]["2023-10-15"])
        assert path.endswith("REGIONAL_JOB/2/4/2023-10-15")

        # other region-mp pairs must be absent (recursive execution don't triggers projection nodes)
        assert app.poll(regional_daily_job[1][1]["2023-10-15"]) == (None, None)
        assert app.poll(regional_daily_job[1][771770]["2023-10-15"]) == (None, None)
        assert app.poll(regional_daily_job[2][3]["2023-10-15"]) == (None, None)

        assert not app.get_active_routes(), "There should not be any left-over active records after recursive execution!"

        self.patch_aws_stop()

    def test_application_project_timer_fanout_with_shift(self):
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True)

        app = AWSApplication("project-shift", self.region)

        daily_timer = app.add_timer(
            "DAILY_TIMER",
            schedule_expression=f"cron(0 1 * * ? *)",  # every day UTC 23:00
            # Run at 00:00 am (UTC) every 1st day of the month
            time_dimension_id="date",
            time_dimension_granularity=DatetimeGranularity.DAY,
            time_dimension_shift=-6,
        )

        regionalized_timer = app.project(
            "REGIONAL_DAILY_TIMER",
            input=daily_timer,
            output_dimension_spec={
                "region_id": {
                    type: DimensionType.LONG,
                    "marketplace_id": {type: DimensionType.LONG, "date": {type: DimensionType.DATETIME, "format": "%Y-%m-%d"}},
                }
            },
            output_dimension_filter={
                1: {1: {"*": {}}, 771770: {"*": {}}},  # NA  # US  # will be retrieved from daily_timer("date") at runtime  # MEX
                2: {3: {"*": {}}, 4: {"*": {}}},  # EU  # UK  # DE
            },
        )

        regional_daily_job = app.create_data("REGIONAL_JOB", inputs=[regionalized_timer], compute_targets=[NOOPCompute])
        app.activate()

        # make sure orchestration loop is active locally
        # otherwise propagation from timer projection node ("REGIONAL_DAILY_TIMER") to "REGIONAL_JOB" would not work.
        orchestration_cycle_time_in_secs = 5
        self.activate_event_propagation(app, cycle_time_in_secs=orchestration_cycle_time_in_secs)

        # now simulate timer trigger
        # app.process(daily_timer["2025-05-07"])
        app.process(
            {
                "version": "0",
                "id": "7c289f9d-0d83-e481-8279-257acdc04fac",
                "detail-type": "Scheduled Event",
                "source": "aws.events",
                "account": self.account_id,
                "time": "2025-05-07T23:00:00Z",
                "region": "us-east-1",
                "resources": [f"arn:aws:events:us-east-1:{self.account_id}:rule/project-shift-DAILY_TIMER"],
                "detail": {},
            }
        )

        time.sleep(2 * orchestration_cycle_time_in_secs)

        path, _ = app.poll(regional_daily_job[1][1]["2025-05-01"])
        assert path.endswith("REGIONAL_JOB/1/1/2025-05-01")

        self.patch_aws_stop()
