# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from datetime import datetime, timedelta, timezone

from intelliflow.api_ext import *


def create(
    app: AWSApplication,
    tommy_external: MarshalerNode,
    dama: MarshalerNode,
    xdf: MarshalerNode,
    pdex_consolidated: MarshalerNode,
    region: str,
    region_id: int,
    marketplace_id: int,
) -> MarshalerNode:
    return app.create_data(
        id=f"C2P_TOMMY_DATA_SCALA_{region}_{marketplace_id}",
        inputs={
            "tommy_external": tommy_external["US"]["*"]["*"].ref.range_check(True),
            "digital_external": dama[region_id].ref,
            "xdf_external": xdf["NA"][:-14].nearest(),
            "pdex_consolidated": pdex_consolidated[region_id][marketplace_id]["*"]["*"],
        },
        input_dim_links=[
            (
                tommy_external("partition_date"),
                lambda day_in_utc, hour_in_utc: datetime(
                    day_in_utc.year, day_in_utc.month, day_in_utc.day, hour_in_utc, tzinfo=timezone.utc
                ).astimezone(timezone(timedelta(hours=-7))),
                pdex_consolidated("day", "hour"),
            ),
            (
                tommy_external("partition_hour"),
                lambda day_in_utc, hour_in_utc: datetime(
                    day_in_utc.year, day_in_utc.month, day_in_utc.day, hour_in_utc, tzinfo=timezone.utc
                ).astimezone(timezone(timedelta(hours=-7))),
                pdex_consolidated("day", "hour"),
            ),
            # provide reverse links for testing convenience (from console.py or in the notebooks) when tommy external
            # is provided as seed for execute or process APIs.
            (
                pdex_consolidated("day"),
                lambda t_d: datetime(t_d.year, t_d.month, t_d.day, t_d.hour, tzinfo=timezone(timedelta(hours=-7))).astimezone(timezone.utc),
                tommy_external("partition_date"),
            ),
            (
                pdex_consolidated("hour"),
                lambda t_h: datetime(t_h.year, t_h.month, t_h.day, t_h.hour, tzinfo=timezone(timedelta(hours=-7)))
                .astimezone(timezone.utc)
                .hour,
                tommy_external("partition_hour"),
            ),
            (
                xdf("day"),
                lambda day_in_utc, hour_in_utc: datetime(
                    day_in_utc.year, day_in_utc.month, day_in_utc.day, hour_in_utc, tzinfo=timezone.utc
                ).astimezone(timezone(timedelta(hours=-7))),
                pdex_consolidated("day", "hour"),
            ),
        ],
        output_dimension_spec={
            "region_id": {
                type: DimensionType.LONG,
                "marketplace_id": {
                    type: DimensionType.LONG,
                    "day": {"format": "%Y-%m-%d", type: DimensionType.DATETIME, "hour": {type: DimensionType.LONG}},
                },
            }
        },
        output_dim_links=[
            ("region_id", EQUALS, region_id),
            ("marketplace_id", EQUALS, marketplace_id),
            ("day", EQUALS, tommy_external("partition_date")),
            ("hour", lambda hour_dt: hour_dt.hour, tommy_external("partition_hour")),
            # define the reverse link for testing and debugging convenience
            #  output("day", "hour") -> tommy_external("hour")
            (tommy_external("partition_hour"), lambda o_day, o_hour: datetime(o_day.year, o_day.month, o_day.day, o_hour), ("day", "hour")),
        ],
        compute_targets=[
            BatchCompute(
                code=scala_script(
                    """
            val tommy = tommy_external.limit(10)
            val tommy_count = tommy.count()
            val dama = digital_external.limit(10).count()
            val xdf = xdf_external.limit(10).count()
            val pdex = pdex_consolidated.limit(10).count()
            tommy 
                    """
                ),
                lang=Lang.SCALA,
                GlueVersion="1.0",
                WorkerType=GlueWorkerType.G_1X.value,
                NumberOfWorkers=150,
                Timeout=4 * 60,  # 4 hours
            )
        ],
        dataset_format=DataFormat.PARQUET,
        auto_input_dim_linking_enabled=False,
    )
