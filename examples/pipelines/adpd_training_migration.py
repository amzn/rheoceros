# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from datetime import datetime

from intelliflow.core.platform.drivers.compute.aws import AWSGlueBatchComputeBasic

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
from intelliflow.core.platform.definitions.aws.glue.client_wrapper import GlueWorkerType

flow.init_basic_logging()

PIPELINE_DAY_DIMENSION="day"
ADPD_RANGE_IN_DAYS = 7
REGION_ID = 1
MARKETPLACE_ID = 1
ANALYTICS_DATA_BUCKET = "<YOUR_DATA_BUCKET>"
PERFECTMILE_TABLE_MAP = {1: "d_perfectmile_package_items_na", 2: "d_perfectmile_package_items_eu"}

app = AWSApplication("adpd-pipeline", HostPlatform(AWSConfiguration.builder()
                                                     .with_default_credentials(as_admin=True)
                                                     .with_region("us-east-1")
                                                      # disable CompositeBatchCompute (multiple BatchCompute drivers)
                                                      # , use Glue batch compute only.
                                                     .with_batch_compute(AWSGlueBatchComputeBasic)
                                                     .build()))

# daily timer
daily_timer = app.add_timer("daily_timer",
                            "rate(1 day)",
                            time_dimension_id=PIPELINE_DAY_DIMENSION)

denorm_archive_ad = app.create_data("denorm_archive_ad",
                                 inputs=[daily_timer],
                                 compute_targets=[
                                     BatchCompute(
                                         code=f"""
from datetime import datetime, timedelta
import dateutil.parser
from pyspark.sql.functions import col

day = dimensions["{PIPELINE_DAY_DIMENSION}"]
date = dateutil.parser.parse(day)

# get the last {ADPD_RANGE_IN_DAYS} days (starting from partition day and going backwards)
date_list = list(date - timedelta(days=i) for i in range({ADPD_RANGE_IN_DAYS}))

def create_path(date_list):
    date_list = [d.strftime('%Y-%m-%d') for d in date_list]
    denorm_archive_s3_path_start = 's3://{ANALYTICS_DATA_BUCKET}/denormalized_archive/creation_date='
    denorm_archive_s3_path_end = '/region_id={REGION_ID}/system=F2P/'
    s3_path = list()
    hours = ['-00', '-01', '-02', '-03', '-04', '-05', '-06', '-07', '-08', '-09', '-10', '-11', '-12', '-13', '-14',
             '-15', '-16', '-17', '-18', '-19', '-20', '-21', '-22', '-23']
    quarters = ['-00', '-15', '-30', '-45']
    for i in date_list:
        for j in hours:
            for k in quarters:
                s3_path.append(denorm_archive_s3_path_start + i + j + k + denorm_archive_s3_path_end)

    return s3_path

s3_paths = create_path(date_list)

# IF_MIGRATION_FIX sync with Anupama 'ready_date' and 'supply_type' were duplicates here
spark.read.parquet(*s3_paths).select("fc_name", "iaid", "planning_date", "ramp_cost", "order_id", "order_date", "ship_method",
                                              "processing_hold_date" , "expected_ship_date", "expected_arrival_date", "internal_pdd",
                                              "internal_psd", "fulfillment_request_id",  "marketplace_id", "context_name", "order_fta_flag",
                                              "fulfillment_fta_flag", "planned_shipment_id", "shipment_id", "redecide_flag",
                                              "fulfillment_reference_id", "ship_cpu", "zip_code", "supply_type", "transship_from", 
                                              "transship_cpt",  "shipgen_deadline", "gl_product_group", "release_date", "split_promise",
                                              "hazmat_flag", "atrops_penalty_cost", "delivery_promise_authoritative","box_type","boxes_per_plan",
                                              "our_price", "fc_type", "boxes_per_order", "promises_per_order","supply_time", "line_item_id",
                                              "address_id", "units_per_plan", "gift_wrap_flag", "surety_duration",  "delivery_group",
                                              "units_per_shipment", "promise_quality", "supply_ft_flag", "iog", "orders_per_shipment",
                                              "box_dimensions", "box_volume", "ship_weight", "pallet_ship_method_flag", "processing_hold_type",
                                              "plan_transship_cost", "plan_opportunity_cost", "previous_fc", "ready_date", "redecide_reason", 
                                              "ship_promise_authoritative", "units_per_order", "delivery_oriented_flag", "ship_option" ) \\
                             .createOrReplaceTempView('denorm_archive')

output = spark.sql(
    "select * from denorm_archive where ship_option = 'second-nominated-day' and marketplace_id={MARKETPLACE_ID} and context_name = 'f2p_production' ")
                                         """,
                                         extra_permissions=[Permission(resource=['arn:aws:s3:::<AWAY_TEAM_S3_BUCKET>/denormalized_archive/*'],
                                                                       action=['s3:Get*'])
                                                            ],
                                         lang=Lang.PYTHON,
                                         GlueVersion="2.0",
                                         WorkerType=GlueWorkerType.G_1X.value,
                                         NumberOfWorkers=200,
                                         Timeout=10 * 60  # 10 hours
                                     )
                                 ])


#data, format = app.preview_data(denorm_archive_ad['2021-01-12'], limit=2)

d_perfectmile_package_items_na = app.glue_table(database="perfectmile"
                                                , table_name=PERFECTMILE_TABLE_MAP.get(REGION_ID)
                                                , id="d_perfectmile_package_items_na"
                                                , dimension_spec={
                                                    PIPELINE_DAY_DIMENSION: {
                                                        type: DimensionType.DATETIME,
                                                        "format": "%Y-%m-%d"
                                                    }
                                                }
                                                , dimension_filter={
                                                    '*': {
                                                    }
                                                }
                                            )

dppi_df = app.create_data("dppi_df",
                          inputs={"timer": daily_timer,
                                  # read the last 7 days from the day of timer
                                  "dppi_low_range": d_perfectmile_package_items_na[:-ADPD_RANGE_IN_DAYS].ref,
                                  # try to read 14 days into the future for shipment data
                                  "dppi_high_range": d_perfectmile_package_items_na[:14].ref
                                  },
                          compute_targets=[
                              BatchCompute(
                                  code=f"""
import dateutil.parser
from datetime import datetime, timedelta

start_day = dimensions["{PIPELINE_DAY_DIMENSION}"]
start_date = dateutil.parser.parse(start_day)

end_date = start_date - timedelta({ADPD_RANGE_IN_DAYS})
end_day = end_date.strftime('%Y-%m-%d')

dppi_low_range.unionAll(dppi_high_range).createOrReplaceTempView('dppi_full_range')

output = spark.sql(f'''
SELECT ordering_order_id,
    customer_order_item_id,
    ship_day, 
    rcnt_supply_category_type_id, 
    asin, 
    order_datetime, 
    fulfillment_reference_id, 
    is_instock, 
    carrier_sort_code, 
    clock_stop_event_datetime_utc, 
    warehouse_id, 
    package_id, 
    internal_promised_delivery_date_item, 
    ship_method, 
    customer_ship_option, 
    quantity, 
    ship_date_utc, 
    is_hb, 
    promise_data_source, 
    planned_shipment_id, 
    fulfillment_shipment_id, 
    estimated_arrival_date, 
    event_timezone, 
    marketplace_id, 
    ship_option_group, 
    customer_shipment_item_id
    FROM dppi_full_range
        WHERE 1=1
            and region_id = {REGION_ID}
            AND marketplace_id = {MARKETPLACE_ID} 
            and customer_ship_option = 'second-nominated-day'
            and cast(order_datetime as date) >= "{{end_day}}"
            and cast(order_datetime as date) <= "{{start_day}}"
                   ''')

                         """,
                            lang=Lang.PYTHON,
                            retry_count=2,
                            GlueVersion="1.0",
                            WorkerType=GlueWorkerType.G_1X.value,
                            NumberOfWorkers=100,
                            Timeout=8 * 60
                        )
                    ])

slam_broadcast = app.create_data("slam_broadcast",
                          inputs=[dppi_df],
                          compute_targets=[
                              BatchCompute(
                                  code=f"""

output = spark.sql("select ordering_order_id, asin, max(planned_shipment_id) as planned_shipment_id,max(FULFILLMENT_REFERENCE_ID) as FULFILLMENT_REFERENCE_ID from dppi_df  group by ordering_order_id, asin")

                                         """,
                                  lang=Lang.PYTHON,
                                  GlueVersion="2.0",
                                  WorkerType=GlueWorkerType.G_1X.value,
                                  NumberOfWorkers=150,
                                  Timeout=8 * 60
                              )
                          ])

plans_df = app.create_data("plans_df",
                             inputs=[denorm_archive_ad,
                                     slam_broadcast],
                             compute_targets=[
                                 BatchCompute(
                                     code=f"""

output = spark.sql("select order_id, iaid, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN a.marketplace_id END) as fta_MARKETPLACE_ID, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN fc_name END) as FTA_FC_NAME, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN A.zip_code END) as FTA_zip_code,  MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN a.ship_method END) as FTA_ship_method, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN planning_date END) as FTA_planning_date, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN supply_type END) as FTA_supply_type, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN transship_from END) as FTA_transship_from,  MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN ship_cpu END) as FTA_ship_cpu, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN transship_cpt END) as FTA_transship_cpt, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN order_date END) as FTA_order_date, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN internal_psd END) as FTA_internal_psd, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN internal_pdd END) as FTA_internal_pdd, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN expected_ship_date END) as FTA_expected_ship_date, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN expected_arrival_date END) as FTA_expected_arrival_date, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN ready_date END) as FTA_ready_date, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN shipgen_deadline END) as FTA_shipgen_deadline, CASE WHEN MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN TRANSSHIP_FROM END) IS NOT NULL THEN 'Y' ELSE 'N' END AS IS_FTA_TRANSSHIP, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.marketplace_id END) as LTA_MARKETPLACE_ID, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.fc_name END) as LTA_fc_name, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.zip_code END) as LTA_zip_code, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.ship_method END) as LTA_ship_method, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.planning_date END) as LTA_planning_date, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.supply_type END) as LTA_supply_type,  max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.transship_from END) as LTA_transship_from, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.ship_cpu END) as LTA_ship_cpu,  max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.box_volume END) as LTA_box_volume, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.processing_hold_type END) as LTA_processing_hold_type, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.ship_weight END) as LTA_ship_weight, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.order_date END) as LTA_order_date, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.promise_quality END) as LTA_promise_quality, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.internal_psd END) as LTA_internal_psd, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.internal_pdd END) as LTA_internal_pdd, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.expected_ship_date END) as LTA_expected_ship_date, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.expected_arrival_date END) as LTA_expected_arrival_date, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.gl_product_group END) as LTA_gl_product_group, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.ready_date END) as LTA_ready_date, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.fulfillment_request_id END) as lta_fulfillment_request_id, MIN( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.shipgen_deadline END) as LTA_shipgen_deadline, CASE WHEN MAX( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN TRANSSHIP_FROM END) IS NOT NULL THEN 'Y' ELSE 'N' END as IS_LTA_TRANSSHIP, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.fc_type END) as LTA_fc_type, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.our_price END) as lta_our_price, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.delivery_group END) as lta_delivery_group, MAX( CASE WHEN redecide_reason in ( 'MissedAllocationDeadline', 'FasttrackMissedAllocationDeadline', 'MissShipGenDeadline' , 'MissedShipGenDeadliFasttrackMissedShipgenDeadline' ) then 'Y' else 'N' END) as PLAN_EVER_MISSED_DEADLINE from denorm_archive_ad a join slam_broadcast b on b.ordering_order_id=a.order_id and iaid = b.asin group by order_id, iaid")


                                     """,
                                     lang=Lang.PYTHON,
                                     GlueVersion="2.0",
                                     WorkerType=GlueWorkerType.G_1X.value,
                                     NumberOfWorkers=150,
                                     Timeout=15 * 60
                                 )
                             ])


slam_data_transformed = app.create_data("slam_data_transformed",
                           inputs=[dppi_df],
                           compute_targets=[
                               BatchCompute(
                                   code=f'''

output = spark.sql("""
    SELECT
        marketplace_id
        , ordering_order_id
        , asin
        , ship_day
        , fulfillment_shipment_id
        , fulfillment_reference_id
        , ship_date_utc
        ,from_utc_timestamp(ship_date_utc, 'PST') as ship_date_local
        , planned_shipment_id
        , warehouse_id
        , order_datetime
        , ship_method
        , quantity
        , package_id
        , customer_shipment_item_id
        , customer_ship_option
        , carrier_sort_code
        ,internal_promised_delivery_date_item
        , estimated_arrival_date
        , clock_stop_event_datetime_utc
        ,CASE WHEN clock_stop_event_datetime_utc is null THEN 'NoScan' WHEN clock_stop_event_datetime_utc is not null and cast(from_utc_timestamp(clock_stop_event_datetime_utc, 'PST') as date)  > cast(internal_promised_delivery_date_item as date) THEN 'IntLate' WHEN clock_stop_event_datetime_utc is not null and cast(from_utc_timestamp(clock_stop_event_datetime_utc, 'PST') as date)  < cast(internal_promised_delivery_date_item as date) THEN 'IntEarly' WHEN clock_stop_event_datetime_utc is not null and cast(from_utc_timestamp(clock_stop_event_datetime_utc, 'PST') as date)  = cast(internal_promised_delivery_date_item as date) THEN 'IntOnTime' END AS int_dea_status
    from dppi_df dosi""")
                                     ''',
                                   lang=Lang.PYTHON,
                                   GlueVersion="2.0",
                                   WorkerType=GlueWorkerType.G_1X.value,
                                   NumberOfWorkers=100,
                                   Timeout=10 * 60
                               )
                           ])

ramp = app.create_data("ramp",
                        inputs=[slam_data_transformed, plans_df],
                        compute_targets=[
                            BatchCompute(
                                code=f'''

output = spark.sql("""
    SELECT 
        order_day,
        ship_day,
        ship_date_utc,
        ship_date_local
        , lta_fc_type
        ,dosi.order_id
        ,dosi.asin
        ,ship_method as dosi_ship_method
        ,LTA_zip_code
        ,LTA_planning_date
        ,FTA_supply_type
        ,LTA_supply_type
        ,lta_ship_method
        ,lta_gl_product_group
        ,lta_ship_cpu
        ,fulfillment_shipment_id
        ,lta_fulfillment_request_id
        ,lta_promise_quality
        ,lta_ship_weight
        ,lta_box_volume
        ,lta_processing_hold_type
        ,lta_delivery_group
        ,lta_our_price
        , case 
                when ship_method = 'AMZL_US_PREMIUM_AIR' then 'AMZL_AIR'
                when ship_method = 'AMZL_US_PREMIUM' then 'AMZL_GROUND'
                when substring(ship_method,0,4) = 'AMZL' then 'AMZL_Other'
                when substring(ship_method,0,4) = 'UPS_' then 'UPS'
                when substring(ship_method,0,5) = 'FEDEX' then 'FEDEX'
                when substring(ship_method,0,5) = 'USPS_' then 'USPS'
                else 'Others' end as dosi_carrier
        ,case 
                when lta_ship_method = 'AMZL_US_PREMIUM_AIR' then 'AMZL_AIR'
                when lta_ship_method = 'AMZL_US_PREMIUM' then 'AMZL_GROUND'
                when substring(lta_ship_method,0,4) = 'AMZL' then 'AMZL_Other'
                when substring(lta_ship_method,0,4) = 'UPS_' then 'UPS'
                when substring(lta_ship_method,0,5) = 'FEDEX' then 'FEDEX'
                when substring(lta_ship_method,0,5) = 'USPS_' then 'USPS'
                else 'Others' end as lta_carrier
        ,case when ship_method IN ('AMZL_US_LMA_AIR','AMZL_US_PREMIUM_AIR','UPS_2ND','UPS_2ND_COM','UPS_2ND_COM_SIG','UPS_2ND_DAY','UPS_2ND_SAT','UPS_2ND_SIG','UPS_AIR_DIRECT','UPS_NEXT',
       'UPS_NEXT_COM','UPS_NEXT_COM_SIG','UPS_NEXT_DAY','UPS_NEXT_DAY_SAVER','UPS_NEXT_SAT','UPS_NEXT_SAT_SIG','UPS_NEXT_SAVER','UPS_NEXT_SAVER_COM','UPS_NEXT_SAVER_COM_SIG','UPS_NEXT_SIG',
        'UPS_NXT_SVR_SIG','UPS_SAT_2ND','UPS_SAT_2ND_COM','UPS_SAT_2ND_SIG','USPS_ATS_BPM_AIR','USPS_ATS_PARCEL_AIR','USPS_ATS_STD_AIR')   then 'Air' else 'Ground' end as Mode
        ,warehouse_id as dosi_warehouse_id
        , lta_fc_name
        , int_dea_status 
        ,from_utc_timestamp(lta_internal_psd, 'PST') as lta_internal_psd_local,
        from_utc_timestamp(lta_internal_pdd, 'PST') as lta_internal_pdd_local,
        from_utc_timestamp(LTA_expected_ship_date, 'PST') as LTA_expected_ship_date_local,
           from_utc_timestamp(LTA_expected_arrival_date, 'PST') as LTA_expected_arrival_date_local,
        internal_promised_delivery_date_item,
        estimated_arrival_date,
        clock_stop_event_datetime_utc,
        is_fta_transship,
        is_lta_transship,
        PLAN_EVER_MISSED_DEADLINE,
        quantity
        from
         (  select warehouse_id,
                cast(order_datetime as date) as order_day,
                ship_day,
                ship_date_utc,
                ship_date_local,
                ship_method,
                ordering_order_id as order_id
                , asin
                , int_dea_status 
                ,internal_promised_delivery_date_item
                ,estimated_arrival_date
                ,clock_stop_event_datetime_utc
                ,fulfillment_shipment_id
                ,sum(quantity) as quantity
            from slam_data_transformed
            group by ordering_order_id
                ,ship_day
                ,ship_date_utc
                ,ship_date_local
                , asin
                , int_dea_status 
                ,warehouse_id
                ,ship_method
                ,internal_promised_delivery_date_item
                ,estimated_arrival_date
                ,clock_stop_event_datetime_utc
                ,fulfillment_shipment_id
                ,cast(order_datetime as date)
          ) dosi
        inner join 
         plans_df plans
        ON dosi.ORDER_ID = plans.ORDER_ID AND dosi.ASIN = plans.iaid
        where dosi.asin is not null
        and fta_planning_date is not null
        and lta_planning_date is not null
    """)
                                     ''',
                                                lang=Lang.PYTHON,
                                                GlueVersion="2.0",
                                                WorkerType=GlueWorkerType.G_1X.value,
                                                NumberOfWorkers=100,
                                                Timeout=10 * 60
                                            )
                                        ])

slam_broadcast = app.update_data("slam_broadcast",
                                 inputs=[plans_df],
                                 compute_targets=[
                                     BatchCompute(
                                         code=f"""

output = spark.sql("select ordering_order_id, asin, max(planned_shipment_id) as planned_shipment_id,max(FULFILLMENT_REFERENCE_ID) as FULFILLMENT_REFERENCE_ID from dppi_df  group by ordering_order_id, asin")

                                         """,
                                         lang=Lang.PYTHON,
                                         GlueVersion="2.0",
                                         WorkerType=GlueWorkerType.G_1X.value,
                                         NumberOfWorkers=150,
                                         Timeout=8 * 60
                                     )
                                 ])

app.activate()

app.process(daily_timer[datetime(2021, 1, 16)])
# feed daily timer but target only 'denorm_archive_ad' (won't trigger dppi_df)
# app.process(daily_timer[datetime(2021, 1, 16)], target_route_id=denorm_archive_ad)

#app.execute(plans_df[datetime(2021, 1, 16)])
