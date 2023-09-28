# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from datetime import datetime, timedelta


def timer_to_pre_process_data_day(timer_date: datetime) -> datetime:
    return timer_date - timedelta(days=1)


def pre_process_data_day_to_timer(pre_process_date: datetime) -> datetime:
    return pre_process_date + timedelta(days=1)
