# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List

LOG_GROUPS = ["output", "error"]


def generate_loggroup_name(typ: str) -> str:
    return f"/aws-glue/jobs/{typ}"


def generate_logstream_urls(region: str, job_run_id: str) -> List[str]:
    return [
        f"https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}#logsV2:log-groups/log-group/$252Faws-glue$252Fjobs$252F{log_group}$3FlogStreamNameFilter$3D{job_run_id}"
        for log_group in LOG_GROUPS
    ]
