# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import re
from enum import Enum, unique
from typing import Dict, Optional, Union

from packaging import version
from packaging.version import Version

from intelliflow.core.platform.definitions.aws.glue.client_wrapper import GlueVersion, glue_spark_version_map
from intelliflow.core.signal_processing.definitions.compute_defs import Lang


@unique
class EmrJobLanguage(str, Enum):
    PYTHON = "py"
    SCALA = "scala"

    def __init__(self, extension="unknown"):
        self._extension = extension

    @property
    def extension(self) -> str:
        return self._extension

    @classmethod
    def from_slot_lang(cls, lang: Lang):
        if lang in [Lang.PYTHON, Lang.SPARK_SQL]:
            return cls.PYTHON
        elif lang == Lang.SCALA:
            return cls.SCALA
        else:
            raise ValueError(f"Slot lang '{lang!r}' is not supported by AWS EMR!")


@unique
class EmrReleaseLabel(Enum):
    """
    Versions have to be placed in ASC order
    """

    AUTO = None, None
    VERSION_5_12_3 = version.parse("5.12.3"), version.parse("2.2.1")
    VERSION_5_26_0 = version.parse("5.26.0"), version.parse("2.4.3")
    VERSION_6_3_1 = version.parse("6.3.1"), version.parse("3.1.1")

    def __init__(self, emr_version: Version, spark_version: Version):
        """
        EMR release label with corresponding software versions
        https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-app-versions-6.x.html
        https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-app-versions-5.x.html
        """
        self._emr_version = emr_version
        self._spark_version = spark_version

    @property
    def display_name(self) -> str:
        return self._emr_version.__str__()

    @property
    def extension(self) -> str:
        return f"emr_{self._emr_version.major}_{self._emr_version.minor}_{self._emr_version.patch}"

    @property
    def to_aws_label(self) -> str:
        """
        AWS EMR release label format: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-components.html
        """
        return f"emr-{self.display_name}"

    @property
    def spark_version(self) -> Version:
        return self._spark_version

    @classmethod
    def resolve_from_glue_version(cls, glue_version: GlueVersion) -> Optional["EmrReleaseLabel"]:
        """
        Find the lowest possible EMR release label to satisfy software versions from the input glue version
        """
        least_spark_version = glue_spark_version_map()[glue_version]
        if not least_spark_version:
            return None
        for release_label in EmrReleaseLabel:
            spark_version = release_label.spark_version
            if not spark_version:
                continue
            if least_spark_version <= spark_version:
                return release_label
        return None


def build_job_arn(region: str, account_id: str, job_id: str, partition: str = "aws") -> str:
    return f"arn:{partition}:elasticmapreduce:{region}:{account_id}:cluster/{job_id}"


def validate_job_name(job_name: str) -> bool:
    """
    EMR job name pattern from: https://docs.aws.amazon.com/emr/latest/APIReference/API_RunJobFlow.html#EMR-RunJobFlow-request-Name
    """
    return re.compile(r"^[\u0020-\uD7FF\uE000-\uFFFD\uD800\uDBFF-\uDC00\uDFFF\r\n\t]*$").match(job_name) is not None
