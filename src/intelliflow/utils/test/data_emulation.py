# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Union

from boto3 import Session

from intelliflow.core.application.application import Application
from intelliflow.core.application.context.node.filtered_views import FilteredView
from intelliflow.core.application.context.node.marshaling.nodes import MarshalerNode
from intelliflow.core.platform.constructs import Storage
from intelliflow.core.platform.definitions.aws.s3.bucket_wrapper import bucket_exists, create_bucket, get_bucket
from intelliflow.core.platform.definitions.aws.s3.object_wrapper import object_exists, put_object
from intelliflow.core.platform.drivers.aws_common import AWSConstructMixin
from intelliflow.core.signal_processing.signal_source import (
    InternalDatasetSignalSourceAccessSpec,
    S3SignalSourceAccessSpec,
    SignalSourceType,
)


# TODO can be made part of a data emulation mixin
def add_test_data(
    app: Application,
    signal_view: Union[FilteredView, MarshalerNode],
    object_name: str,
    data: Union[str, bytes],
    subfolder: str = None,
    emit_signal: bool = False,
):
    # emulate partition update in the background
    path: str = app.materialize(signal_view)[0]
    resource_access_spec = app._get_input_signal(signal_view).resource_access_spec

    if resource_access_spec.source == SignalSourceType.INTERNAL:
        _add_data_for_internal_signal(app.platform.storage, data, object_name, path, subfolder)

    elif resource_access_spec.source == SignalSourceType.S3:
        routing_table = app.platform.routing_table
        if isinstance(routing_table, AWSConstructMixin) and isinstance(resource_access_spec, S3SignalSourceAccessSpec):
            _add_data_for_s3_signal(
                routing_table.session,
                routing_table.region,
                data,
                object_name,
                path,
                resource_access_spec.bucket,
            )
        else:
            raise TypeError(
                f"resource access spec's source is {SignalSourceType.S3} but it is not backed" f"by an AWS-based routing table!"
            )

    else:
        raise NotImplementedError(f"Unable to emulate data for {resource_access_spec.source}")

    if emit_signal:
        app.process(signal_view)


def _add_data_for_s3_signal(
    session: Session,
    region: str,
    data: Union[str, bytes],
    object_name: str,
    path: str,
    bucket_name: str,
):
    s3 = session.resource("s3", region_name=region)
    if not bucket_exists(s3, bucket_name):
        # make emulation environment (moto, etc) happy
        bucket = create_bucket(s3, bucket_name, region)
    else:
        bucket = get_bucket(s3, bucket_name)
    prefix = path.replace(f"s3://{bucket_name}/", "")
    if isinstance(data, str):
        data = data.encode("utf-8")
    put_object(bucket, prefix + "/" + object_name, data)


def _add_data_for_internal_signal(
    storage: Storage,
    data: Union[str, bytes],
    object_name: str,
    path: str,
    subfolder: str,
):
    folder = path[path.find(InternalDatasetSignalSourceAccessSpec.FOLDER) :]
    if subfolder is not None:
        folder = folder + InternalDatasetSignalSourceAccessSpec.path_delimiter() + subfolder
    if isinstance(data, str):
        # Will be UTF-8 encoded by the storage implementation
        storage.save(data, [folder], object_name)
    else:
        storage.save_object(data, [folder], object_name)


def check_test_data(
    app: Application,
    signal_view: Union[FilteredView, MarshalerNode],
    object_name: str,
    subfolder: str = None,
) -> bool:
    path: str = app.materialize(signal_view)[0]
    resource_access_spec = app._get_input_signal(signal_view).resource_access_spec

    if resource_access_spec.source == SignalSourceType.INTERNAL:
        return _check_data_for_internal_signal(app.platform.storage, object_name, path, subfolder)
    elif resource_access_spec.source == SignalSourceType.S3:
        routing_table = app.platform.routing_table
        if isinstance(routing_table, AWSConstructMixin) and isinstance(resource_access_spec, S3SignalSourceAccessSpec):
            return _check_data_for_s3_signal(
                routing_table.session,
                routing_table.region,
                object_name,
                path,
                resource_access_spec.bucket,
            )
        else:
            raise TypeError(
                f"resource access spec's source is {SignalSourceType.S3} but it is not backed" f"by an AWS-based routing table!"
            )
    else:
        raise NotImplementedError(f"Unable to emulate data for {resource_access_spec.source}")


def _check_data_for_s3_signal(
    session: Session,
    region: str,
    object_name: str,
    path: str,
    bucket_name: str,
) -> bool:
    s3 = session.resource("s3", region_name=region)
    if not bucket_exists(s3, bucket_name):
        return False
    bucket = get_bucket(s3, bucket_name)
    prefix = path.replace(f"s3://{bucket_name}/", "")
    return object_exists(s3, bucket, prefix + "/" + object_name)


def _check_data_for_internal_signal(
    storage: Storage,
    object_name: str,
    path: str,
    subfolder: str,
):
    folder = path[path.find(InternalDatasetSignalSourceAccessSpec.FOLDER) :]
    if subfolder is not None:
        folder = folder + InternalDatasetSignalSourceAccessSpec.path_delimiter() + subfolder
    return storage.check_object([folder], object_name)
