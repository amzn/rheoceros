# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from intelliflow.api import InlinedCompute
from intelliflow.core.application.application import Application
from intelliflow.core.signal_processing.definitions.dimension_defs import Type as DimensionType


def inlined_compute_impl(*args, **kwargs):
    pass


def dim_link(rhs):
    return rhs


def join(app: Application):
    app.create_data(
        "test_node",
        compute_targets=[InlinedCompute(inlined_compute_impl)],
        output_dimension_spec={"dim": {type: DimensionType.STRING}},
        output_dim_links=[("dim", dim_link, "dummy")],
    )
