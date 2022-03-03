# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from intelliflow.api_ext import python_module


def execute_and_get_output(relative_path, locals, globals, output_df_name="output"):
    code = python_module(relative_path)
    exec(code, globals, locals)
    out = locals[output_df_name]
    return out
