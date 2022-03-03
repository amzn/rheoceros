# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from intelliflow.api_ext import *
from intelliflow.core.platform.constructs import (
    ConstructEncryption,
    ConstructPersistenceSecurityDef,
    ConstructSecurityConf,
    EncryptionKeyAllocationLevel,
    Storage,
)
from intelliflow.mixins.aws.test import AWSTestBase


class TestAWSApplicationSecurity(AWSTestBase):
    def test_application_security_standalone(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        from test.intelliflow.core.application.test_aws_application_execution_control import TestAWSApplicationExecutionControl

        app = TestAWSApplicationExecutionControl._create_test_application(self, "child_app")
        app.set_security_conf(
            Storage,
            ConstructSecurityConf(
                persisting=ConstructPersistenceSecurityDef(
                    ConstructEncryption(
                        EncryptionKeyAllocationLevel.HIGH,
                        key_rotation_cycle_in_days=365,
                        is_hard_rotation=False,
                        reencrypt_old_data_during_hard_rotation=False,
                        trust_access_from_same_root=False,
                    )
                ),
                passing=None,
                processing=None,
            ),
        )
        app.activate()

        # activation is successful with a security conf
        # now let's see if we can re-load
        app = AWSApplication("child_app", self.region)
        # this will reset the security conf
        app.activate()

        self.patch_aws_stop()

    def test_application_security_in_collaboration(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        from test.intelliflow.core.application.test_aws_application_create_and_query import TestAWSApplicationBuild
        from test.intelliflow.core.application.test_aws_application_execution_control import TestAWSApplicationExecutionControl

        app = TestAWSApplicationExecutionControl._create_test_application(self, "child_app")
        app.activate()

        upstream_app = TestAWSApplicationBuild._create_test_application(self, "parent_app")
        # set upstream security conf
        upstream_app.set_security_conf(
            Storage,
            ConstructSecurityConf(
                persisting=ConstructPersistenceSecurityDef(
                    ConstructEncryption(
                        EncryptionKeyAllocationLevel.HIGH,
                        key_rotation_cycle_in_days=365,
                        is_hard_rotation=False,
                        reencrypt_old_data_during_hard_rotation=False,
                        trust_access_from_same_root=False,
                    )
                ),
                passing=None,
                processing=None,
            ),
        )
        # connect them
        upstream_app.authorize_downstream("child_app", self.account_id, self.region)
        upstream_app.activate()

        app.import_upstream("parent_app", self.account_id, self.region)
        app.activate()

        # fetch from app (parent's data is visible within child's scope)
        assert app["eureka_default_selection_data_over_two_days"]

        # Reload and Undo
        # this will actually re-load the app (since it is already active)
        upstream_app = TestAWSApplicationBuild._create_test_application(self, "parent_app")
        # note that there is no security conf anymore
        upstream_app.authorize_downstream("child_app", self.account_id, self.region)
        upstream_app.activate()

        # refresh
        app = AWSApplication("child_app", self.region)
        app.attach()
        app.activate()

        # this will prove that upstream platform was implicitly loaded with no security
        assert app["eureka_default_selection_data_over_two_days"]

        self.patch_aws_stop()
