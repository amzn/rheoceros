# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from intelliflow.core.platform.constructs import (
    ConstructEncryption,
    ConstructPersistenceSecurityDef,
    ConstructSecurityConf,
    EncryptionKeyAllocationLevel,
    Storage,
)
from intelliflow.mixins.aws.integ_test import AWSIntegTestMixin


class TestAWSApplicationSecurity(AWSIntegTestMixin):
    def setup(self):
        super().setup("IntelliFlow")

    def test_application_security_standalone(self):

        from test.intelliflow.core.application.test_aws_application_execution_control import TestAWSApplicationExecutionControl

        app = super()._create_test_application("child-app", reset_if_exists=True)
        app = TestAWSApplicationExecutionControl._create_test_application(self, app)
        app.set_security_conf(
            Storage,
            ConstructSecurityConf(
                persisting=ConstructPersistenceSecurityDef(
                    ConstructEncryption(
                        EncryptionKeyAllocationLevel.HIGH,
                        key_rotation_cycle_in_days=365,
                        is_hard_rotation=False,
                        reencrypt_old_data_during_hard_rotation=False,
                        trust_access_from_same_root=True,
                    )
                ),
                passing=None,
                processing=None,
            ),
        )
        app.activate()

        # activation is successful with a security conf
        # now let's see if we can re-load
        app = super()._create_test_application("child-app")
        # this will reset the security conf
        app.activate()

        app.terminate()
        app.delete()

    def test_application_security_in_collaboration(self):
        from test.intelliflow.core.application.test_aws_application_create_and_query import TestAWSApplicationBuild
        from test.intelliflow.core.application.test_aws_application_execution_control import TestAWSApplicationExecutionControl

        app = super()._create_test_application("child-app", from_other_account=True, reset_if_exists=True)
        app = TestAWSApplicationExecutionControl._create_test_application(self, app)
        app.activate()

        upstream_app = super()._create_test_application("parent-app", reset_if_exists=True)
        upstream_app = TestAWSApplicationBuild._create_test_application(self, upstream_app)
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
                        trust_access_from_same_root=True,
                    )
                ),
                passing=None,
                processing=None,
            ),
        )
        # connect them
        upstream_app.authorize_downstream(app.id, self.cross_account_id, self.region)
        upstream_app.activate()

        app.import_upstream(upstream_app.id, self.account_id, self.region)
        app.activate()

        # fetch from app (parent's data is visible within child's scope)
        assert app["eureka_default_selection_data_over_two_days"]

        # Reload and Undo
        # this will actually re-load the app (since it is already active)
        upstream_app = super()._create_test_application("parent-app")
        upstream_app = TestAWSApplicationBuild._create_test_application(self, upstream_app)
        # note that there is no security conf anymore
        upstream_app.authorize_downstream(app.id, self.cross_account_id, self.region)
        upstream_app.activate()

        # refresh
        app = super()._create_test_application("child-app", from_other_account=True)
        app.attach()
        app.activate()

        # this will prove that upstream platform was implicitly loaded with no security
        assert app["eureka_default_selection_data_over_two_days"]

        app.terminate()
        app.delete()
        upstream_app.terminate()
        upstream_app.delete()
