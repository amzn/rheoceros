# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import time

import intelliflow.api_ext as flow
from intelliflow.api_ext import *


logger = flow.init_basic_logging(root_level=logging.CRITICAL)

dev_account = "427809481713"

# assume that default credentials on the machine belongs to account "427809481713"
# so the following app will be provisioned in that account
app = AWSApplication("andes-ant-appp", "us-east-1")
# This examples does not assume that the child application exists already.
# That is the reason for 'seemingly' redundant activation. It is required to establish connections between apps.
# A downstream app must have been activated (in existence) before authorizing it by an upstream app.
#app.activate()

# here we assume that this parent exists already (see 'examples/eureka_pretaining_etl.py')
# again in the same account
# we have to use the same account here because in this test we are relying on default credentials in the same process
parent_app = AWSApplication("eureka-dev-yunusko", "us-east-1")
parent_app.attach()
parent_app.authorize_downstream("andes-ant-appp", dev_account, 'us-east-1')
parent_app.set_security_conf(Storage, ConstructSecurityConf(
                                            persisting=ConstructPersistenceSecurityDef(
                                                ConstructEncryption(EncryptionKeyAllocationLevel.HIGH,
                                                                    key_rotation_cycle_in_days=365,
                                                                    is_hard_rotation=False,
                                                                    reencrypt_old_data_during_hard_rotation=False,
                                                                    trust_access_from_same_root=False)),
                                            passing=None,
                                            processing=None))
parent_app.activate()

# import secure eureka app
app.import_upstream(parent_app.id, dev_account, 'us-east-1')

processed_data = app.create_data(id="PROCESSED_EUREKA_DATA",
                                 inputs=[
                                     parent_app["eureka_default_selection_data_over_two_days"]["*"]["*"]
                                 ],
                                 compute_targets="output = eureka_default_selection_data_over_two_days.limit(100)"
                                 )
app.activate()


# now manually trigger upstream data and watch:
#   - signal propagation from upstream app into downstream upon the completion of this re-run
#   - and implicit secure access into upstream storage while 'PROCESSED_EUREKA_DATA' is being executed

# Note: First execute call (convenient one) cannot be used since reverse lookup from materialized output to material inputs
# cannot be done due to a non-trivial link from 'offline_data' input's 'day' dimension to output's 'day'.
# see 'examples/eureka_pretraining_etl.py).
#   ValueError: Cannot do reverse lookup for input: 'offline_data' from output: eureka_default_selection_data_over_two_days!
#   Input dimensions {'day'} cannot be mapped from output dimensions: odict_keys(['reg', 'day'])
#parent_app.execute(parent_app["eureka_default_selection_data_over_two_days"]['NA']['2020-03-18'])
parent_app.execute(parent_app["eureka_default_selection_data_over_two_days"],
                   [parent_app["eureka_training_all_data"]['2020-03-18'],
                    parent_app["eureka_training_data"]["NA"]['2020-03-18']])

while app.poll(processed_data['NA']['2020-03-18']) == (None, None):
    logger.critical("PROCESSED_EUREKA_DATA['NA']['2020-03-18'] is not ready yet.")
    logger.critical("If this is not your first time seeing this, then propagation from upstream "
                    "'eureka_default_selection_data_over_two_days' into this app might have failed.")
    logger.critical("Will try in 5 seconds...")
    time.sleep(5)

logger.critical("Cross-account secure signalling/collaboration flow has been completed successfully! Exiting...")
