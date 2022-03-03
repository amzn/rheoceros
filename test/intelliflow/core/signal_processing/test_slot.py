# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest

from intelliflow.core.serialization import dumps, loads
from intelliflow.core.signal_processing.definitions.compute_defs import ABI, Lang
from intelliflow.core.signal_processing.slot import *


class TestSlot:
    # using the following class level declarations instead of fixtures to be able to use them from other modules.
    # hacks using 'conftest.py' to make fixtures available to other modules is just non-sense.
    slot_batch_compute_basic = Slot(
        SlotType.ASYNC_BATCH_COMPUTE, "output = input1", Lang.PYTHON, ABI.GLUE_EMBEDDED, {"NumberOfWorkers": 10, "WorkerType": "G.1X"}, None
    )
    slot_batch_compute_basic_clone = Slot(
        SlotType.ASYNC_BATCH_COMPUTE, "output = input1", Lang.PYTHON, ABI.GLUE_EMBEDDED, {"NumberOfWorkers": 10, "WorkerType": "G.1X"}, None
    )
    slot_inlined_compute_basic = Slot(SlotType.SYNC_INLINED, "print('hi dad!')", None, None, None, None)
    slot_inlined_compute_basic_with_permissions = Slot(
        SlotType.SYNC_INLINED, "print('hi dad!')", None, None, None, None, [Permission(["resource"], ["action"])], 4
    )

    def test_slot_equality(self):
        assert self.slot_batch_compute_basic == self.slot_batch_compute_basic_clone
        assert self.slot_batch_compute_basic != self.slot_inlined_compute_basic
        assert self.slot_inlined_compute_basic != self.slot_inlined_compute_basic_with_permissions

    def test_slot_check_integrity(self):
        assert self.slot_batch_compute_basic.check_integrity(self.slot_batch_compute_basic_clone)
        assert not self.slot_batch_compute_basic.check_integrity(self.slot_inlined_compute_basic)
        assert not self.slot_inlined_compute_basic.check_integrity(self.slot_inlined_compute_basic_with_permissions)
        other = self.slot_batch_compute_basic
        assert not self.slot_batch_compute_basic.check_integrity(
            Slot(other.type, "different code", other.code_lang, other.code_abi, other.extra_params, None)
        )
        assert not self.slot_batch_compute_basic.check_integrity(
            Slot(other.type, other.code, Lang.SCALA, other.code_abi, other.extra_params, None)
        )
        assert not self.slot_batch_compute_basic.check_integrity(
            Slot(other.type, other.code, other.code_lang, ABI.PARAMETRIZED_QUERY, other.extra_params, None)
        )
        assert not self.slot_batch_compute_basic.check_integrity(Slot(other.type, other.code, other.code_lang, other.code_abi, {}, None))

    def test_slot_api(self):
        assert (
            self.slot_batch_compute_basic.type == self.slot_batch_compute_basic_clone.type
            and self.slot_batch_compute_basic.code == self.slot_batch_compute_basic_clone.code
            and self.slot_batch_compute_basic.code_lang == self.slot_batch_compute_basic_clone.code_lang
            and self.slot_batch_compute_basic.code_abi == self.slot_batch_compute_basic_clone.code_abi
            and self.slot_batch_compute_basic.extra_params == self.slot_batch_compute_basic_clone.extra_params
            and self.slot_batch_compute_basic.output_signal_desc == self.slot_batch_compute_basic_clone.output_signal_desc
            and self.slot_batch_compute_basic.permissions == self.slot_batch_compute_basic_clone.permissions
            and self.slot_batch_compute_basic.max_retry_count == self.slot_batch_compute_basic_clone.max_retry_count
        )

    def test_slot_serialization(self):
        assert self.slot_batch_compute_basic == loads(dumps(self.slot_batch_compute_basic))
        assert self.slot_inlined_compute_basic_with_permissions == loads(dumps(self.slot_inlined_compute_basic_with_permissions))

    # TODO
    # def test_slot_immutability(self):
    #    with pytest.raises(Exception) as error:
    #        self.slot_batch_compute_basic.type = SlotType.SYNC_INLINED

    #    assert error.typename == 'AttributeError'
