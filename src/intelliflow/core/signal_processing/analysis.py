# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from abc import ABC, abstractmethod
from typing import Dict

logger = logging.getLogger(__name__)


class SignalIntegrityChecker(ABC):
    @classmethod
    @abstractmethod
    def check(cls, signal_source: "SignalSource", protocol: "SignalIntegrityProtocol") -> bool:
        ...

    @classmethod
    @abstractmethod
    def get_required_resource_name(cls, materialized_access_spec: "SignalAccessSpec", protocol: "SignalIntegrityProtocol") -> str:
        ...


class StatelessResourceChecker(SignalIntegrityChecker):
    """A basic stateless checker to see a resource is ready"""

    # overrides
    @classmethod
    def check(cls, signal_source: "SignalSource", protocol: "SignalIntegrityProtocol") -> bool:
        if "file" in protocol.args:
            if isinstance(protocol.args["file"], str):
                return signal_source.name == protocol.args["file"]
            elif isinstance(protocol.args["file"], list):
                return signal_source.name in protocol.args["file"]
        elif "object" in protocol.args:
            if isinstance(protocol.args["object"], str):
                return signal_source.name == protocol.args["object"]
            elif isinstance(protocol.args["object"], list):
                return signal_source.name in protocol.args["object"]

        return False

    @classmethod
    def get_required_resource_name(cls, materialized_access_spec: "SignalAccessSpec", protocol: "SignalIntegrityProtocol") -> str:

        if "file" in protocol.args:
            if isinstance(protocol.args["file"], str):
                return protocol.args["file"]
            elif isinstance(protocol.args["file"], list):
                if len(protocol.args["file"]) >= 1:
                    logger.info("file Signal Integrity Protocol has a list of values. " "Returning first value for required resource name")
                    return protocol.args["file"][0]
        elif "object" in protocol.args:
            if isinstance(protocol.args["object"], str):
                return protocol.args["object"]
            elif isinstance(protocol.args["object"], list):
                if len(protocol.args["object"]) >= 1:
                    logger.info(
                        "object Signal Integrity Protocol has a list of values. " "Returning first value for required resource name"
                    )
                    return protocol.args["object"][0]


class SpectralAnalyzer(SignalIntegrityChecker, ABC):
    """Stateful checker that would need to read a range of resources or other resources to decide on Signal state,
    depending on signal_source type (technology) and again the protocol.

    We hope that we won't need this against the majority of SignalSource's and their use-cases by RheocerOS.
    """

    pass


# TODO create a factory for this if the business logic in this module goes beyond
# our expectations. We are expecting that 'StatelessResourceChecker' would suffice in
# majority of use-cases against different resources / types (not only datasets).
INTEGRITY_CHECKER_MAP = {
    "MANIFEST": StatelessResourceChecker,
    "MANIFEST_FILE": StatelessResourceChecker,
    "MANIFEST_CHECK": StatelessResourceChecker,
    "FILE_CHECK": StatelessResourceChecker,
}
