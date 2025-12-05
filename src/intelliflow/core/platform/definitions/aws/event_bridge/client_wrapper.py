import logging
from enum import Enum, unique

logger = logging.getLogger(__name__)


EVENT_BRIDGE_RULE_STATE_KEY = "State"


@unique
class StateType(str, Enum):
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"
