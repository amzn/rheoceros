import ast
import json
import os
from enum import Enum
from typing import Any, ClassVar, Dict, Optional, Union

from intelliflow.core.platform.compute_targets.sim import CTI, SIM
from intelliflow.core.platform.compute_targets.slack import Slack
from intelliflow.core.signal_processing.definitions.metric_alarm_defs import CWAInternal

from ...core.deployment import IS_ON_AWS_LAMBDA, is_on_remote_dev_env
from ...core.platform.development import AWSConfiguration
from .regions import Regions


class FlowConfig:
    """Standardized and environment-aware way to manage application configuration for IntelliFlow workflows

    Supports App Config, in memory dictionary and environment variables with the following key lookup precedence:

    - in_memory_overwrites
    - then AppConfig
    - finally os.environment['INTELLIFLOW_APP_CONFIG_JSON']

    configuration stored in any of those mediums must be compatible with Amazon App Config format by nesting
    ```
      - namespace
         - key
            - <data: Any>
    ```

    being equivalent to the following in AppConfig:

    ```
    *.*.namespace.key = {
      <data>
    }
    ```

    Example 1 (using in memory to overwrite app name):

        pipeline_config = FlowConfig(app_config, in_memory_overwrites={
         "GeoNetworkConnectivityIntelliFlow": {
                "app": {
                   "app_name": "geo-net-beta",
                }
         }
        })

    Example 2 (use-case: define as AWS Lambda Env Variable using IF bootstrapper stack in CDK)

       os.environment[FlowConfig.INTELLIFLOW_APP_CONFIG_JSON] = json.dumps({
         "GeoNetworkConnectivityIntelliFlow": {
                "app": {
                   "app_name": "geo-net-beta",
                }
         }

       })

       pipeline_config_env_only = FlowConfig()
    """

    INTELLIFLOW_APP_CONFIG_JSON: ClassVar[str] = "INTELLIFLOW_APP_CONFIG_JSON"

    class _ConfigReader:

        def __init__(
            self, app_config: Optional["bender.config.ServiceConfig"] = None, in_memory_overwrites: Optional[Dict[str, Any]] = None
        ) -> None:
            self._app_config: "bender.config.ServiceConfig" = None
            self._in_memory_overwrites: Dict[str, Any] = None
            self._environment: Dict[str, Any] = dict()

            if app_config:
                import bender.config

                if isinstance(app_config, bender.config.ServiceConfig):
                    self._app_config = app_config
                else:
                    raise ValueError(f"App Config object type [{type(app_config)!r}] to FlowConfig is unsupported!")

            if in_memory_overwrites:
                if isinstance(in_memory_overwrites, dict):
                    self._in_memory_overwrites = dict(in_memory_overwrites)

            env_app_config_json_serialized: Optional[str] = os.environ.get(FlowConfig.INTELLIFLOW_APP_CONFIG_JSON, None)
            if env_app_config_json_serialized:
                if not isinstance(env_app_config_json_serialized, str):
                    raise ValueError(
                        f"{FlowConfig.INTELLIFLOW_APP_CONFIG_JSON!r} type [{type(env_app_config_json_serialized)}] is not supported!"
                    )
                self._environment = json.loads(env_app_config_json_serialized)

        def get(self, namespace, key, **options) -> Any:
            """
            Supports App Config style key lookup from App config (if defined) and/or in memory dictionary
            and/or environment variables with the following key lookup precedence:

            1- in_memory_overwrites
            2- then AppConfig
            3- finally os.environment['INTELLIFLOW_APP_CONFIG_JSON']
            """

            # 1- check in memory overwrites first
            if self._in_memory_overwrites and namespace in self._in_memory_overwrites and key in self._in_memory_overwrites[namespace]:
                return self._in_memory_overwrites[namespace][key]

            # 2- app config
            if self._app_config:
                import bender.config

                try:
                    return self._app_config.get(namespace, key, **options)
                except bender.config.MissingConfigurationValue:
                    pass

            # 3- environment
            if self._environment and namespace in self._environment and key in self._environment[namespace]:
                return self._environment[namespace][key]

            raise KeyError(f"Missing configuration for [namespace={namespace!r}, key={key!r}]!")

        def _check(self, namespace, key, **options) -> Optional[Any]:
            try:
                return self.get(namespace, key, **options)
            except KeyError:
                return None

    def __init__(
        self, app_config: Optional["bender.config.ServiceConfig"] = None, in_memory_overwrites: Optional[Dict[str, Any]] = None
    ) -> None:
        self._confi_reader = FlowConfig._ConfigReader(app_config, in_memory_overwrites)

    def get(self, namespace, key, **options) -> Any:
        return self._confi_reader.get(namespace, key, **options)

    def aws_conf_builder(self, turtle_role_arn: Optional[str] = None, account_id: Optional[str] = None) -> AWSConfiguration:
        conf_builder = AWSConfiguration.builder()
        if is_on_remote_dev_env() or IS_ON_AWS_LAMBDA:
            from intelliflow.core.platform.definitions.aws.common import CommonParams as AWSCommonParams

            conf_builder.with_param(AWSCommonParams.AUTO_UPDATE_DEV_ROLE, True)
        elif self.use_turtle:
            _turtle_role_arn = self.turtle_role_arn if not turtle_role_arn else turtle_role_arn
            conf_builder = conf_builder.with_turtle_config(role_arn=_turtle_role_arn, refresh_cycle_in_sec=60 * 5)
        else:
            _account_id = self.account_id if not account_id else account_id
            conf_builder = conf_builder.with_dev_role_credentials(_account_id)
        return conf_builder
