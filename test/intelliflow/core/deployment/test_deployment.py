# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import importlib
import io
import os
import zipfile
from pathlib import Path

import pytest

from intelliflow.core import deployment
from intelliflow.core.deployment import DeploymentConfiguration


class TestDeployment:
    @classmethod
    def _get_module_folder_path(cls, module: str) -> str:
        m_spec = importlib.util.find_spec(module)
        return str(Path(m_spec.loader.path).parent)

    def test_bundle_creation_cache(self):
        """with the same parameters, deployment module returns the same working set instance"""
        working_set = deployment.get_working_set_as_zip_stream()
        assert working_set is deployment.get_working_set_as_zip_stream()

        working_set2 = deployment.get_working_set_as_zip_stream(include_resources=True)
        assert working_set2 is not working_set

        working_set3 = deployment.get_working_set_as_zip_stream(include_resources=True)
        assert working_set3 is working_set2

        # no need to check the effect of modified DeploymentConfiguration at runtime since other tests below

    def test_bundle_creation_with_path_overwrites(self):
        """test the effect of parameters into deployment::get_working_set_as_zip_stream, which are used for
        finer granular and programmatic bundling with an even higher priority over DeploymentConfiguration."""
        # based on this test module setup, folder structure IF normally should have no idea about our intent for the
        # inclusion of 'test.intelliflow.core.deployment.
        working_set = deployment.get_working_set_as_zip_stream()
        with zipfile.ZipFile(io.BytesIO(working_set), "r") as bundle_zip:
            archieve_index = set(bundle_zip.namelist())
            assert "dummy_package_root/__init__.py" not in archieve_index
            # dependencies
            assert "dummy_dependency1/__init__.py" not in archieve_index
            assert "dummy_dependency2/__init__.py" not in archieve_index
            assert "dummy_package_to_avoid/__init__.py" not in archieve_index

        working_set = deployment.get_working_set_as_zip_stream(
            source_path=self._get_module_folder_path("test.intelliflow.core.deployment.dummy_app_root.src")
        )
        with zipfile.ZipFile(io.BytesIO(working_set), "r") as bundle_zip:
            archieve_index = set(bundle_zip.namelist())
            assert "dummy_package_root/__init__.py" in archieve_index
            # dependencies
            assert "dummy_dependency1/__init__.py" not in archieve_index
            assert "dummy_dependency2/__init__.py" not in archieve_index
            assert "dummy_package_to_avoid/__init__.py" not in archieve_index

        working_set = deployment.get_working_set_as_zip_stream(
            dependencies_path=self._get_module_folder_path("test.intelliflow.core.deployment.dummy_app_root.dependencies"),
            source_path=self._get_module_folder_path("test.intelliflow.core.deployment.dummy_app_root.src"),
        )
        with zipfile.ZipFile(io.BytesIO(working_set), "r") as bundle_zip:
            archieve_index = set(bundle_zip.namelist())
            assert "dummy_package_root/__init__.py" in archieve_index
            # dependencies do exist now
            assert "dummy_dependency1/__init__.py" in archieve_index
            assert "dummy_dependency2/__init__.py" in archieve_index
            assert "dummy_package_to_avoid/__init__.py" in archieve_index

        working_set = deployment.get_working_set_as_zip_stream(
            dependencies_path=self._get_module_folder_path("test.intelliflow.core.deployment.dummy_app_root.dependencies"),
            source_path=self._get_module_folder_path("test.intelliflow.core.deployment.dummy_app_root.src"),
            extra_folders_to_avoid={"dummy_package_to_avoid"},
        )
        with zipfile.ZipFile(io.BytesIO(working_set), "r") as bundle_zip:
            archieve_index = set(bundle_zip.namelist())
            assert "dummy_package_root/__init__.py" in archieve_index
            # dependencies do exist now
            assert "dummy_dependency1/__init__.py" in archieve_index
            assert "dummy_dependency2/__init__.py" in archieve_index
            assert "dummy_package_to_avoid/__init__.py" not in archieve_index

    def test_bundle_creation_with_deployment_configuration(self):
        """test the effect of parameters from DeploymentConfiguration which is an abstraction around both env
        parameters and their dynamic overwrites set by the user indirectly or directly via the same module."""
        # first make sure that we have a brand-new start (no caching interference, or test tooling messing up)
        working_set = deployment.get_working_set_as_zip_stream()
        with zipfile.ZipFile(io.BytesIO(working_set), "r") as bundle_zip:
            archieve_index = set(bundle_zip.namelist())
            # app src
            assert "dummy_package_root/__init__.py" not in archieve_index
            # dependencies
            assert "dummy_dependency1/__init__.py" not in archieve_index
            assert "dummy_dependency2/__init__.py" not in archieve_index

        DeploymentConfiguration.app_root = "gibberish"
        with pytest.raises(ValueError):
            deployment.get_working_set_as_zip_stream()

        DeploymentConfiguration.app_root = self._get_module_folder_path("test.intelliflow.core.deployment.dummy_app_root")
        working_set = deployment.get_working_set_as_zip_stream()
        with zipfile.ZipFile(io.BytesIO(working_set), "r") as bundle_zip:
            archieve_index = set(bundle_zip.namelist())
            # app src auto inferred from 'app root'
            assert "dummy_package_root/__init__.py" in archieve_index
            # dependencies (cannot be found) as this folder setup is not supported
            assert "dummy_dependency1/__init__.py" not in archieve_index
            assert "dummy_dependency2/__init__.py" not in archieve_index
            # extra modules
            assert "test/intelliflow/core/deployment/dummy_extra_module/__init__.py" not in archieve_index

        with pytest.raises(ValueError):
            DeploymentConfiguration.app_extra_modules = "wrong type"

        DeploymentConfiguration.app_extra_modules = {"test.intelliflow.core.deployment.dummy_extra_module"}
        working_set = deployment.get_working_set_as_zip_stream()
        with zipfile.ZipFile(io.BytesIO(working_set), "r") as bundle_zip:
            archieve_index = set(bundle_zip.namelist())
            # app src
            assert "dummy_package_root/__init__.py" in archieve_index
            # dependencies
            assert "dummy_dependency1/__init__.py" not in archieve_index
            assert "dummy_dependency2/__init__.py" not in archieve_index
            # extra modules
            # Please note that imported obeying the entire module hierarchy (major diff between extra package and module declarations)
            assert "test/intelliflow/core/deployment/dummy_extra_module/__init__.py" in archieve_index

        with pytest.raises(ValueError):
            DeploymentConfiguration.app_extra_package_paths = "bad type"

        DeploymentConfiguration.app_extra_package_paths = {
            "test.intelliflow.core.deployment.dummy_app_root.dependencies.dummy_dependency1",
            "test.intelliflow.core.deployment.dummy_app_root.dependencies.dummy_dependency2",
        }
        with pytest.raises(ValueError):  # will fail because folder-paths should be used not full module name
            deployment.get_working_set_as_zip_stream()
        DeploymentConfiguration.app_extra_package_paths = {
            self._get_module_folder_path("test.intelliflow.core.deployment.dummy_app_root.dependencies.dummy_dependency1"),
            self._get_module_folder_path("test.intelliflow.core.deployment.dummy_app_root.dependencies.dummy_dependency2"),
        }
        working_set = deployment.get_working_set_as_zip_stream()
        with zipfile.ZipFile(io.BytesIO(working_set), "r") as bundle_zip:
            archieve_index = set(bundle_zip.namelist())
            # app src
            assert "dummy_package_root/__init__.py" in archieve_index
            # dependencies are explicitly injected now!
            # Please note that they are added as 'root' level modules to the bundle (major diff between extra package and module declarations)
            assert "dummy_dependency1/__init__.py" in archieve_index
            assert "dummy_dependency2/__init__.py" in archieve_index
            # extra modules
            assert "test/intelliflow/core/deployment/dummy_extra_module/__init__.py" in archieve_index

    def test_bundle_creation_with_env_variables(self):
        """test the effect of parameters from DeploymentConfiguration which is an abstraction around both env
        parameters and their dynamic overwrites set by the user indirectly or directly via the same module."""
        # first make sure that we have a brand-new start
        DeploymentConfiguration._conf.clear()

        working_set = deployment.get_working_set_as_zip_stream()
        with zipfile.ZipFile(io.BytesIO(working_set), "r") as bundle_zip:
            archieve_index = set(bundle_zip.namelist())
            # app src
            assert "dummy_package_root/__init__.py" not in archieve_index
            # dependencies
            assert "dummy_dependency1/__init__.py" not in archieve_index
            assert "dummy_dependency2/__init__.py" not in archieve_index

        os.environ[DeploymentConfiguration.APP_ROOT] = "gibberish"
        with pytest.raises(ValueError):
            deployment.get_working_set_as_zip_stream()

        # prove the precedence of configuration value set directly by showing that deployment error will be gone.
        DeploymentConfiguration.app_root = None
        deployment.get_working_set_as_zip_stream()

        # reset again (otherwise app_root will remain None even if os.environ has it to honor user's explicit setting)
        DeploymentConfiguration._conf.clear()

        os.environ[DeploymentConfiguration.APP_ROOT] = self._get_module_folder_path("test.intelliflow.core.deployment.dummy_app_root")
        working_set = deployment.get_working_set_as_zip_stream()
        with zipfile.ZipFile(io.BytesIO(working_set), "r") as bundle_zip:
            archieve_index = set(bundle_zip.namelist())
            # app src auto inferred from 'app root'
            assert "dummy_package_root/__init__.py" in archieve_index
            # dependencies (cannot be found) as this folder setup is not supported
            assert "dummy_dependency1/__init__.py" not in archieve_index
            assert "dummy_dependency2/__init__.py" not in archieve_index
            # extra modules
            assert "test/intelliflow/core/deployment/dummy_extra_module/__init__.py" not in archieve_index

        os.environ[DeploymentConfiguration.APP_EXTRA_MODULES] = "wrong module"
        with pytest.raises(ValueError):
            deployment.get_working_set_as_zip_stream()
        os.environ[DeploymentConfiguration.APP_EXTRA_MODULES] = "test.intelliflow.core.deployment.dummy_extra_module"
        working_set = deployment.get_working_set_as_zip_stream()
        with zipfile.ZipFile(io.BytesIO(working_set), "r") as bundle_zip:
            archieve_index = set(bundle_zip.namelist())
            # app src
            assert "dummy_package_root/__init__.py" in archieve_index
            # dependencies
            assert "dummy_dependency1/__init__.py" not in archieve_index
            assert "dummy_dependency2/__init__.py" not in archieve_index
            # extra modules
            # Please note that imported obeying the entire module hierarchy (major diff between extra package and module declarations)
            assert "test/intelliflow/core/deployment/dummy_extra_module/__init__.py" in archieve_index

        os.environ[DeploymentConfiguration.APP_EXTRA_PACKAGE_PATHS] = "bad path"
        with pytest.raises(ValueError):
            deployment.get_working_set_as_zip_stream()

        os.environ[DeploymentConfiguration.APP_EXTRA_PACKAGE_PATHS] = (
            f"test.intelliflow.core.deployment.dummy_app_root.dependencies.dummy_dependency1"
            f"{DeploymentConfiguration.SEPARATOR}"
            f"test.intelliflow.core.deployment.dummy_app_root.dependencies.dummy_dependency2"
        )
        with pytest.raises(ValueError):  # will fail because folder-paths should be used not full module name
            deployment.get_working_set_as_zip_stream()
        os.environ[DeploymentConfiguration.APP_EXTRA_PACKAGE_PATHS] = (
            f"{self._get_module_folder_path('test.intelliflow.core.deployment.dummy_app_root.dependencies.dummy_dependency1')}"
            f"{DeploymentConfiguration.SEPARATOR}"
            f"{self._get_module_folder_path('test.intelliflow.core.deployment.dummy_app_root.dependencies.dummy_dependency2')}"
        )
        working_set = deployment.get_working_set_as_zip_stream()
        with zipfile.ZipFile(io.BytesIO(working_set), "r") as bundle_zip:
            archieve_index = set(bundle_zip.namelist())
            # app src
            assert "dummy_package_root/__init__.py" in archieve_index
            # dependencies are explicitly injected now!
            # Please note that they are added as 'root' level modules to the bundle (major diff between extra package and module declarations)
            assert "dummy_dependency1/__init__.py" in archieve_index
            assert "dummy_dependency2/__init__.py" in archieve_index
            # extra modules
            assert "test/intelliflow/core/deployment/dummy_extra_module/__init__.py" in archieve_index

        # now finally show that explicit reset on the parameters of the configuration completely nullifies them
        # for the rest of the session (even if they are defined in the env)
        DeploymentConfiguration.app_root = None
        DeploymentConfiguration.app_source_root = None
        DeploymentConfiguration.app_extra_package_paths = None
        DeploymentConfiguration.app_extra_modules = None
        working_set = deployment.get_working_set_as_zip_stream()
        with zipfile.ZipFile(io.BytesIO(working_set), "r") as bundle_zip:
            archieve_index = set(bundle_zip.namelist())
            # nothing can be found
            assert "dummy_package_root/__init__.py" not in archieve_index
            assert "dummy_dependency1/__init__.py" not in archieve_index
            assert "dummy_dependency2/__init__.py" not in archieve_index
            assert "test/intelliflow/core/deployment/dummy_extra_module/__init__.py" not in archieve_index
