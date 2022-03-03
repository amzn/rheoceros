# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import imp
import importlib
import io
import logging
import os
import subprocess as sp
import sys
import zipfile
from pathlib import Path
from typing import Any, ClassVar, Dict, Optional, Set, Tuple, Union

from intelliflow.core.platform.definitions.aws.sagemaker.common import get_sagemaker_notebook_instance_pkg_root
from intelliflow.core.runtime import PYTHON_VERSION_MAJOR, PYTHON_VERSION_MINOR

logger = logging.getLogger(__name__)


class _DeploymentConfiguration(type):
    def __init__(cls, *args, **kwargs):
        cls._conf = dict()

    @property
    def conf(cls) -> Dict[str, Union[str, Set[str]]]:
        return cls._conf

    @property
    def app_root(cls) -> Optional[str]:
        """First checks whether the user has set the value programmatically, then falls back to environment variable"""
        return cls._conf.get(cls.APP_ROOT, os.getenv(cls.APP_ROOT))

    @app_root.setter
    def app_root(cls, app_root: str) -> None:
        cls._conf[cls.APP_ROOT] = app_root

    @property
    def app_source_root(cls) -> Optional[str]:
        """First checks whether the user has set the value programmatically, then falls back to environment variable"""
        return cls._conf.get(cls.APP_SOURCE_ROOT, os.getenv(cls.APP_SOURCE_ROOT))

    @app_source_root.setter
    def app_source_root(cls, app_src_root: str) -> None:
        cls._conf[cls.APP_SOURCE_ROOT] = app_src_root

    @property
    def app_extra_package_paths(cls) -> Optional[Set[str]]:
        """First checks whether the user has set the value programmatically, then falls back to environment variable"""
        return cls._conf.get(
            cls.APP_EXTRA_PACKAGE_PATHS, {p for p in str(os.getenv(cls.APP_EXTRA_PACKAGE_PATHS)).split(cls.SEPARATOR) if p != str(None)}
        )

    @app_extra_package_paths.setter
    def app_extra_package_paths(cls, app_extra_package_paths: Set[str]) -> None:
        if app_extra_package_paths is not None:
            if not isinstance(app_extra_package_paths, set) or any(not isinstance(p, str) for p in app_extra_package_paths):
                raise ValueError(
                    f"Invalid parameter for {cls.APP_EXTRA_PACKAGE_PATHS!r}! The following parameter should"
                    f" be a set of path strings only (each pointing to a relative or absolute path on the"
                    f" file system): {app_extra_package_paths!r}."
                )
        cls._conf[cls.APP_EXTRA_PACKAGE_PATHS] = app_extra_package_paths

    @property
    def app_extra_modules(cls) -> Optional[Set[str]]:
        """First checks whether the user has set the value programmatically, then falls back to environment variable"""
        return cls._conf.get(
            cls.APP_EXTRA_MODULES, {p for p in str(os.getenv(cls.APP_EXTRA_MODULES)).split(cls.SEPARATOR) if p != str(None)}
        )

    @app_extra_modules.setter
    def app_extra_modules(cls, app_extra_modules: Set[str]) -> None:
        if app_extra_modules is not None:
            if not isinstance(app_extra_modules, set) or any(not isinstance(p, str) for p in app_extra_modules):
                raise ValueError(
                    f"Invalid parameter for {cls.APP_EXTRA_MODULES!r}! The following parameter should"
                    f" be a set of Python modules only (e.g set(['sys', 'my_pkg_root.foo']) ):"
                    f" {app_extra_modules!r}."
                )
        cls._conf[cls.APP_EXTRA_MODULES] = app_extra_modules


class DeploymentConfiguration(metaclass=_DeploymentConfiguration):
    """Environment parameters and their programmatically set versions can be used to control the way the dependency
    graph is generated for the Python process hosting an IF app.

    Parameters exposed as class attributes here can also be set and modified as environment variables.
    """

    APP_ROOT: ClassVar[str] = "INTELLIFLOW_APP_ROOT"
    # path in which IF will search for 'pysrc' and 'src' folders; modules from those folders will be included in bundles (as root level modules)
    APP_SOURCE_ROOT: ClassVar[str] = "INTELLIFLOW_APP_SOURCE_ROOT"
    # relative or full path for Python packages that will be read as is and included in bundles (as root level modules)
    APP_EXTRA_PACKAGE_PATHS: ClassVar[str] = "INTELLIFLOW_APP_EXTRA_PACKAGE_PATHS"
    # unique Python modules from the sys path for which path extraction will be done by IF automatically and included in bundles
    APP_EXTRA_MODULES: ClassVar[str] = "INTELLIFLOW_APP_EXTRA_MODULES"

    SEPARATOR: ClassVar[str] = ";"

    _conf: ClassVar[Dict[str, Union[str, Set]]]


def set_deployment_conf(
    app_root: Optional[str] = None,
    app_source_root: Optional[str] = None,
    app_extra_package_paths: Optional[Set[str]] = None,
    app_extra_modules: Optional[Set[str]] = None,
) -> None:
    """Convenience method to offer a one-stop-shop for everything related to deployment as part of high-level user API

    :param app_root: overwrite the app_root that will otherwise be resolved from the env variable or automatically by
    the framework if both not set. This path is then used to infer the application logic 'src' root (if
    'app_source_root' is not set) and more importantly to find the entire dependency chain (aka workingset such as a
    virtualenv). Should be set in environments where frameworks auto-detection fails and the app development should be
    unblocked till the problematic environment in question will be supported by the framework.

    :param app_source_root: overwrite the app_source_root that will otherwise be resolved from the env variable or
    automatically by the framework if both not set. This path indicates where the project (application specific packages
    which are under development) resides in so that the framework can sweep all of its content (root level Python
    packages) for the deployment bundle. If not set or cannot be found via 'os.env', then the framework uses
    'app_root/pysrc' and finally 'app_root/src' to search for app specific packages. So this parameter should only be
    set in cases where this automatic source inclusion is to be avoided for some reason. In those scenarios, a power
    user might prefer controlling the finer-granular app-specific package bundling via 'app_extra_package_paths' and/or
    'app_extra_modules' parameters.

    :param app_extra_package_paths: overwrite the app_extra_package_paths that will otherwise be resolved from the env
    variable. There is no default behaviour from the framework if the env variable is not set either. That is why this
    parameter is an extra. However, in environments where app_root and app_source_set setting / auto-detection is
    either complicated or unwanted, this parameter can be used to control the final contents of the bundle. If the paths
    provided are expected to be merged with whatever paths already exist in
        `os.getenv(DeploymentConfiguration.APP_EXTRA_PACKAGE_PATHS)`
    then it is up to the user to do that set merge operation before calling this API, otherwise it is an overwrite.
    One important case where these paths would be defined is when a set of Python packages are not in sys path but are
    intended to be pushed to the runtime (target) system (e.g AWS Lambda, Glue) based on application design. Package
    folders are included as root level packages in the bundle so sub-modules are not advised to be included using this
    parameter. For sub-modules (that are to declared with full module name such as 'root_module.foo.bar') should be
    included via 'app_extra_modules'.

    :param app_extra_modules: almost equivalent to app_extra_package_paths with the subtle difference of accepting a set
    of Python modules or sub-modules (e.g "python_module" or "python_module.foo.bar") rather than relative/full folder
    paths for a better abstraction / convenience of high-level user code. When sub-modules such as 'root.foo.bar' are
    declared, then their entire folder branch such as 'root/foo/bar' are included in the final bundle so that there will
    not be any need for aligning imports at runtime( do 'import root.foo.bar' both at application development and
    runtime. These modules should exist in sys path otherwise deployment won't succeed. Module existence check is
    intentionally deferred to actual deployment (activation) time to give user a chance to avoid chicken-egg dilemma in
    some cases by being able to manipulate the sys path (or do explicit imports) programmatically after the process init
    and before the application activation. Module import is avoided to allow application developers to push dev-time
    incompatible modules to target/runtime systems.
    """
    if app_root:
        DeploymentConfiguration.app_root = app_root
    if app_source_root:
        DeploymentConfiguration.app_source_root = app_source_root
    if app_extra_package_paths:
        DeploymentConfiguration.app_extra_package_paths = app_extra_package_paths
    if app_extra_modules:
        DeploymentConfiguration.app_extra_modules = app_extra_modules


FRAMEWORK_PACKAGE_NAME = "rheoceros"

# TODO read from 'bundle' module programmatically
INTELLIFLOW_CORE_BUNDLE_PATH = os.path.join("intelliflow", "core", "bundle")

INTELLIFLOW_GLUE_DEV_ENDPOINT_LIVY_APP_DIR = "/mnt/yarn/usercache/livy"

# TODO move to 'runtime' module
IS_ON_SAGEMAKER_NOTEBOOK_INSTANCE: bool = False
IS_ON_GLUE_DEV_ENDPOINT: bool = False

_sagemaker_pkg_root = Path(get_sagemaker_notebook_instance_pkg_root(PYTHON_VERSION_MAJOR, PYTHON_VERSION_MINOR))
_livy_app_root = Path(INTELLIFLOW_GLUE_DEV_ENDPOINT_LIVY_APP_DIR)

try:
    if _sagemaker_pkg_root.exists():
        IS_ON_SAGEMAKER_NOTEBOOK_INSTANCE = True
except PermissionError as pe:
    IS_ON_SAGEMAKER_NOTEBOOK_INSTANCE = False

if not IS_ON_SAGEMAKER_NOTEBOOK_INSTANCE:
    try:
        if _livy_app_root.exists():
            IS_ON_GLUE_DEV_ENDPOINT = True
    except PermissionError as pe:
        IS_ON_GLUE_DEV_ENDPOINT = False


INTELLIFLOW_RESOURCE_EXTENSIONS = {".jar"}

# we cannot risk putting precompiled python files into our deployment bundle.
# this will cause compatibility issues in target systems (see "AWS Lambda" cannot import name errors for example).
FILE_EXTS_TO_AVOID = {".pyc"}
# TODO SIZE_OPTIMIZATION
# FILE_EXTS_TO_AVOID = {'.pyc', '.so', '.egg-info'}

# TODO expand
FOLDERS_TO_AVOID = {"cryptography"}


try:
    import zlib

    ZIP_COMPRESSION_TYPE = zipfile.ZIP_DEFLATED
except:
    ZIP_COMPRESSION_TYPE = zipfile.ZIP_STORED

# when using ZIP_DEFLATED integers 0 through 9 are accepted.
# If compress type is not deflated, it is supposed to be ignored
# so safe to set it without any other check.
# (zipfile.write / writestr introduced this in Python 3.7 which is min for RheocerOS)
ZIP_COMPRESSION_LEVEL = 9


def is_environment_immutable() -> bool:
    """Environment is created already and relies on existing remote resources but local setup, artifacts, asssets are
    immutable. Rest of the system should use this hint to decide on which remote resource (bundle, etc) updates are
    necessary or not"""
    return IS_ON_GLUE_DEV_ENDPOINT


def is_on_remote_dev_env() -> bool:
    return IS_ON_SAGEMAKER_NOTEBOOK_INSTANCE or IS_ON_GLUE_DEV_ENDPOINT


def _zipdir(path, zip_stream, include_resources, archive_prefix=None, extra_folders_to_avoid: Set[str] = None) -> Set[str]:
    """
    Parameters
    ----------
    path: path to directory to be zipped. can be both absolute and relative.
    zip_stream: ZipFile handle/stream

    Returns
    -------
    None
    """
    folders_to_avoid = FOLDERS_TO_AVOID | extra_folders_to_avoid if extra_folders_to_avoid else FOLDERS_TO_AVOID
    zipped_set: Set[str] = set()
    for root, dirs, files in os.walk(path, followlinks=True):
        for file in files:
            relative_root = root.replace(path, "")
            if archive_prefix:
                relative_root = os.path.join(archive_prefix, relative_root.lstrip(os.path.sep))

            archive_name = os.path.join(relative_root, file).lstrip(os.path.sep)
            head, tail = os.path.split(archive_name)
            if head and head != tail:
                archive_root = head.split(os.path.sep)[0]
                if archive_root.startswith(tuple(folders_to_avoid)):
                    continue
            # check the file is compiled (CPython) file, avoid compatibility issues with target
            # systems (AWS Lambda, etc)
            ext = Path(file).suffix.lower()
            if ext not in FILE_EXTS_TO_AVOID:
                if ext not in INTELLIFLOW_RESOURCE_EXTENSIONS or include_resources:
                    zipped_set.add(archive_name.replace(os.path.sep, "/"))
                    zip_stream.write(
                        os.path.join(root, file), archive_name, compress_type=ZIP_COMPRESSION_TYPE, compresslevel=ZIP_COMPRESSION_LEVEL
                    )
    return zipped_set


def _get_app_root_dir() -> Path:
    if IS_ON_SAGEMAKER_NOTEBOOK_INSTANCE:
        # belongs to wherever the notebook file is located at.
        # currently not used in an activation/deployment for SageMaker case but
        # implementing this for the sake of consistency and in case of future support
        # for same-folder python module (e.g 'src') upload.
        return Path.cwd()
    else:
        if_app_root = DeploymentConfiguration.app_root
        if if_app_root:
            app_root_dir = Path(if_app_root)
            if not app_root_dir.exists():
                raise ValueError(
                    f"The path {if_app_root!r} provided as application root directory via deployment env parameter "
                    f"{DeploymentConfiguration.APP_ROOT} does not exist! Either unset it "
                    f"for auto-detection of application root directory or define it correctly."
                )
            return app_root_dir
        app_root_dir = Path.cwd()
        # traverse up till app root is found
        search_dir = app_root_dir
        while True:
            # refer "packaging.python.org" for more details on "pyproject.toml"
            # treat it as 1st class indicator for app Python project root, in other cases be paranoid.
            # 'pyproject.toml' also gives us a better chance to cover projects that don't use setuptools.
            if (search_dir / "pyproject.toml").exists() or (
                (search_dir / "requirements.txt").exists() and ((search_dir / "setup.py").exists() or (search_dir / "setup.cfg").exists())
            ):
                logger.critical(f"{DeploymentConfiguration.APP_ROOT} is automatically detected as '{search_dir}'.")
                return search_dir
            search_dir = search_dir.parent
            if search_dir == Path(search_dir.root):
                break
        logger.warning(
            f"{DeploymentConfiguration.APP_ROOT} could not be detected in the folder hierarchy! "
            f"Will use process root {app_root_dir} instead. If you receive 'module not found' errors "
            f"at runtime, please consider setting deployment parameters via 'set_deployment_conf' method or"
            f"set {DeploymentConfiguration.APP_ROOT} as an environment variable explicitly."
        )
        return app_root_dir


def _get_app_src_dir() -> Optional[str]:
    if DeploymentConfiguration.app_source_root:
        if_app_src_path = Path(DeploymentConfiguration.app_source_root)
        if if_app_src_path.exists() and if_app_src_path.is_dir():
            return DeploymentConfiguration.app_source_root
        else:
            raise ValueError(
                f"Application source folder {DeploymentConfiguration.app_source_root!r} provided as part "
                f"of {DeploymentConfiguration.APP_SOURCE_ROOT} is not valid!"
                f" It should be a relative or absolute path to a valid folder on the"
                f" file system, and it should contain all of the root level source packages"
                f" to be bundled during the deployment."
            )

    root_dir: Path = _get_app_root_dir()
    app_pysrc_dir = root_dir / "pysrc"
    app_src_dir = root_dir / "src"
    if app_pysrc_dir.exists():
        return str(app_pysrc_dir)
    elif app_src_dir.exists():
        return str(app_src_dir)
    else:
        return None


def get_dependency_path():
    dependency_path = None
    if IS_ON_SAGEMAKER_NOTEBOOK_INSTANCE:
        dependency_path = str(_sagemaker_pkg_root)
    else:
        app_root_dir = _get_app_root_dir()
        if (app_root_dir / "venv").exists():
            # use virtual env bundle
            if (app_root_dir / "venv" / "Lib" / "site-packages").exists():
                dependency_path = str(app_root_dir / "venv" / "Lib" / "site-packages")
            elif (app_root_dir / "venv" / "lib" / "site-packages").exists():
                dependency_path = str(app_root_dir / "venv" / "lib" / "site-packages")
            elif (app_root_dir / "venv" / "lib" / f"python{PYTHON_VERSION_MAJOR}.{PYTHON_VERSION_MINOR}" / "site-packages").exists():
                dependency_path = str(
                    app_root_dir / "venv" / "lib" / f"python{PYTHON_VERSION_MAJOR}.{PYTHON_VERSION_MINOR}" / "site-packages"
                )
            else:
                logger.critical("Unrecognized project structure. Cannot resolve virtual dependencies!")

        else:
            logger.critical(
                "Unrecognized project structure. Cannot resolve dependencies!"
                "If you are on a notebook, please configure RheocerOS accordingly"
            )

    if dependency_path:
        logger.critical(f"Deployment dependency_path detected as {dependency_path!r}")
    else:
        logger.warning(f"Deployment dependency_path could not be detected on the host platform!")

    return dependency_path


_working_set_cache = {}


def get_working_set_as_zip_stream(
    include_resources=False,
    use_embedded_bundle=False,
    dependencies_path: str = None,
    source_path: str = None,
    extra_folders_to_avoid: Set[str] = None,
):
    if is_environment_immutable():
        raise RuntimeError(
            f"Bundle generation is not supported on this immutable development environment. " f"Use already deployed bundle."
        )

    # Not thread-safe.
    # TODO RheocerOS concurrent app development, in the context of experimentation, integ-tests, etc
    set_key: Tuple[bool, bool, Optional[str], Optional[str], Union[Tuple[str, ...], Set[str], None], Tuple[Any, ...]] = (
        include_resources,
        use_embedded_bundle,
        dependencies_path,
        source_path,
        tuple(extra_folders_to_avoid) if extra_folders_to_avoid else extra_folders_to_avoid,
        tuple(
            tuple(v) if isinstance(v, set) else str(v)
            for v in (
                DeploymentConfiguration.app_root,
                DeploymentConfiguration.app_source_root,
                DeploymentConfiguration.app_extra_package_paths,
                DeploymentConfiguration.app_extra_modules,
            )
        ),
    )
    working_set = _working_set_cache.get(set_key, None)
    if not working_set:
        try:
            working_set = _get_working_set_as_zip_stream(
                include_resources, use_embedded_bundle, dependencies_path, source_path, extra_folders_to_avoid
            )
        except Exception as err:
            logger.error(f"Cannot retrieve/create RheocerOS working set! Error: {str(err)}")
            raise

        _working_set_cache[set_key] = working_set
    return working_set


def _get_working_set_as_zip_stream(
    include_resources=False, use_embedded_bundle=False, dependencies_path=None, source_path=None, extra_folders_to_avoid: Set[str] = None
):
    from importlib.resources import read_binary, contents, path
    from . import bundle

    buffer = io.BytesIO()
    bundle_zip_file = None
    # use bundle by default if not on amzn
    force_bundle = False

    app_name = _get_app_root_dir().name
    if not (IS_ON_SAGEMAKER_NOTEBOOK_INSTANCE or IS_ON_GLUE_DEV_ENDPOINT):
        # convenience (best-effort) support for IDEs: PyCharm uses python path lib modification among dependent packages.
        # not-critical for productionization scenarios or notebook support.
        if app_name != FRAMEWORK_PACKAGE_NAME:
            force_bundle = True

    if use_embedded_bundle or force_bundle:
        # data = read_binary(bundle, INTELLIFLOW_CORE_BUNDLE_NAME)
        for resource in contents(bundle):
            with path(bundle, resource) as resource_path:
                if zipfile.is_zipfile(resource_path):
                    bundle_zip_file = resource_path

    dependencies = dependencies_path if dependencies_path else get_dependency_path()
    source = source_path if source_path else _get_app_src_dir()

    if not (dependencies or source):
        if bundle_zip_file:
            return read_binary(bundle, bundle_zip_file)
        else:
            if not use_embedded_bundle:
                err_msg = (
                    "Cannot create a working set! Please consider enabling the use of default core bundle"
                )
            else:
                err_msg = (
                    "Cannot create a working set! Project layout is not recognized and also framework"
                    " core bundle / dependency closure cannot be found within the library."
                )
            logger.error(err_msg)
            raise RuntimeError(err_msg)

    with zipfile.ZipFile(buffer, "w") as zip_stream:
        # zipped_set not check in subsequent calls to allow overwrites in order of
        #   (1) dependencies (farm, virtualenv) -> (2) app source -> (3) explicit pkg or module declarations
        if dependencies:
            zipped_set = _zipdir(dependencies, zip_stream, include_resources, extra_folders_to_avoid=extra_folders_to_avoid)

            if not (IS_ON_SAGEMAKER_NOTEBOOK_INSTANCE or IS_ON_GLUE_DEV_ENDPOINT):
                if app_name != FRAMEWORK_PACKAGE_NAME and "intelliflow/__init__.py" not in zipped_set:
                    # convenience feature to pull intelliflow core into the working set
                    # when it is in the path as a sister project (i.e PyCharm).
                    if_spec = importlib.util.find_spec("intelliflow")
                    if if_spec:
                        if_path = str(Path(if_spec.loader.path).parent)
                        _zipdir(if_path, zip_stream, include_resources, "intelliflow")
                    elif bundle_zip_file:
                        # try to pull core framework from the bundle if not in dependencies.
                        with zipfile.ZipFile(bundle_zip_file, "r") as bundle_zip:
                            for if_core_package in bundle_zip.namelist():
                                if if_core_package not in zipped_set and if_core_package.startswith("intelliflow"):
                                    # TODO / FIXME this write operation messes up with permissions
                                    # currently we are copying core src over to app venv for non-amzn deployments.
                                    zip_stream.writestr(if_core_package, bundle_zip.open(if_core_package).read())
        elif bundle_zip_file:
            with zipfile.ZipFile(bundle_zip_file, "r") as bundle_zip:
                for if_core_package in bundle_zip.namelist():
                    # TODO / FIXME this write operation messes up with permissions
                    # currently we are copying core src over to app venv for non-amzn deployments.
                    zip_stream.writestr(if_core_package, bundle_zip.open(if_core_package).read())

        if source:
            _zipdir(source, zip_stream, include_resources)

        if DeploymentConfiguration.app_extra_modules:
            for extra_module in DeploymentConfiguration.app_extra_modules:
                # find without importing them
                try:
                    m_spec = importlib.util.find_spec(extra_module)
                except ModuleNotFoundError:
                    m_spec = None
                if m_spec:
                    m_dir_path = str(Path(m_spec.loader.path).parent)
                    # e.g "module_root.foo.bar" will be packed with the content of 'bar' only but with an archive
                    # prefix of 'module_root/foo/bar' so that import statements will still be rooted at 'module_root'
                    _zipdir(m_dir_path, zip_stream, include_resources, extra_module.replace(".", os.path.sep))
                else:
                    # see doc for 'set_deployment_conf' for the deferral of validation till actual bundling/deployment
                    raise ValueError(
                        f"Module {extra_module!r} provided as part of {DeploymentConfiguration.APP_EXTRA_MODULES}"
                        f" cannot be found! If it is intentionally kept out of the sys path, then consider"
                        f" using {DeploymentConfiguration.APP_EXTRA_PACKAGE_PATHS} with relative or"
                        f" absolute paths to its folder location."
                    )

        if DeploymentConfiguration.app_extra_package_paths:
            for extra_pkg_path in DeploymentConfiguration.app_extra_package_paths:
                pkg_path = Path(extra_pkg_path)
                if pkg_path.exists() and pkg_path.is_dir() and (pkg_path / "__init__.py").exists():
                    _zipdir(extra_pkg_path, zip_stream, include_resources, pkg_path.name)
                else:
                    raise ValueError(
                        f"Python package path {extra_pkg_path!r} provided as part of "
                        f"{DeploymentConfiguration.APP_EXTRA_PACKAGE_PATHS} is not valid!"
                        f" It should be a relative or absolute path to a valid Python package folder on the"
                        f" file system, with a '__init__.py' in it."
                    )

    buffer.seek(0)
    return buffer.read()
