################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import json
import os
import uuid

__all__ = ['DependencyManager']


class DependencyManager(object):
    """
    Container class of dependency-related parameters. It collects all the dependency parameters
    and transmit them to JVM before executing job.
    """

    PYTHON_FILE_PREFIX = "python_file"
    PYTHON_REQUIREMENTS_FILE_PREFIX = "python_requirements_file"
    PYTHON_REQUIREMENTS_CACHE_PREFIX = "python_requirements_cache"
    PYTHON_ARCHIVE_PREFIX = "python_archive"

    PYTHON_FILE_MAP = "python.environment.file-map"
    PYTHON_REQUIREMENTS_FILE = "python.environment.requirements-file"
    PYTHON_REQUIREMENTS_CACHE = "python.environment.requirements-cache"
    PYTHON_ARCHIVES_MAP = "python.environment.archive-map"
    PYTHON_EXEC = "python.environment.exec"

    _prefix_map = {
        PYTHON_FILE_MAP: PYTHON_FILE_PREFIX,
        PYTHON_REQUIREMENTS_FILE: PYTHON_REQUIREMENTS_FILE_PREFIX,
        PYTHON_REQUIREMENTS_CACHE: PYTHON_REQUIREMENTS_CACHE_PREFIX,
        PYTHON_ARCHIVES_MAP: PYTHON_ARCHIVE_PREFIX}

    def __init__(self, parameters, j_env):

        self._parameters = parameters
        self._j_env = j_env
        self._python_file_map = dict()  # type: dict[str, str]
        self._archives_map = dict()  # type: dict[str, str]
        self._counter_map = {self.PYTHON_FILE_PREFIX: -1, self.PYTHON_ARCHIVE_PREFIX: -1}

    def add_python_file(self, file_path):

        key = self._register_file(self.PYTHON_FILE_MAP, file_path)
        self._python_file_map[key] = os.path.basename(file_path)
        self._parameters.set_string(
            self.PYTHON_FILE_MAP, json.dumps(self._python_file_map))

    def set_python_requirements(self, requirements_file_path, requirements_cached_dir=None):

        self._remove_file_if_exists(self.PYTHON_REQUIREMENTS_FILE)
        self._remove_file_if_exists(self.PYTHON_REQUIREMENTS_CACHE)

        self._register_file(self.PYTHON_REQUIREMENTS_FILE,
                            requirements_file_path,
                            set_parameters=True)
        if requirements_cached_dir is not None:
            self._register_file(self.PYTHON_REQUIREMENTS_CACHE,
                                requirements_cached_dir,
                                set_parameters=True)

    def add_python_archive(self, archive_path, target_dir=None):

        key = self._register_file(self.PYTHON_ARCHIVES_MAP, archive_path)
        if target_dir is not None:
            self._archives_map[key] = target_dir
        else:
            self._archives_map[key] = os.path.basename(archive_path)
        self._parameters.set_string(self.PYTHON_ARCHIVES_MAP, json.dumps(self._archives_map))

    def _generate_file_key(self, parameter_name):

        prefix = self._prefix_map[parameter_name]
        if prefix in self._counter_map:
            self._counter_map[prefix] += 1
            return "%s_%d_%s" % (prefix, self._counter_map[prefix], uuid.uuid4())
        else:
            return "%s_%s" % (prefix, uuid.uuid4())

    def _remove_file_if_exists(self, parameter_name):

        if self._parameters.contains_key(parameter_name):
            key = self._parameters.get_string(parameter_name, "")
            cache_files = self._j_env.getCachedFiles()
            for key_file_tuple in cache_files:
                if key == key_file_tuple.f0:
                    cache_files.remove(key_file_tuple)
                    break
            self._parameters.remove_config(parameter_name)

    def _register_file(self, parameter_name, file_path, set_parameters=False):

        file_key = self._generate_file_key(parameter_name)
        self._j_env.registerCachedFile(file_path, file_key)
        if set_parameters:
            self._parameters.set_string(parameter_name, file_key)
        return file_key
