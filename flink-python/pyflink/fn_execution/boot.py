#!/usr/bin/env python
#################################################################################
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
"""
This script is a python implementation of the "boot.go" script in "beam-sdks-python-container"
project of Apache Beam, see in:

https://github.com/apache/beam/blob/release-2.14.0/sdks/python/container/boot.go

It is implemented in golang and will introduce unnecessary dependencies if used in pure python
project. So we add a python implementation which will be used when the python worker runs in
process mode. It downloads and installs users' python artifacts, then launches the python SDK
harness of Apache Beam.
"""
import shlex
import argparse
import hashlib
import os
from subprocess import call

import grpc
import logging
import sys

from apache_beam.portability.api.beam_provision_api_pb2_grpc import ProvisionServiceStub
from apache_beam.portability.api.beam_provision_api_pb2 import GetProvisionInfoRequest
from apache_beam.portability.api.beam_artifact_api_pb2_grpc import ArtifactRetrievalServiceStub
from apache_beam.portability.api.beam_artifact_api_pb2 import (GetManifestRequest,
                                                               GetArtifactRequest)
from apache_beam.portability.api.endpoints_pb2 import ApiServiceDescriptor

from google.protobuf import json_format, text_format


def check_not_empty(check_str, error_message):
    if check_str == "":
        logging.fatal(error_message)
        exit(1)


python_exec = sys.executable

PYTHON_REQUIREMENTS_FILE = "_PYTHON_REQUIREMENTS_FILE"
PYTHON_REQUIREMENTS_CACHE = "_PYTHON_REQUIREMENTS_CACHE"
PYTHON_REQUIREMENTS_DIR_ENV = "_PYTHON_REQUIREMENTS_INSTALL_DIR"


def pip_install_requirements_process_mode():
    if (PYTHON_REQUIREMENTS_FILE in os.environ
            and PYTHON_REQUIREMENTS_DIR_ENV in os.environ):
        requirements_file_path = os.environ[PYTHON_REQUIREMENTS_FILE]
        requirements_target_path = os.environ[PYTHON_REQUIREMENTS_DIR_ENV]
        requirements_dir_path = None

        if PYTHON_REQUIREMENTS_CACHE in os.environ:
            requirements_dir_path = os.environ[PYTHON_REQUIREMENTS_CACHE]

        if "PATH" in os.environ:
            os.environ["PATH"] = \
                os.pathsep.join(
                    [os.path.join(requirements_target_path, "bin"), os.environ["PATH"]])
        else:
            os.environ["PATH"] = os.path.join(requirements_target_path, "bin")
        env = dict(os.environ)
        from distutils.dist import Distribution
        install_obj = Distribution().get_command_obj('install', create=True)
        install_obj.prefix = requirements_target_path
        install_obj.finalize_options()
        installed_dir = [install_obj.install_purelib]
        if install_obj.install_purelib != install_obj.install_platlib:
            installed_dir.append(install_obj.install_platlib)
        if len(installed_dir) > 0:
            installed_python_path = os.pathsep.join(installed_dir)
            if "PYTHONPATH" in env:
                env["PYTHONPATH"] = os.pathsep.join(
                    [installed_python_path, env["PYTHONPATH"]])
            else:
                env["PYTHONPATH"] = installed_python_path
        # since '--prefix' option is only supported for pip 8.0+, so here we fallback to
        # use '--install-option' when the pip version is lower than 8.0.0.
        from pkg_resources import get_distribution, parse_version
        pip_version = get_distribution("pip").version
        line_continuation = None
        with open(requirements_file_path, "r") as f:
            while True:
                text_line = f.readline()
                if text_line:
                    if line_continuation is not None:
                        text_line = text_line.strip("\n\r")
                    else:
                        text_line = text_line.strip()
                    if text_line.startswith("#"):
                        continue
                    if line_continuation is not None:
                        text_line = line_continuation + text_line
                    if text_line.strip().endswith("\\"):
                        line_continuation = text_line
                        continue
                    if len(text_line.strip()) == 0:
                        continue
                    user_args = shlex.split(text_line)
                    py_args = [python_exec, "-m", "pip", "install"]
                    py_args.extend(user_args)
                    if parse_version(pip_version) >= parse_version('8.0.0'):
                        py_args.extend(["--prefix", requirements_target_path, "--ignore-installed"])
                    else:
                        py_args.extend(['--install-option', '--prefix=' + requirements_target_path,
                                        "--ignore-installed"])
                    if requirements_dir_path is not None:
                        py_args.extend(["--find-links", requirements_dir_path])
                    logging.info("Run command: %s\n" % " ".join(py_args))
                    exit_code = call(py_args, stdout=sys.stdout, stderr=sys.stderr, env=env)
                    if exit_code > 0:
                        raise Exception("Run command: %s error! exit code: %d" % (" ".join(py_args),
                                                                                  exit_code))
                    line_continuation = None
                else:
                    break
        os.environ["PYTHONPATH"] = env["PYTHONPATH"]
    return 0


if __name__ == "__main__":
    # do not print DEBUG and TRACE messages
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    parser = argparse.ArgumentParser()

    parser.add_argument("--id", default="", help="Local identifier (required).")
    parser.add_argument("--logging_endpoint", default="",
                        help="Logging endpoint (required).")
    parser.add_argument("--artifact_endpoint", default="",
                        help="Artifact endpoint (required).")
    parser.add_argument("--provision_endpoint", default="",
                        help="Provision endpoint (required).")
    parser.add_argument("--control_endpoint", default="",
                        help="Control endpoint (required).")
    parser.add_argument("--semi_persist_dir", default="/tmp",
                        help="Local semi-persistent directory (optional).")

    args = parser.parse_args()

    worker_id = args.id
    logging_endpoint = args.logging_endpoint
    artifact_endpoint = args.artifact_endpoint
    provision_endpoint = args.provision_endpoint
    control_endpoint = args.control_endpoint
    semi_persist_dir = args.semi_persist_dir

    check_not_empty(worker_id, "No id provided.")
    check_not_empty(logging_endpoint, "No logging endpoint provided.")
    check_not_empty(artifact_endpoint, "No artifact endpoint provided.")
    check_not_empty(provision_endpoint, "No provision endpoint provided.")
    check_not_empty(control_endpoint, "No control endpoint provided.")

    logging.info("Initializing python harness: %s" % " ".join(sys.argv))

    metadata = [("worker_id", worker_id)]

    # read job information from provision stub
    with grpc.insecure_channel(provision_endpoint) as channel:
        client = ProvisionServiceStub(channel=channel)
        info = client.GetProvisionInfo(GetProvisionInfoRequest(), metadata=metadata).info
        options = json_format.MessageToJson(info.pipeline_options)

    staged_dir = os.path.join(semi_persist_dir, "staged")

    # download files
    with grpc.insecure_channel(artifact_endpoint) as channel:
        client = ArtifactRetrievalServiceStub(channel=channel)
        # get file list via retrieval token
        response = client.GetManifest(GetManifestRequest(retrieval_token=info.retrieval_token),
                                      metadata=metadata)
        artifacts = response.manifest.artifact
        # download files and check hash values
        for artifact in artifacts:
            name = artifact.name
            permissions = artifact.permissions
            sha256 = artifact.sha256
            file_path = os.path.join(staged_dir, name)
            if os.path.exists(file_path):
                with open(file_path, "rb") as f:
                    sha256obj = hashlib.sha256()
                    sha256obj.update(f.read())
                    hash_value = sha256obj.hexdigest()
                if hash_value == sha256:
                    logging.info("The file: %s already exists and its sha256 hash value: %s is the "
                                 "same as the expected hash value, skipped." % (file_path, sha256))
                    continue
                else:
                    os.remove(file_path)
            if not os.path.exists(os.path.dirname(file_path)):
                os.makedirs(os.path.dirname(file_path), 0o755)
            stream = client.GetArtifact(
                GetArtifactRequest(name=name, retrieval_token=info.retrieval_token),
                metadata=metadata)
            with open(file_path, "wb") as f:
                sha256obj = hashlib.sha256()
                for artifact_chunk in stream:
                    sha256obj.update(artifact_chunk.data)
                    f.write(artifact_chunk.data)
                hash_value = sha256obj.hexdigest()
            if hash_value != sha256:
                raise Exception("The sha256 hash value: %s of the downloaded file: %s is not the"
                                " same as the expected hash value: %s" %
                                (hash_value, file_path, sha256))
            os.chmod(file_path, int(str(permissions), 8))

    pip_install_requirements_process_mode()

    os.environ["WORKER_ID"] = worker_id
    os.environ["PIPELINE_OPTIONS"] = options
    os.environ["SEMI_PERSISTENT_DIRECTORY"] = semi_persist_dir
    os.environ["LOGGING_API_SERVICE_DESCRIPTOR"] = text_format.MessageToString(
        ApiServiceDescriptor(url=logging_endpoint))
    os.environ["CONTROL_API_SERVICE_DESCRIPTOR"] = text_format.MessageToString(
        ApiServiceDescriptor(url=control_endpoint))

    env = dict(os.environ)

    if "FLINK_BOOT_TESTING" in os.environ and os.environ["FLINK_BOOT_TESTING"] == "1":
        exit(0)

    call([python_exec, "-m", "pyflink.fn_execution.sdk_worker_main"],
         stdout=sys.stdout, stderr=sys.stderr, env=env)
