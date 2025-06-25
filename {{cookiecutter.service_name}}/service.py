from __future__ import annotations
from typing import Dict
import pathlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import cwl_helper


try:
    import zoo
except ImportError:

    class ZooStub(object):
        def __init__(self):
            self.SERVICE_SUCCEEDED = 3
            self.SERVICE_FAILED = 4

        def update_status(self, conf, progress):
            print(f"Status {progress}")

        def _(self, message):
            print(f"invoked _ with {message}")

    zoo = ZooStub()

import os
import sys
import traceback
import yaml
import json
import boto3  # noqa: F401
import botocore
from loguru import logger
from urllib.parse import urlparse
from botocore.exceptions import ClientError
from botocore.client import Config
from pystac import read_file
from pystac.stac_io import DefaultStacIO, StacIO
from pystac.item_collection import ItemCollection
from zoo_calrissian_runner import ExecutionHandler, ZooCalrissianRunner


logger.remove()
logger.add(sys.stderr, level="INFO")


class SimpleExecutionHandler(ExecutionHandler):
    def __init__(self, conf):
        super().__init__()
        self.conf = conf
        self.results = None

    def pre_execution_hook(self):

        logger.info("Pre execution hook")
        input_request = self.conf['request']['jrequest']
        import json
        service_name = json.loads(input_request)['inputs']['thematic_service_name']
        logger.info(f"Thematic service name: {service_name}")
        self.conf['thematic_service_name'] = service_name
        
        stageout_yaml = yaml.safe_load(open("/assets/stageout.yaml","rb"))
        
        logger.info(f"Stageout: {stageout_yaml}")
        logger.info("WRAPPER_STAGE_OUT" in os.environ)

        self.stageout_file_path = f"/{self.conf['main']['tmpPath']}/stageout{self.conf['lenv']['usid']}.yaml"
        stageout_file=open(self.stageout_file_path,"w")
        yaml.dump(stageout_yaml,stageout_file)
        stageout_file.close()
        os.environ["WRAPPER_STAGE_OUT"] = self.stageout_file_path

        logger.info("WRAPPER_STAGE_OUT" in os.environ)

    def post_execution_hook(self, log, output, usage_report, tool_logs):

        # unset HTTP proxy or else the S3 client will use it and fail
        os.environ.pop("HTTP_PROXY", None)

        logger.info("Post execution hook")

    def _get_env_var(self, prefix):
        identifier = '{}_{}'.format(prefix, self.conf['thematic_service_name'].upper())
        value = self.conf['pod_env_vars'].get(identifier)
        if not value:
            raise ValueError("No env var found named {}".format(identifier))
        return value
    
    def get_pod_env_vars(self):
        # This method is used to set environment variables for the pod
        # spawned by calrissian.

        logger.info("get_pod_env_vars")
        
        
        bucket_name = self._get_env_var("S3_BUCKET_ADDRESS")
        access_key = self._get_env_var("S3_BUCKET_ACCESS_KEY")
        access_secret_key = self._get_env_var("S3_BUCKET_SECRET_KEY")  # corrected if needed
        
        env_vars = {
            "S3_BUCKET_NAME": bucket_name,
            "AWS_ACCESS_KEY_ID": access_key,
            "AWS_SECRET_ACCESS_KEY": access_secret_key,
            "AWS_DEFAULT_REGION": "eu-central-1",
            "PROCESS_ID": self.conf["lenv"]["usid"],
            "SERVICE_NAME": self.conf['thematic_service_name']
        }

        return env_vars

    def get_pod_node_selector(self):
        # This method is used to set node selectors for the pod
        # spawned by calrissian.

        logger.info("get_pod_node_selector")

        node_selector = {}

        return node_selector

    def get_additional_parameters(self):
        # sets the additional parameters for the execution
        # of the wrapped Application Package

        logger.info("get_additional_parameters")
        additional_parameters: Dict[str, str] = {}
        additional_parameters = self.conf.get("additional_parameters", {})
        
        additional_parameters["sub_path"] = self.conf["lenv"]["usid"]

        logger.info(f"additional_parameters: {additional_parameters.keys()}")
        import json
        logger.info(json.dumps(self.conf))
        return additional_parameters

    def handle_outputs(self, log, output, usage_report, tool_logs):
        """
        Handle the output files of the execution.

        :param log: The application log file of the execution.
        :param output: The output file of the execution.
        :param usage_report: The metrics file.
        :param tool_logs: A list of paths to individual workflow step logs.

        """
        try:
            logger.info("handle_outputs")
            logger.info(tool_logs)
            logger.info(output)
            logger.info(log)
            logger.info(usage_report)
        except Exception as e:
            logger.error("ERROR in handle_outputs...")
            logger.error(traceback.format_exc())
            raise (e)

    def get_secrets(self):
        return {}


def {{cookiecutter.workflow_id |replace("-", "_")  }}(conf, inputs, outputs):  # noqa

    try:
        logger.info(inputs)
        with open(
            os.path.join(
                pathlib.Path(os.path.realpath(__file__)).parent.absolute(),
                "app-package.cwl",
            ),
            "r",
        ) as stream:
            cwl = yaml.safe_load(stream)

        execution_handler = SimpleExecutionHandler(conf=conf)

        finalized_cwl = cwl_helper.finalize_cwl(cwl)
        print("** finalized_cwl **")
        print(finalized_cwl)
        print("** finalized_cwl **")

        runner = ZooCalrissianRunner(
            cwl=finalized_cwl,
            conf=conf,
            inputs=inputs,
            outputs=outputs,
            execution_handler=execution_handler,
        )

        working_dir = os.path.join(conf["main"]["tmpPath"], runner.get_namespace_name())
        os.makedirs(
            working_dir,
            mode=0o777,
            exist_ok=True,
        )
        os.chdir(working_dir)

        exit_status = runner.execute()

        if exit_status == zoo.SERVICE_SUCCEEDED:
            """logger.info(f"Setting Collection into output key {list(outputs.keys())[0]}")
            outputs["stac_catalog"]["value"] = json.dumps(
                execution_handler.results, indent=2
            )"""
            return zoo.SERVICE_SUCCEEDED

        else:
            conf["lenv"]["message"] = zoo._("Execution failed")
            logger.error("Execution failed")
            return zoo.SERVICE_FAILED

    except Exception as e:

        logger.error("ERROR in processing execution template...")
        logger.error("Try to fetch the tool logs if any...")

        try:
            tool_logs = runner.execution.get_tool_logs()
            execution_handler.handle_outputs(None, None, None, tool_logs)
        except Exception as e:
            logger.error(f"Fetching tool logs failed! ({str(e)})")

        stack = traceback.format_exc()

        logger.error(stack)

        conf["lenv"]["message"] = zoo._(f"Exception during execution...\n{stack}\n")

        return zoo.SERVICE_FAILED
