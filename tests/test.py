

import os
import sys
import unittest

import requests
from cookiecutter.main import cookiecutter
from loguru import logger
import yaml
from tests.helpers import Manifest, apply_manifests

class TestExecutionHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):

        cls.conf = {}
        cls.conf["auth_env"] = {"user": "alice"}
        cls.conf["lenv"] = {"message": ""}
        cls.conf["lenv"] = {
            "Identifier": "water-bodies",
            "usid": "cool-collection-2",
        }
        cls.conf["tmpPath"] = "/tmp"
        cls.conf["main"] = {
            "tmpPath": "/tmp",
            "tmpUrl": "http://localhost:8080",
        }

        cls.conf["additional_parameters"] = {}

        with open(f"{os.path.dirname(__file__)}/assets/manifest.yaml", "r") as f:
            content = yaml.safe_load_all(f.read())

        localstack_manifest = Manifest(
            name="manifests", key="manifests", readonly=True, persist=False, content=[e for e in content]
        )

        namespace = f"{cls.conf['lenv']['Identifier']}-{cls.conf['lenv']['usid']}"

        # create namespace
        apply_manifests(manifest=Manifest(name="namespace", key="namespace", content=[{"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": namespace}}]), namespace="")

        apply_manifests(manifest=localstack_manifest, namespace=namespace)
        
        cls.service_name = "water_bodies"
        cls.workflow_id = "water-bodies"

        cookiecutter_values = {
            "service_name": cls.service_name,
            "workflow_id": cls.workflow_id,
        }

        os.environ[
            "WRAPPER_STAGE_IN"
        ] = f"{os.path.dirname(__file__)}/assets/stagein.yaml"
        os.environ[
            "WRAPPER_STAGE_OUT"
        ] = f"{os.path.dirname(__file__)}/assets/stageout.yaml"
        os.environ["WRAPPER_MAIN"] = f"{os.path.dirname(__file__)}/assets/main.yaml"
        os.environ["WRAPPER_RULES"] = f"{os.path.dirname(__file__)}/assets/rules.yaml"

        os.environ["DEFAULT_VOLUME_SIZE"] = "10000"
        os.environ["STORAGE_CLASS"] = "standard"

        template_folder = f"{os.path.dirname(__file__)}/.."

        service_tmp_folder = "tests/"

        cookiecutter(
            template_folder,
            extra_context=cookiecutter_values,
            output_dir=service_tmp_folder,
            no_input=True,
            overwrite_if_exists=True,
        )

        cls.inputs = {
            "aoi": {"value": "-121.399,39.834,-120.74,40.472"},
            "bands": {"value": ["green", "nir"]},
            "epsg": {"value": "EPSG:4326"},
            "stac_items": {
                "value": [
                    "https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a/items/S2A_10TFK_20210708_0_L2A",  # noqa
                ]
            },
        }

        cls.outputs = {}
        cls.outputs["stac"] = {"value": ""}

    def test_runner(self):
        from tests.water_bodies.service import water_bodies
        print(self.conf)
        water_bodies(self.conf, self.inputs, self.outputs)