
import os
from typing import Dict, List, Optional,  TextIO
from kubernetes.utils import create_from_dict
from kubernetes import client, config
from kubernetes.client import Configuration
from pydantic import BaseModel
from loguru import logger



class Manifest(BaseModel):
    name: str
    key: str
    content: Optional[List[Dict]] = None
    persist: Optional[bool] = True

def get_api_client(kubeconfig_file: TextIO = None):

    proxy_url = os.getenv("HTTP_PROXY", None)
    kubeconfig = os.getenv("KUBECONFIG", None)

    if proxy_url:
        api_config = Configuration(host=proxy_url)
        api_config.proxy = proxy_url
        api_client = client.ApiClient(api_config)

    elif kubeconfig:
        # this is needed because kubernetes-python does not consider
        # the KUBECONFIG env variable
        config.load_kube_config(config_file=kubeconfig)
        api_client = client.ApiClient()
    elif kubeconfig_file:
        config.load_kube_config(config_file=kubeconfig)
        api_client = client.ApiClient()
    else:
        # if nothing is specified, kubernetes-python will use the file
        # in ~/.kube/config
        config.load_kube_config()
        api_client = client.ApiClient()


def apply_manifests(manifest, namespace):
    
    api_client = get_api_client()

    def apply_manifest(manifest):

        create_from_dict(
            k8s_client=api_client,
            data=manifest,
            verbose=True,
            namespace=namespace,
        )


    logger.info(f"Apply manifest {manifest.name}")

        
    for k8_object in manifest.content:
        try:
            # Log the object and its type
            logger.info(f"K8 Object: {k8_object}")
            logger.info(f"Object Type: {type(k8_object)}")

            # Check and log the 'kind' of the Kubernetes object
            if 'kind' in k8_object:
                logger.info(f"Applying manifest of kind: {k8_object['kind']}")
                apply_manifest(k8_object)  # Apply the manifest
            else:
                logger.warning(f"Manifest does not contain a 'kind': {k8_object}")

        except Exception as err:
            logger.error(f"Unexpected {err}, {type(err)}")
            logger.error(
                f"Skipping creation of manifest {manifest.name}"
            )