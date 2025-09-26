import yaml
import sys
from pathlib import Path

TEMPLATE_DIR = Path(__file__).resolve().parent.parent / "{{cookiecutter.service_name}}"
sys.path.insert(0, str(TEMPLATE_DIR))

import cwl_helper


def test_update_main_workflow():
    with open("tests/reference_user.cwl", "r") as f:
        cwl_in_yaml = yaml.safe_load(f.read())

    cwl_out = cwl_helper.finalize_cwl(cwl_in_yaml)

    with open("tests/reference_user_finalized.cwl", "w") as f:
        f.write(yaml.dump(cwl_out))

    # check number of graphs
    graphs = cwl_out["$graph"]

    # check graph ids
    graph_ids = [graph["id"] for graph in graphs]
    assert "analyse" in graph_ids
    assert "data_analysis_results_interceptor" in graph_ids
    assert "process" in graph_ids
    assert "process_results_interceptor" in graph_ids
