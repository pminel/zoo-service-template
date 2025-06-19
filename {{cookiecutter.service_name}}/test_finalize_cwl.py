import yaml

from cwl_editor import finalize_cwl

def test_update_main_workflow():
    with open("../reference_user.cwl", "r") as f:
        cwl_in_yaml = yaml.safe_load(f.read())

    cwl_out = finalize_cwl(cwl_in_yaml)

    graphs = cwl_out["$graph"]
    assert len(graphs) == 6

    graph_ids = [graph["id"] for graph in graphs]
    assert "analyse" in graph_ids
    assert "stageout_data_analysis" in graph_ids
    assert "split_tiles" in graph_ids
    assert "process" in graph_ids
    assert "merge_results" in graph_ids