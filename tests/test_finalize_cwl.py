import yaml
import sys
from pathlib import Path

TEMPLATE_DIR = Path(__file__).resolve().parent.parent / "{{cookiecutter.service_name}}"
sys.path.insert(0, str(TEMPLATE_DIR))

import cwl_helper


def test_update_main_workflow():
    with open("reference_user.cwl", "r") as f:
        cwl_in_yaml = yaml.safe_load(f.read())

    cwl_out = cwl_helper.finalize_cwl(cwl_in_yaml)

    with open("reference_user_finalized.cwl", "w") as f:
        f.write(yaml.dump(cwl_out))

    # check number of graphs
    graphs = cwl_out["$graph"]
    assert len(graphs) == 6

    # check graph ids
    graph_ids = [graph["id"] for graph in graphs]
    assert "analyse" in graph_ids
    assert "stageout_data_analysis" in graph_ids
    assert "split_tiles" in graph_ids
    assert "process" in graph_ids
    assert "merge_results" in graph_ids

    # check analyse graph
    analyse_graph = [graph for graph in graphs if graph["id"] == "analyse"][0]
    analyse_graph_class = analyse_graph["class"]
    assert analyse_graph_class == "CommandLineTool"
    analyse_graph_id = analyse_graph["id"]
    assert analyse_graph_id == "analyse"
    analyse_graph_base_command = analyse_graph["baseCommand"]
    assert analyse_graph_base_command == "python"
    analyse_graph_arguments = analyse_graph["arguments"]
    assert analyse_graph_arguments == [
        "/app/data_availability.py",
        "--spatial_extent",
        "$(inputs.spatial_extent[0])",
        "$(inputs.spatial_extent[1])",
        "$(inputs.spatial_extent[2])",
        "$(inputs.spatial_extent[3])",
    ]
    analyse_graph_inputs = analyse_graph["inputs"]
    assert analyse_graph_inputs == {"spatial_extent": {"type": "string[]"}}
    analyse_graph_outputs = analyse_graph["outputs"]
    assert analyse_graph_outputs == {"data_analysis_results": {"type": "Directory", "outputBinding": {"glob": "."}}}

    # check stageout_data_analysis graph
    stageout_data_analysis_graph = [graph for graph in graphs if graph["id"] == "stageout_data_analysis"][0]
    stageout_data_analysis_graph_class = stageout_data_analysis_graph["class"]
    assert stageout_data_analysis_graph_class == "CommandLineTool"
    stageout_data_analysis_graph_id = stageout_data_analysis_graph["id"]
    assert stageout_data_analysis_graph_id == "stageout_data_analysis"
    stageout_data_analysis_graph_base_command = stageout_data_analysis_graph["baseCommand"]
    assert stageout_data_analysis_graph_base_command == "python"
    stageout_data_analysis_graph_arguments = stageout_data_analysis_graph["arguments"]
    assert stageout_data_analysis_graph_arguments == [
        "/app/stageout_data_analysis.py",
        "--data_analysis_results",
        "$(inputs.data_analysis_results)",
    ]
    stageout_data_analysis_graph_inputs = stageout_data_analysis_graph["inputs"]
    assert stageout_data_analysis_graph_inputs == {"data_analysis_results": {"type": "Directory"}}
    stageout_data_analysis_graph_outputs = stageout_data_analysis_graph["outputs"]
    assert stageout_data_analysis_graph_outputs == {"stageout_data_analysis_results": {"type": "Directory", "outputBinding": {"glob": "."}}}

    # check split_tiles graph
    split_tiles_graph = [graph for graph in graphs if graph["id"] == "split_tiles"][0]
    split_tiles_graph_class = split_tiles_graph["class"]
    assert split_tiles_graph_class == "CommandLineTool"
    split_tiles_graph_id = split_tiles_graph["id"]
    assert split_tiles_graph_id == "split_tiles"
    split_tiles_graph_base_command = split_tiles_graph["baseCommand"]
    assert split_tiles_graph_base_command == "python"
    split_tiles_graph_arguments = split_tiles_graph["arguments"]
    assert split_tiles_graph_arguments == [
        "/app/split_tiles.py",
        "--spatial_extent",
        "$(inputs.spatial_extent[0])",
        "$(inputs.spatial_extent[1])",
        "$(inputs.spatial_extent[2])",
        "$(inputs.spatial_extent[3])",
        "--data_analysis_results",
        "$(inputs.data_analysis_results)",
    ]
    split_tiles_graph_inputs = split_tiles_graph["inputs"]
    assert split_tiles_graph_inputs == {
        "spatial_extent": {"type": {"type": "string[]"}},
        "data_analysis_results": {"type": "Directory"},
        "stageout_data_analysis_results": {"type": "Directory"},
    }
    split_tiles_graph_outputs = split_tiles_graph["outputs"]
    assert split_tiles_graph_outputs == {
        "split_tiles_results": {"type": "Directory", "outputBinding": {"glob": "."}},
        "tiles": {"type": {"type": "array", "items": {"fields": [{"name": "spatial_extent", "type": "string[]"}], "type": "record", "name": "TileRecord"}}, "outputBinding": {"glob": "tiles/tiles.json", "loadContents": True, "outputEval": "${ return JSON.parse(self[0].contents); }"}},
    }

    # check process graph
    process_graph = [graph for graph in graphs if graph["id"] == "process"][0]
    process_graph_class = process_graph["class"]
    assert process_graph_class == "CommandLineTool"
    process_graph_id = process_graph["id"]
    assert process_graph_id == "process"
    process_graph_base_command = process_graph["baseCommand"]
    assert process_graph_base_command == "python"
    process_graph_arguments = process_graph["arguments"]
    assert process_graph_arguments == [
        "/app/run.py",
        "--spatial_extent",
        "$(inputs.spatial_extent[0])",
        "$(inputs.spatial_extent[1])",
        "$(inputs.spatial_extent[2])",
        "$(inputs.spatial_extent[3])",
    ]
    process_graph_inputs = process_graph["inputs"]
    assert process_graph_inputs == {"spatial_extent": {"type": "string[]"}}
    process_graph_outputs = process_graph["outputs"]
    assert process_graph_outputs == {"process_results": {"type": "Directory", "outputBinding": {"glob": "."}}}

    # check merge_results graph
    merge_results_graph = [graph for graph in graphs if graph["id"] == "merge_results"][0]
    merge_results_graph_class = merge_results_graph["class"]
    assert merge_results_graph_class == "CommandLineTool"
    merge_results_graph_id = merge_results_graph["id"]
    assert merge_results_graph_id == "merge_results"
    merge_results_graph_base_command = merge_results_graph["baseCommand"]
    assert merge_results_graph_base_command == "python"
    merge_results_graph_arguments = merge_results_graph["arguments"]
    assert merge_results_graph_arguments == [
        "/app/merge_results.py",
        "--scatter_execution_results",
        "$(inputs.process_results)",
    ]
    merge_results_graph_inputs = merge_results_graph["inputs"]
    assert merge_results_graph_inputs == {"process_results": {"type": "Directory[]"}}
    merge_results_graph_outputs = merge_results_graph["outputs"]
    assert merge_results_graph_outputs == {"execution_results": {"type": "Directory", "outputBinding": {"glob": "."}}}
