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

    analyse_graph = [graph for graph in graphs if graph["id"] == "analyse"][0]
    assert analyse_graph == {
        "class": "CommandLineTool",
        "id": "analyse",
        "requirements": {"ResourceRequirement": {"coresMax": 1, "ramMax": 512}},
        "hints": {"DockerRequirement": {"dockerPull": "pminel/zoo_reference_implementation_v4"}},
        "baseCommand": "python",
        "arguments": [
            "/app/data_availability.py",
            "--spatial_extent",
            "$(inputs.spatial_extent[0])",
            "$(inputs.spatial_extent[1])",
            "$(inputs.spatial_extent[2])",
            "$(inputs.spatial_extent[3])",
        ],
        "inputs": {"spatial_extent": {"type": "string[]"}},
        "outputs": {"data_analysis_results": {"type": "Directory", "outputBinding": {"glob": "."}}},
    }

    stageout_data_analysis_graph = [graph for graph in graphs if graph["id"] == "stageout_data_analysis"][0]
    assert stageout_data_analysis_graph == {
        "class": "CommandLineTool",
        "id": "stageout_data_analysis",
        "requirements": {"ResourceRequirement": {"coresMax": 1, "ramMax": 512}},
        "hints": {"DockerRequirement": {"dockerPull": "pminel/zoo_reference_implementation_v4"}},
        "baseCommand": "python",
        "arguments": [
            "/app/stageout_data_analysis.py",
            "--data_analysis_results",
            "$(inputs.data_analysis_results)",
        ],
        "inputs": {
            "data_analysis_results": {"type": "Directory"},
        },
        "outputs": {"stageout_data_analysis_results": {"type": "Directory", "outputBinding": {"glob": "."}}},
    }

    split_tiles_graph = [graph for graph in graphs if graph["id"] == "split_tiles"][0]
    assert split_tiles_graph == {
        "class": "CommandLineTool",
        "id": "split_tiles",
        "requirements": {"ResourceRequirement": {"coresMax": 1, "ramMax": 512}},
        "hints": {"DockerRequirement": {"dockerPull": "pminel/zoo_reference_implementation_v4"}},
        "baseCommand": "python",
        "arguments": [
            "/app/split_tiles.py",
            "--spatial_extent",
            "$(inputs.spatial_extent[0])",
            "$(inputs.spatial_extent[1])",
            "$(inputs.spatial_extent[2])",
            "$(inputs.spatial_extent[3])",
            "--data_analysis_results",
            "$(inputs.data_analysis_results)",
        ],
        "inputs": {
            "spatial_extent": {"type": {"type": "string[]"}},
            "data_analysis_results": {"type": "Directory"},
            "stageout_data_analysis_results": {"type": "Directory"},
        },
        "outputs": {
            "split_tiles_results": {"type": "Directory", "outputBinding": {"glob": "."}},
            "tiles": {
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "TileRecord",
                        "fields": [
                            {
                                "name": "spatial_extent",
                                "type": "string[]"
                            },
                        ]
                    }
                },
                "outputBinding": {
                    "glob": "tiles/tiles.json",
                    "loadContents": True,
                    "outputEval": "${ return JSON.parse(self[0].contents); }"
                }
            },
        },
    }

    process_graph = [graph for graph in graphs if graph["id"] == "process"][0]
    assert process_graph == {
        "class": "CommandLineTool",
        "id": "process",
        "requirements": {"ResourceRequirement": {"coresMax": 1, "ramMax": 512}},
        "hints": {"DockerRequirement": {"dockerPull": "pminel/zoo_reference_implementation_v4"}},
        "baseCommand": "python",
        "arguments": [
            "/app/process.py",
            "--spatial_extent",
            "$(inputs.spatial_extent[0])",
            "$(inputs.spatial_extent[1])",
            "$(inputs.spatial_extent[2])",
            "$(inputs.spatial_extent[3])",

    # stageout_data_analysis_graph = [graph for graph in graphs if graph["id"] == "stageout_data_analysis"][0]
    # assert stageout_data_analysis_graph["in"] == {"data_analysis_results": "analyse/data_analysis_results"}
    # assert stageout_data_analysis_graph["out"] == ["stageout_data_analysis_results"]
    #
    # split_tiles_graph = [graph for graph in graphs if graph["id"] == "split_tiles"][0]
    # assert split_tiles_graph["in"] == {
    #     "spatial_extent": "spatial_extent",
    #     "data_analysis_results": "analyse/data_analysis_results",
    #     "stageout_data_analysis_results": "stageout_data_analysis/stageout_data_analysis_results",
    # }
    # assert split_tiles_graph["out"] == ["split_tiles_results", "tiles"]
    #
    # process_graph = [graph for graph in graphs if graph["id"] == "process"][0]
    # assert process_graph["in"] == {
    #     "spatial_extent": "spatial_extent",
    #     "data_analysis_results": "analyse/data_analysis_results",
    # }