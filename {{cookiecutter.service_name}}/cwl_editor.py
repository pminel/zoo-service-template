def update_workflow_graph(workflow_graph):
    workflow_graph["outputs"]["execution_results"] = {
        "type": "Directory",
        "outputSource": ["merge_results/execution_results"]
    }

    workflow_graph["steps"] = {
        "analyse": {
            "run": "#analyse",
            "in": {"spatial_extent": "spatial_extent"},
            "out": ["data_analysis_results"],
        },
        "stageout_data_analysis": {
            "run": "#stageout_data_analysis",
            "in": {"data_analysis_results": "analyse/data_analysis_results"},
            "out": ["stageout_data_analysis_results"],
        },
        "split_tiles": {
            "run": "#split_tiles",
            "in": {
                "spatial_extent": "spatial_extent",
                "data_analysis_results": "analyse/data_analysis_results",
                "stageout_data_analysis_results": "stageout_data_analysis/stageout_data_analysis_results",
            },
            "out": ["split_tiles_results", "tiles"],
        },
        "process": {
            "run": "#process",
            "in": {
                "spatial_extent": {
                    "source": "split_tiles/tiles",
                    "valueFrom": "$(self.spatial_extent)",
                }
            },
            "out": ["process_results"],
            "scatter": "spatial_extent",
            "scatterMethod": "flat_crossproduct",
        },
        "merge_results": {
            "run": "#merge_results",
            "in": {
                "process_results": "process/process_results"
            },
            "out": ["execution_results"],
        },
    }
    return workflow_graph


def update_process_graph(process_graph):
    process_graph["inputs"] = {
        "spatial_extent": {
            "type": "string[]"
        }
    }
    return process_graph


def add_stageout_data_analysis_graph():
    return {
        "class": "CommandLineTool",
        "id": "stageout_data_analysis",
        "baseCommand": "python",
        "arguments": [
            "/app/stageout_data_analysis.py",
            "--data_analysis_results",
            "$(inputs.data_analysis_results)",
        ],
        "requirements": {
            "ResourceRequirement": {
                "coresMax": 1,
                "ramMax": 512,
            }
        },
        "hints": {
            "DockerRequirement": {
                "dockerPull": "pminel/zoo_reference_implementation_v4",
            }
        },
        "inputs": {
            "data_analysis_results": {
                "type": "Directory"
            }
        },
        "outputs": {
            "stageout_data_analysis_results": {
                "type": "Directory",
                "outputBinding": {
                    "glob": "."
                }
            }
        }
    }


def add_split_tiles_graph():
    return {
        "class": "CommandLineTool",
        "id": "split_tiles",
        "baseCommand": "python",
        "arguments": [
            "/app/split_tiles.py",
            "--spatial_extent",
            "$(inputs.spatial_extent[0])",
            "$(inputs.spatial_extent[1])",
            "$(inputs.spatial_extent[2])",
            "$(inputs.spatial_extent[3])",
            "-- data_analysis_results",
            "$(inputs.data_analysis_results)",
        ],
        "requirements": {
            "ResourceRequirement": {
                "coresMax": 1,
                "ramMax": 512,
            }
        },
        "hints": {
            "DockerRequirement": {
                "dockerPull": "pminel/zoo_reference_implementation_v4",
            }
        },
        "inputs": {
            "spatial_extent": {
                "type": {
                    "type": "string[]",
                }
            },
            "data_analysis_results": {
                "type": "Directory"
            },
            "stageout_data_analysis_results": {
                "type": "Directory"
            }
        },
        "outputs": {
            "split_tiles_results": {
                "type": "Directory",
                "outputBinding": {
                    "glob": "."
                }
            },
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
            }
        }
    }


def add_merge_results_graph():
    return {
        "class": "CommandLineTool",
        "id": "merge_results",
        "baseCommand": "python",
        "arguments": [
            "/app/merge_results.py",
            "--scatter_execution_results",
            "$(inputs.process_results)",
        ],
        "requirements": {
            "ResourceRequirement": {
                "coresMax": 1,
                "ramMax": 1024,
            }
        },
        "hints": {
            "DockerRequirement": {
                "dockerPull": "pminel/zoo_reference_implementation_v4",
            }
        },
        "inputs": {
            "process_results": {
                "type": "Directory[]"
            }
        },
        "outputs": {
            "execution_results": {
                "type": "Directory",
                "outputBinding": {
                    "glob": "."
                }
            }
        }
    }


def finalize_cwl(cwl):
    graphs = cwl["$graph"]
    for graph in graphs:
        if graph["class"] == "Workflow":
            print("Updating workflow graph")
            updated_workflow_graph = update_workflow_graph(graph)
            graphs.remove(graph)
            graphs.append(updated_workflow_graph)
        elif graph["class"] == "CommandLineTool" and graph["id"] == "process":
            print("Updating process graph")
            updated_process_graph = update_process_graph(graph)
            graphs.remove(graph)
            graphs.append(updated_process_graph)

    print("Adding stageout_data_analysis graph")
    graphs.append(add_stageout_data_analysis_graph())

    print("Adding split_tiles graph")
    graphs.append(add_split_tiles_graph())

    print("Adding merge_results graph")
    graphs.append(add_merge_results_graph())

    return cwl