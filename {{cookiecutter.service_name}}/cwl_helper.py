def update_workflow_graph(workflow_graph):
    workflow_graph["steps"]["stageout_data_analysis"] = {
            "run": "#stageout_data_analysis",
            "in": {"data_analysis_results": "analyse/data_analysis_results"},
            "out": ["stageout_data_analysis_results"],
        }
    workflow_graph["steps"]["process"]["in"]["stageout_data_analysis_results"] = "stageout_data_analysis/stageout_data_analysis_results"
    return workflow_graph

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
                "dockerPull": "brunifrancesco/zoo_reference_implementation:v5",
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

def finalize_cwl(cwl):
    graphs = cwl["$graph"]
    for graph in graphs:
        if graph["class"] == "Workflow":
            updated_workflow_graph = update_workflow_graph(graph)
            graphs.remove(graph)
            graphs.append(updated_workflow_graph)
    graphs.append(add_stageout_data_analysis_graph())
    return cwl