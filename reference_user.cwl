cwlVersion: v1.2
$namespaces:
  s: https://schema.org/
s:softwareVersion: 0.1.2
schemas:
  - http://schema.org/version/9.0/schemaorg-current-http.rdf
$graph:

  - class: Workflow
    id: convert-url
    label: convert url app minel
    doc: Convert URL
    inputs:
      spatial_extent:
        type: string[]
        label: Spatial extent bounding box [minLon, minLat, maxLon, maxLat]
    outputs:
      execution_results:
        type: Directory
        outputSource: [process/process_results]
    steps:
      analyse:
        run: "#analyse"
        in:
          spatial_extent: spatial_extent
        out: [data_analysis_results]
      process:
        run: "#process"
        in:
          spatial_extent: spatial_extent
          data_analysis_results: analyse/data_analysis_results
        out: [process_results]

  - class: CommandLineTool
    id: analyse
    requirements:
      ResourceRequirement:
        coresMax: 1
        ramMax: 512
    hints:
      DockerRequirement:
        dockerPull: pminel/zoo_reference_implementation_v4
    baseCommand: python
    arguments:
      - /app/data_availability.py
      - --spatial_extent
      - $(inputs.spatial_extent[0])
      - $(inputs.spatial_extent[1])
      - $(inputs.spatial_extent[2])
      - $(inputs.spatial_extent[3])
    inputs:
      spatial_extent:
        type: string[]
    outputs:
      data_analysis_results:
        type: Directory
        outputBinding:
          glob: .

  - class: CommandLineTool
    id: process
    baseCommand: python
    arguments:
      - /app/run.py
      - --spatial_extent
      - $(inputs.spatial_extent[0])
      - $(inputs.spatial_extent[1])
      - $(inputs.spatial_extent[2])
      - $(inputs.spatial_extent[3])
    requirements:
      ResourceRequirement:
        coresMax: 1
        ramMax: 1024
    hints:
      DockerRequirement:
        dockerPull: pminel/zoo_reference_implementation_v4
    inputs:
      spatial_extent:
        type: string[]
      data_analysis_results:
        type: Directory
    outputs:
      process_results:
        type: Directory
        outputBinding:
          glob: .