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
    requirements:
      ResourceRequirement:
        coresMax: 1
        ramMax: 1024
      ScatterFeatureRequirement: {}
      StepInputExpressionRequirement: {}
    inputs:
      thematic_service_name:
        label: Thematic Service name
        type: string
        default: "thematic_service_name_default"
      spatial_extent:
        type: string[]
        label: Spatial extent bounding box [minLon, minLat, maxLon, maxLat]
    outputs:
      execution_results:
        type: Directory
        outputSource: [merge_results/execution_results]
    steps:
      analyse:
        run: "#analyse"
        in:
          spatial_extent: spatial_extent
        out: [data_analysis_results]
      stageout_data_analysis:
        run: "#stageout_data_analysis"
        in:
          data_analysis_results: analyse/data_analysis_results
        out: [stageout_data_analysis_results]
      split_tiles:
        run: "#split_tiles"
        in:
          spatial_extent: spatial_extent
          data_analysis_results: analyse/data_analysis_results
          stageout_data_analysis_results: stageout_data_analysis/stageout_data_analysis_results
        out: [split_tiles_results, tiles]
      process:
        run: "#process"
        in:
          spatial_extent:
            source: split_tiles/tiles
            valueFrom: $(self.spatial_extent)
        out: [process_results]
        scatter: spatial_extent
        scatterMethod: flat_crossproduct
      merge_results:
        run: "#merge_results"
        in:
          process_results: process/process_results
        out: [execution_results]


  # written by user #
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
        type:
          type: array
          items: string
    outputs:
      data_analysis_results:
        type: Directory
        outputBinding:
          glob: .


  # injected by service-template #
  - class: CommandLineTool
    id: stageout_data_analysis
    baseCommand: python
    arguments:
      - /app/stageout_data_analysis.py
      - --data_analysis_results
      - $(inputs.data_analysis_results)
    requirements:
      ResourceRequirement:
        coresMax: 1
        ramMax: 512
    hints:
      DockerRequirement:
        dockerPull: pminel/zoo_reference_implementation_v4
    inputs:
      data_analysis_results:
        type: Directory
    outputs:
      stageout_data_analysis_results:
        type: Directory
        outputBinding:
          glob: .


  # injected by service-template #
  - class: CommandLineTool
    id: split_tiles
    baseCommand: python
    arguments:
      - /app/split_tiles.py
      - --spatial_extent
      - $(inputs.spatial_extent[0])
      - $(inputs.spatial_extent[1])
      - $(inputs.spatial_extent[2])
      - $(inputs.spatial_extent[3])
      - --data_analysis_results
      - $(inputs.data_analysis_results)
    requirements:
      ResourceRequirement:
        coresMax: 1
        ramMax: 512
    hints:
      DockerRequirement:
        dockerPull: pminel/zoo_reference_implementation_v4
    inputs:
      spatial_extent:
        type:
          type: array
          items: string
      data_analysis_results:
        type: Directory
      stageout_data_analysis_results:
        type: Directory
    outputs:
      split_tiles_results:
        type: Directory
        outputBinding:
          glob: .
      tiles:
        type:
          type: array
          items:
            type: record
            name: TileRecord
            fields:
              - name: spatial_extent
                type: string[]
        outputBinding:
          glob: tiles/tiles.json
          loadContents: true
          outputEval: |
            ${ return JSON.parse(self[0].contents); }








  # written by user #
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
    outputs:
      process_results:
        type: Directory
        outputBinding:
          glob: .


  # injected by service-template #
  - class: CommandLineTool
    id: merge_results
    baseCommand: python
    arguments:
      - /app/merge_results.py
      - --scatter_execution_results
      - $(inputs.process_results)
    requirements:
      ResourceRequirement:
        coresMax: 1
        ramMax: 1024
    hints:
      DockerRequirement:
        dockerPull: pminel/zoo_reference_implementation_v4
    inputs:
      process_results:
        type: Directory[]
    outputs:
      execution_results:
        type: Directory
        outputBinding:
          glob: .

