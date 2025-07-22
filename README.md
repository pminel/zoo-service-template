# zoo-service-template

ZOO Project template for deploying Application Packages

This template is used in https://github.com/eoap/ogc-api-processes-with-zoo/ and targets simplicity over functionality to show how ZOO Project OGC API Processes Part-2 DRU is customized. 



## Notable changes

- Updated docker image for stageout_data_analysis step from `pminel/zoo_reference_implementation_v4` to `brunifrancesco/zoo_reference_implementation:v5`
- Remove log outputs from service.py and cwl_helper.py
- Implemented get_secrest function in service.py