# Keboola Tentacle

Versioning tool for GoodData projects

## Usage

Run from command line:


```
#!bash

bin/tentacle GD_PID GD_USERNAME GD_PASSWORD S3_ACCESS_KEY S3_SECRET_KEY S3_FILE_PATH
```


Command will download metadata from given GoodData project and save them to separate json files. It will compress 
all of them to gzip file and send it to given destination in Amazon S3.

File structure will be as following:


```
#!ruby

tentacle.tgz
- ldm.json
- dashboards
  - 401
    - detail.json
    - used_by.json
    - using.json
- datasets
  - 16
    - attributes
      - 440
        - detail.json
        - used_by.json
        - using.json
    - facts
      - 445
        - detail.json
        - used_by.json
        - using.json
    - uploads
      - 1.json
      - 2.json
  - 242
- metrics
  - 409
    - detail.json
    - used_by.json
    - using.json
- reports
  - 411
    - definitions
      - 410
        - detail.json
        - used_by.json
        - using.json
    - detail.json
    - used_by.json
    - using.json
- users
  - 0c18c5540bd6bde1a6c8e123794e5353.json
```
