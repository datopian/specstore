# DataHQ Spec Store

[![Build Status](https://travis-ci.org/datahq/specstore.svg?branch=master)](https://travis-ci.org/datahq/specstore)

An API server for managing a Source Spec Registry 

## Quick start

### Clone the repo and install

`make install`

### Run tests

`make test`

### Run server

`python server.py`

## Env Vars
- `DATABASE_URL`: A SQLAlchemy compatible database connection string (where registry is stored) 
- `AUTH_SERVER`: The domain name for the authentication server 
- `DPP_URL`: URL for the datapackage pipelines service (e.g. `http://host:post/`) 

## API

### SourceSpec

```yaml
meta:
  owner: <owner username>
  ownerid: <owner unique id>
  dataset: <dataset name>
  version: 1
  findability: <published/unlisted/private>
inputs:
 -  # only one input is supported atm
    kind: datapackage
    url: <datapackage-url>
    parameters:
      resource-mapping:
        <resource-name-or-path>: <resource-url>
      resource_info: # Excerpt from all resources in the datapackage
        - name: <resource 1 name>
        - path: <resource 1 path>
        - schema: <resource 1 schema, if exists>
        - format: <resource 1 format, if exists>
        - ... # other properties, if available, are optional
        
processing:
 - # Processing steps that need to be done on sources to get proper data streams
   input: <source-resource-name - e.g. `my-excel-resource`>
   output: <destination-resource-name - e.g. `my-excel-resource-sheet-1`>
   tabulator: # Currently we're only supporting tabulator transformations
     # These are sample options for tabulator, see its docs for all available options
     headers: 2 
     sheet: 1

outputs:
  -
    kind: rdbms  # dump.to_sql
    parameters:
      engine: <tbd, should be the name of a user provided configuration - not the actual connection string>
  -
    kind: sqlite # dump.to_sql
  -
    kind: npm  
    parameters:
      credentials: <tbd, should be the name of a user provided configuration - not the actual credentials>
  -
    kind: zip (dump.to_zip)
    parameters: 
        out-file: <name of the file>
  - ... # other output formats

```

### Status

`/source/{identifier}/status`

#### Method

`GET`

#### Response

```javascript=
{
   'state': 'LOADED/REGISTERED/INVALID/RUNNING/SUCCEEDED/FAILED',
   'logs': [
              'log-line', 
              'log-line', // ...
           ],
   'modified': 'specstore-timestamp-of-pipeline-data
}
```

### Status

`/source/{identifier}/info`

#### Method

`GET`

#### Response

```javascript=
{
  "id": "./<pipeline-id>",

  "pipeline": <pipeline>,
  "source": <source>,

  "message": <short-message>,
  "error_log": [ <error-log-lines> ],
  "reason": <full-log>,

  "state": "LOADED/REGISTERED/INVALID/RUNNING/SUCCEEDED/FAILED",
  "success": <last-run-succeeded?>,
  "trigger": <dirty-task/scheduled>,

  "stats": {
      "bytes": <number>,
      "count_of_rows": <number>,
      "dataset_name": <string>,
      "hash": <datapackage-hash>
  },

  "cache_hash": "c69ee347c6019eeca4dbf66141001c55",
  "dirty": false,

  "queued": <numeric-timestamp>,
  "started": <numeric-timestamp>,
  "updated": <numeric-timestamp>,
  "last_success": <numeric-timestamp>,
  "ended": <numeric-timestamp>
}
```

state definition:

- `LOADED`: In the specstore, pipeline not created yet
- `REGISTERED`: Waiting to run
- `INVALID`: Problem with the source spec or the pipeline
- `RUNNING`: Currently running
- `SUCCEEDED`: Finished successfully
- `FAILED`: Failed to run

### Upload 

`/source/upload`

#### Method

`POST`

#### Headers

* `Auth-Token` - permission token (received from conductor)
* Content-type - application/json

#### Body

A valid spec in JSON form.

#### Response

```javascript=
{
  "success": true,
  "id": "<identifier>"
  "errors": [
      "<error-message>"
  ]
}
```
