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

### Status

`/source/{owner}/{dataset-id}/{revision-number}`

*Note: Also, you can get info about latest and latest successful revisions by hitting following endpoints*

* latest - `/source/{owner}/{dataset-id}/latest`
* successful - `/source/{owner}/{dataset-id}/successful`

#### Method

`GET`

#### Response

```javascript=
{
  "id": "<revision-id>",
  "spec_contents": <source-specifications>,
  "modified": <last-modified>,
  "state": <QUEUED|INPROGRESS|SUCCEEDED|FAILED>,
  "logs": <full-logs>,
  "error_log": [ <error-log-lines> ],
  "stats": {
      "bytes": <number>,
      "count_of_rows": <number>,
      "dataset_name": <string>,
      "hash": <datapackage-hash>
  }
}
```

state definition:

- `QUEUED`: In the flowmanager, pipeline not created yet
- `INPROGRESS`: Waiting to run
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

A valid spec in JSON form. You can find example Flow-Spec in README of [planer API](https://github.com/datahq/planner/commit/d4dbc6bbd4d215ed1617969e3a502953b6b62910)

#### Response

```javascript=
{
  "success": true,
  "dataset_id": "<dataset-identifier>",
  "flow_id": "<dataset-identifier-with-revision-number>",
  "errors": [
      "<error-message>"
  ]
}
```

### Update

`/source/update`

#### Method

`POST`

#### Body

Payload in JSON form.

```javascript=
{
  "pipeline": "<pipeline-id>",
  "event": "queue/start/progress/finish",
  "success": true/false (when applicable),
  "errors": [list-of-errors, when applicable]
}
```

#### Response
```javascript=
{
  "success": success/pending/fail,
  "id": "<identifier>"
  "errors": [
      "<error-message>"
  ]
}
```
