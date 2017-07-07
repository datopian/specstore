# DataHQ Spec Store

[![Build Status](https://travis-ci.org/datahq/specstore.svg?branch=master)](https://travis-ci.org/datahq/specstore)

An API server for managing a Source Spec Registry 

## Quick start

- Clone the repo, 
- install dependencies from `requirements.txt`, 
- and run the server (`server.py`)

## Env Vars
- `DATABASE_URL`: A SQLAlchemy compatible database connection string (where registry is stored) 
- `AUTH_SERVER`: The domain name for the authentication server 

## API

### Status

`/source/{identifier}/status`

#### Method

`GET`

#### Response

```javascript=
{
   'state': 'loaded/queued/running/errored',
   'errors': [
       'error-message', // ...
   ],
   'logs': [
              'log-line', 
              'log-line', // ...
           ],
   'history': [
      {
       'execution-time': 'iso-time',
       'success': true or false,
       'termination-time': 'iso-time' or null
      }, // ...   
   ],
   'outputs': [
       {
        'kind': '<kind>', 
        'url': '<url>', 
        'created-at': '<iso-time>',
        'filename': '<displayable-filename>',
        'title': '<displayable-title>'
       }
   ],
   'stats': {
       'key': 'value' // e.g. 'count-of-rows', etc.
   }
}
```

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
