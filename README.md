# SOA Service Communication Protocols

Compares performance between JSON, Gzipped JSON, [JSON-Schema](http://json-schema.org/), [Google Protocol Buffers](https://developers.google.com/protocol-buffers/), and [Apache Avro](http://avro.apache.org/).

## Quick Start
```
$ python benchmark.py
```

## Instructions for Running Up-To-Date Benchmark
There are a few steps to make sure you're running your benchmark on the latest and greatest releases of Python and Google's Protocol Buffers:

1. Update Python to latest version. I prefer using homebrew:
  ```
  $ brew install python
  #OR
  $ brew upgrade python
  ```
1. Install / Upgrade Google Protocol Buffer Compiler
  ```
  $ brew install protobuf
  #OR
  $ brew upgrade protobuf
  ```
1. Install dependencies
  ```
  pip install -r requirements.txt
  ```
1. Upgrade Benchmark Dependencies
  ```
  pip install --upgrade avro
  pip install --upgrade protobuf
  pip install --upgrade jsonschema
  ```
1. Delete Existing Address Book Python Class for Reading/Writing Protobuf
  ```
  rm -f pb_schemas/addressbook_pb2.py
  ```
1. Compile Proto Schema Into Python Class
  ```
  SRC_DIR='pb_schemas' && DST_DIR='pb_schemas' && protoc -I=$SRC_DIR --python_out=$DST_DIR $SRC_DIR/addressbook.proto
  ```
1. Run Benchmarks
  ```
  $ python benchmark.py
  ```


## Benchmark Results
Here are my results, running Python 2.7.9, jsonschema 2.4.0, Protobuf 2.6.1, and Avro 1.7.7 on an Early 2013 15" Macbook Pro Retina:

Notes:
  - JSON and Gzipped JSON do not do schema validation
  - JSON-Schema, Avro, and Protocol Buffers do schema validation
  

Results:

            | Write (s) | Read (s) | Size (bytes) |
------------| ----------|----------|--------------|
json        | 0.47      | 1.08     | 16,388,848   |
json.gz     | 1.88      | 1.52     | 5,887,318    |
json-schema | 0.47      | 13.18    | 16,388,848   |
avro        | 9.12      | 9.88     | 8,094,892    |
proto       | 7.87      | 7.46     | 9,183,403    |
    
    
    
Results (indexed to JSON):

            | Write | Read  | Size |
------------| ------|-------|------|
json        | 1.00  | 1.00  | 1.00 |
json.gz     | 4.02  | 1.40  | 0.36 |
json-schema | 1.00  | 12.16 | 1.00 |
avro        | 19.51 | 9.11  | 0.49 |
proto       | 16.84 | 6.88  | 0.56 |

