# Protobuf Test

Compares [Protocol Buffer](https://developers.google.com/protocol-buffers/)
performance with JSON, Gzipped JSON, and Gzipped CSV.

## Quick Start
```
$ python benchmark.py
```

## Instructions for Running Up-To-Date Benchmark
There are a few steps to make sure you're running your benchmark on the latest and greatest releases
of Python and Google's Protocol Buffers:

\1. Update Python to latest version. I prefer using homebrew:
```
$ brew install python
#OR
$ brew upgrade python
```
\2. Install / Upgrade Google Protocol Buffer Compiler
```
$ brew install protobuf
#OR
$ brew upgrade protobuf
```
\3. Delete Existing Address Book Python Class for Reading/Writing Protobuf
```
rm -f pb_schemas/addressbook_pb2.py
```
\4. Compile Proto Schema Into Python Class
```
SRC_DIR='pb_schemas' && DST_DIR='pb_schemas' && protoc -I=$SRC_DIR --python_out=$DST_DIR $SRC_DIR/addressbook.proto
```
\5. Run Benchmarks
```
$ python benchmark.py
```


## Benchmark Results
Here are my results, running Python 2.7.9, Protobuf 2.6.1 on a Early 2013 15" Macbook Pro Retina:

Results (indexed to JSON):

        | Write | Read | Size |
--------| ------|------|------|
json    | 1.00  | 1.00 | 1.00 |
proto   | 14.39 | 4.96 | 0.57 |
json.gz | 3.17  | 1.04 | 0.37 |
csv.gz  | 3.51  | 0.25 | 0.34 |

It appears that Protocol Buffers in Python are slower than JSON, but offer a file-size advantage.
Both Gzipped JSON and Gzipped CSV are faster and smaller than Protocol Buffers.


