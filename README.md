# Filesystem backed hash table

A filesystem backed hashtable. Keys and values are both integers (4 byte integer).
Upper bound of a billion keys. Recovers state if/when the program is
restarted and the hashtable is accessed. Avoids against state corruption
and is able to always recover fine. No data added to the table should be lost,
except for any data for which the insert (or remove) function hasn't returned (i.e. program
crashes or exits while insert operation is happening). Hashtable works with limited memory Hashtable is thread safe.


# Notes

I implemented this in Python first, then in CPP. I'm more proficient at Python and find it easier to prototype quickly with.


### Python

The Python code sits in `python` directory. Implemented with built-ins and primitives.
- `python/hash_table.py`
  - The logic of the hash table
- `python/test_hash_table.py`
  - Script for testing: `python python/test_hash_table.py`

### CPP

The CPP code sits in the `cpp` directory. Apologies ahead of time for clunky syntax and
lack of CPP best-practices. I implemented the CPP with JSON, so there is a minor linking one must do to the `nlohmann/json` lib.

On MacOS this is easily done with `brew install nlohmann-json`. To locate the lib path, `brew --prefix nlohmann-json`.

##### Compile
Replace `/path/to/nlohmann-json` with the corresponding path to the lib on your local machine.
```
g++ -I/path/to/nlohmann-json/include -std=c++17 -o test_hash_table test_hash_table.cpp hash_table.cpp -lpthread
```

- `cpp/hash_table.h`
  - Header file
- `cpp/hash_table.cpp`
  - Logic
- `cpp/test_hash_table.cpp`
  - Testing script: `./test_hash_table` (after compilation)


# Implementation Thoughts/Details
The hash table is implemented by hashing keys into file buckets. The filesystem is identified by a "root" directory or "table" directory. Each bucket is managed as an individual JSON file.

### Hashing
In Python, the hashing uses SHA256 in an attempt to achieve some semblence of uniformity in distribution. A fixed-number of characters (`bucket_mag`) of the hexademical encoding is used to distinguish and identify the bucket.

In CPP, the hashing is far simpler as it uses a simple modulo math computation.

### Bucketing Magnitude
Because I use hexadecimal hashing, the magnitude of bucketing scales by orders of 16. The default setting is 4096 buckets, i.e. `bucket_mag==3`. Increasing the bucketing magnitude can help reduce the amount of RAM needed at runtime as each bucket scales down in size.

### Read and Write
Each bucket is a JSON file and is loaded for reading as-needed, and overwritten when new key/values are added.

### Logging
When logging is flagged as True, every key/value insertion will be logged in the root directory in a 'table.txt' file. This logging's purpose is for preservation of state. Independent of the file system, the logging can be "replayed" to reconstruct the state's snapshot anew. If it's not logged, it didn't happen. It is not optimized, it could be improved with de-duping.

It should be noted that this reloading via log file is implemented as a Python class method, whereas in CPP, it is an overloaded constructor.

### Threading
Locking is implemented make this hash table threadsafe. Locking is needed at three levels: the log, the mapping for bucket-files, and then for each of the bucket-files.
