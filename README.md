# pystreamer
![Build Status](https://travis-ci.com/Arnoldosmium/pystreamer.svg?branch=master)

A lazy evaluating, memory friendly, chainable stream solution.

Inspired by the syntactical sugar of Java stream.

```python
from streamer import Stream
with open("myfile.txt") as f_input:
    uniq = Stream(f_input) \
        .map(str.strip) \
        .flat_map(str.split) \
        .collect(set)   # uniq tokens in a file
```
