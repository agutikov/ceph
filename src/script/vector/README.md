
```

# record format: size=5000 bytes, vector_offset=64 bytes, vector_length=512 of 64-bit floats

# create 10 objects, each 100 records, next 4 args - record format
./prepare.py ./ceph.conf 10 100 5000 64 f64 512

# send request to 10 objecst, each 5 requests, each search for 3 closest vectors, next 4 args - record format
./vector.py ./ceph.conf 10 5 3 5000 64 f64 512


```
