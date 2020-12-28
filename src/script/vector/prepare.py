#!/usr/bin/env python3


import rados
from pprint import pprint
import sys
import os
import numpy as np
import struct
from datetime import datetime

np.random.seed(42)

obj_count = int(sys.argv[2])
vectors_in_object = int(sys.argv[3])

record_size = int(sys.argv[4])
vector_offset = int(sys.argv[5])
vector_el_type = sys.argv[6]
vector_length = int(sys.argv[7])

fmt = '%d+%d:%sx%d' % (record_size, vector_offset, vector_el_type, vector_length)
vector_fmt = '%sx%d' % (vector_el_type, vector_length)

def pack_type(fmt):
    return {
        'f32' : 'f',
        'f64' : 'd',
        'u8' : 'B',
        'u16' : 'H',
        'u32' : 'I',
        'u64' : 'Q',
        'i8' : 'b',
        'i16' : 'h',
        'i32' : 'i',
        'i64' : 'q',
        }[fmt]

def rand_record(size, offset, el_type, length):
    v = np.random.uniform(low=0.0, high=1.0, size=length)
    if el_type[0] != 'f':
        p = 2 ** (int(el_type[1:]) - 1)
        v = [int(x*p) for x in v]
    #pprint(v)
    b = struct.pack('%d%s' % (len(v), pack_type(el_type)), *v)
    return bytearray(offset) + b + bytearray(size-len(b)-offset)

records = []

def rand_object(vec_count, get_rec):
    global records
    start = datetime.now()
    if len(records) == 0:
        for i in range(vec_count*2):
            r = get_rec()
            #pprint(r)
            records.append(bytes(r))
    np.random.shuffle(records)
    b = b''.join(records[:vec_count])
    end = datetime.now()
    print("rand_object", end - start)
    return b

get_rec = lambda : rand_record(record_size, vector_offset, vector_el_type, vector_length)

v = rand_object(vectors_in_object, get_rec)

obj_ids = [(int(i/30), 2018, 5, 1 + (i % 30)) for i in range(obj_count)]
np.random.shuffle(obj_ids)

completions = {}

class write_oncomplete:
    def __init__(self, obj_name):
        self.obj_name = obj_name

    def __call__(self, completion_obj):
        print('completed', self.obj_name)

obj_names = []

################################################################################

cluster = rados.Rados(conffile=sys.argv[1])

cluster.connect()

if 'p1' not in cluster.list_pools():
    print('required pool "p1"')
    quit(-1)

mypool = cluster.open_ioctx("p1")

for obj_id in obj_ids:
    #obj_name = 'test_%s_%04d' % (vector_fmt, i)
    obj_name = 'test_%04d__%04d_%02d_%02d' % obj_id
    try:
        mypool.remove_object(obj_name)
    except:
        pass
    mypool.write(obj_name, bytes.fromhex('00'))
    mypool.set_xattr(obj_name, 'format', fmt.encode('ascii'))
    print('created', obj_name)
    obj_names.append(obj_name)

for obj_name in obj_names:
    pprint(datetime.now())
    c = mypool.aio_write_full(obj_name, v, oncomplete=write_oncomplete(obj_name))
    pprint(datetime.now())
    completions[obj_name] = c
    print('scheduled', obj_name)
    v = rand_object(vectors_in_object, get_rec)

for obj_name, c in completions.items():
    print("wait", obj_name)
    c.wait_for_complete()
