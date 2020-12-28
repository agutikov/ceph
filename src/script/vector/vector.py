#!/usr/bin/env python3


import rados
from pprint import pprint
import sys
import numpy as np
import struct
import array
from datetime import datetime

np.random.seed(42)

obj_count = int(sys.argv[2])

request_count = int(sys.argv[3])
recrods_to_find = int(sys.argv[4])

record_size = int(sys.argv[5])
vector_offset = int(sys.argv[6])
vector_el_type = sys.argv[7]
vector_length = int(sys.argv[8])

buffer_size = int(sys.argv[9])

vector_fmt = '%sx%d' % (vector_el_type, vector_length)

def type_pack(fmt):
    return {
        'u8' : 'B',
        'u16' : 'H',
        'u32' : 'I',
        'u64' : 'Q',
        'i8' : 'b',
        'i16' : 'h',
        'i32' : 'i',
        'i64' : 'q',
        'f32' : 'f',
        'f64' : 'd',
        }[fmt]

def type_code(fmt):
    return {
        'i8' : 1,
        'i16' : 2,
        'i32' : 3,
        'i64' : 4,
        'u8' : 5,
        'u16' : 6,
        'u32' : 7,
        'u64' : 8,
        'f32' : 9,
        'f64' : 10,
        }[fmt]

def code_pack(code):
    return {
        1 : 'B',
        2 : 'H',
        3 : 'I',
        4 : 'Q',
        5 : 'b',
        6 : 'h',
        7 : 'i',
        8 : 'q',
        9 : 'f',
        10 : 'd',
        }[code]

def code_size(code):
    return {
        1 : 1,
        2 : 2,
        3 : 4,
        4 : 8,
        5 : 1,
        6 : 2,
        7 : 4,
        8 : 8,
        9 : 4,
        10 : 8,
        }[code]

def code_type(code):
    return {
        1 : 'i8',
        2 : 'i16',
        3 : 'i32',
        4 : 'i64',
        5 : 'u8',
        6 : 'u16',
        7 : 'u32',
        8 : 'u64',
        9 : 'f32',
        10 : 'f64',
        }[code]

def rand_vector(el_type, length):
    v = np.random.random_sample(length)
    if el_type[0] != 'f':
        p = 2 ** (int(el_type[1:]) - 1)
        v = [int(x*p) for x in v]
    pprint(v)
    return struct.pack('%d%s' % (len(v), type_pack(el_type)), *v)

def rand_record(size, offset, el_type, length):
    v = rand_vector(el_type, length)
    b = bytearray(offset) + v + bytearray(size-len(v)-offset)
    pprint(b)
    return b

def rand_request(num_to_find, size, offset, el_type, length):
    bin_fmt = struct.pack('IIB3xI', size, offset, type_code(el_type), length)
    b = struct.pack('I', num_to_find) + bin_fmt + rand_record(size, offset, el_type, length)
    pprint(b)
    return b

def rand_multi_request(num_req, num_to_find, size, offset, el_type, length):
    b = struct.pack('I', num_req)
    for i in range(num_req):
        b += rand_request(num_to_find, size, offset, el_type, length)
    pprint(b)
    print(len(b))
    return b


obj_ids = [(int(i/30), 2018, 5, 1 + (i % 30)) for i in range(obj_count)]
obj_names = list(map(lambda obj_id: 'test_%04d__%04d_%02d_%02d' % obj_id, obj_ids))

def fmt_to_str(fmt):
    return '%d+%d:%sx%d' % (fmt[0], fmt[1], code_type(fmt[2]), fmt[3])

def unpack_fmt(b, offset):
    size, vector_offset, el_fmt, vector_length = struct.unpack_from('IIB3xI', b, offset)
    return (size, vector_offset, el_fmt, vector_length), offset + 16

def unpack_record(b, offset, fmt):
    #print('unpack_record')
    #pprint(fmt)
    off1 = offset + fmt[1]
    off2 = off1 + code_size(fmt[2])*fmt[3]
    off3 = offset + fmt[0]
    #print(len(b), offset, off1, off2, off3)
    before = b[offset : off1]
    v = b[off1 : off2]
    after = b[off2 : off3]
    offset = off3
    v = array.array(code_pack(fmt[2]), v)
    return (before, v, after), offset

def unpack_result(b, offset, fmt):
    #print('unpack_result')
    #print(len(b), offset)
    #pprint(fmt)
    distance, = struct.unpack_from('d', b, offset)
    offset += 8
    rec, offset = unpack_record(b, offset, fmt)
    return (distance, rec), offset

def unpack_response(b, offset, response_fmt):
    #print('unpack_response')
    #print(len(b), offset)
    #pprint(response_fmt)
    request_fmt, offset = unpack_fmt(b, offset)
    #pprint(request_fmt)
    request_rec, offset = unpack_record(b, offset, request_fmt)
    #print(len(b), offset)
    num_results, = struct.unpack_from('I', b, offset)
    offset += 4
    results = []
    for i in range(num_results):
        result, offset = unpack_result(b, offset, response_fmt)
        results.append(result)
    return ((fmt_to_str(request_fmt), request_rec), num_results, results), offset

def unpack_multi_response(b):
    #print('unpack_multi_response')
    #print(len(b))
    offset = 0
    response_fmt, offset = unpack_fmt(b, offset)
    #print(len(b), offset)
    #pprint(response_fmt)
    num_req, = struct.unpack_from('I', b, offset)
    offset += 4
    responses = []
    for i in range(num_req):
        response, offset = unpack_response(b, offset, response_fmt)
        responses.append(response)
    return (fmt_to_str(response_fmt), num_req, responses)


################################################################################

cluster = rados.Rados(conffile=sys.argv[1])

cluster.connect()

mypool = cluster.open_ioctx("p1")

obj_results = {}

class find_closest_oncomplete:
    def __init__(self, results, obj_name):
        self.results = results
        self.obj_name = obj_name

    def __call__(self, completion_obj, buffer):
        retval = completion_obj.get_return_value()
        #print('completed', self.obj_name, retval)
        if retval > 0:
            if len(buffer) > 0:
                #s = buffer.hex()
                #pprint([s[i:i+8] for i in range(0, len(s), 8)])
                self.results[self.obj_name][1] = unpack_multi_response(buffer)
            else:
                self.results[self.obj_name][1] = ()
        else:
            self.results[self.obj_name][1] = {'error':retval}

v = rand_multi_request(request_count, recrods_to_find, record_size, vector_offset, vector_el_type, vector_length)

start = datetime.now()

for obj_name in obj_names:
    obj_results[obj_name] = [None, None] # completion, result
    obj_results[obj_name][0] = mypool.aio_execute(obj_name, 'vector', 'find_closest', v, length=buffer_size,
                                                  oncomplete=find_closest_oncomplete(obj_results, obj_name))
    #print('scheduled', obj_name)

print('scheduled')

for obj_name, s in obj_results.items():
    #print('wait for', obj_name)
    s[0].wait_for_complete_and_cb()

print('completed')

end = datetime.now()

#pprint(obj_results)

pprint(end - start)
