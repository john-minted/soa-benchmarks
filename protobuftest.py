import os
import time
import gzip
import random
import json
import csv
from pb_schemas import addressbook_pb2

def person(p):
    """Generates a person in CSV, JSON and protobufs (which is passed as a parameter)
    Input parameter p is a protobuf that is mutated
    Returns j and c, which are equivalent json and csv representations of the same data
    """
    # Create dictionaries for json and csv
    j, c = {}, {}

    #Add Person's Information
    p.name = j['name'] = c['name'] = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for x in range(0, 20))
    p.id = j['id'] = c['id'] = random.randint(0, 100000)
    p.email = j['email'] = c['email'] = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz@.') for x in range(0, 30))

    j['phone'] = []
    for phone_count in range(0, 2):
        pphone = p.phone.add()
        jphone = {}
        j['phone'].append(jphone)
        pphone.number = jphone['number'] = c['%d-number' % phone_count] = ''.join(random.choice('0123456789') for x in range(0, 10))
        pphone.type = jphone['type'] = c['%d-type' % phone_count] = random.choice([0, 1, 2])

    return j, c

# Add 100,000 people to addressbook
print 'Adding 100,000 people to address book...'
addressbook, jsonseq, csvseq = addressbook_pb2.AddressBook(), [], []
for x in range(0, 100000):
    p = addressbook.person.add()
    j, c = person(p)
    jsonseq.append(j)
    csvseq.append(c)


# Write Benchmarks
print 'Running write benchmarks...'
t0 = time.time()
open('./output/output.json', 'w').write(json.dumps(jsonseq, separators=(',', ':')))
t1 = time.time()
open('./output/output.pb', 'wb').write(addressbook.SerializeToString())
t2 = time.time()
gzip.open('./output/output.jsz', 'wb').write(json.dumps(jsonseq, separators=(',', ':')))
t3 = time.time()
out = csv.DictWriter(gzip.open('./output/output.csvz', 'wb'), csvseq[0].keys())
out.writerows(csvseq)
t4 = time.time()

json_write = t1 - t0
proto_write = t2 - t1
gzjson_write = t3 - t2
gzcsv_write = t4 - t3

# Read Benchmarks
print 'Running read benchmarks...'
t0 = time.time()
json.loads(open('./output/output.json').read())
t1 = time.time()
addressbook_pb2.AddressBook().ParseFromString(open('./output/output.pb', 'rb').read())
t2 = time.time()
json.loads(gzip.open('./output/output.jsz', 'rb').read())
t3 = time.time()
list(csv.DictReader(gzip.open('./output/output.jsz', 'rb')))
t4 = time.time()

json_read = t1 - t0
proto_read = t2 - t1
gzjson_read = t3 - t2
gzcsv_read = t4 - t3


# File Size Benchmarks
print 'Running file size benchmarks...\n'
json_size   = float(os.stat('./output/output.json').st_size)
proto_size  = float(os.stat('./output/output.pb'  ).st_size)
gzjson_size = float(os.stat('./output/output.jsz' ).st_size)
gzcsv_size  = float(os.stat('./output/output.csvz').st_size)


#Print Output
print 'Results (indexed to JSON):'
print '\t%s\t%s\t%s'               % ('Write', 'Read', 'Size')
print 'json   \t%0.2f\t%0.2f\t%0.2f' % (json_write   / json_write, json_read   / json_read, json_size   / json_size)
print 'proto  \t%0.2f\t%0.2f\t%0.2f' % (proto_write  / json_write, proto_read  / json_read, proto_size  / json_size)
print 'json.gz\t%0.2f\t%0.2f\t%0.2f' % (gzjson_write / json_write, gzjson_read / json_read, gzjson_size / json_size)
print 'csv.gz \t%0.2f\t%0.2f\t%0.2f' % (gzcsv_write  / json_write, gzcsv_read  / json_read, gzcsv_size  / json_size)
