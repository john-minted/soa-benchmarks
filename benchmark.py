import os
import time
import gzip
import random
import json
from pb_schemas import addressbook_pb2

class Benchmark(object):

    def __init__(self):
        # Add 100,000 people to addressbook
        print 'Adding 100,000 people to address book...'

        self._data_pb   = addressbook_pb2.AddressBook()
        self._data_dict = []

        for x in range(0, 100000):
            person = self._data_pb.person.add()
            data   = self._createPerson(person)
            self._data_dict.append(data)


    def _createPerson(self, person):
        """Generates a person in Python dictionary and protobuf (which is passed in as a parameter)
        Input parameter person is a protobuf that is mutated
        Returns a dictionary representation of protobuf schema
        """
        res = {}

        #Add Person's Information
        person.name  = res['name']  = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for x in range(0, 20))
        person.id    = res['id']    = random.randint(0, 100000)
        person.email = res['email'] = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz@.') for x in range(0, 30))

        res['phone'] = []
        for phone_count in range(0, 2):
            pphone = person.phone.add()
            resphone = {}
            res['phone'].append(resphone)
            pphone.number = resphone['number'] = ''.join(random.choice('0123456789') for x in range(0, 10))
            pphone.type   = resphone['type']   = random.choice([0, 1, 2])

        return res

    def write(self, format):
        time_start = time.time()

        if format == 'json':
            open('./output/output.json', 'w').write(json.dumps(self._data_dict, separators=(',', ':')))

        elif format == 'protobuf':
            open('./output/output.pb', 'wb').write(self._data_pb.SerializeToString())

        elif format == 'gzjson':
            gzip.open('./output/output.jsz', 'wb').write(json.dumps(self._data_dict, separators=(',', ':')))

        time_end = time.time()

        return time_start - time_end

    def read(self, format):
        time_start = time.time()

        if format == 'json':
            json.loads(open('./output/output.json').read())

        elif format == 'protobuf':
            addressbook_pb2.AddressBook().ParseFromString(open('./output/output.pb', 'rb').read())

        elif format == 'gzjson':
            json.loads(gzip.open('./output/output.jsz', 'rb').read())

        time_end = time.time()

        return time_start - time_end

    def size(self, format):
        extension = {'json': 'json', 'protobuf': 'pb', 'gzjson': 'jsz'}
        return float(os.stat('./output/output.%s' % extension[format]).st_size)


    
benchmark = Benchmark()

# Write Benchmarks
print 'Running write benchmarks...'
json_write   = benchmark.write('json')
proto_write  = benchmark.write('protobuf')
gzjson_write = benchmark.write('gzjson')


# Read Benchmarks
print 'Running read benchmarks...'
json_read   = benchmark.read('json')
proto_read  = benchmark.read('protobuf')
gzjson_read = benchmark.read('gzjson')


# File Size Benchmarks
print 'Running file size benchmarks...\n'
json_size   = benchmark.size('json')
proto_size  = benchmark.size('protobuf')
gzjson_size = benchmark.size('gzjson')


#Print Output
print 'Results (indexed to JSON):'
print '\t%s\t%s\t%s'               % ('Write', 'Read', 'Size')
print 'json   \t%0.2f\t%0.2f\t%0.2f' % (json_write   / json_write, json_read   / json_read, json_size   / json_size)
print 'proto  \t%0.2f\t%0.2f\t%0.2f' % (proto_write  / json_write, proto_read  / json_read, proto_size  / json_size)
print 'json.gz\t%0.2f\t%0.2f\t%0.2f' % (gzjson_write / json_write, gzjson_read / json_read, gzjson_size / json_size)
