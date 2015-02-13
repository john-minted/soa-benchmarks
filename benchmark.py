import os
import time
import gzip
import random
import json

from pb_schemas import addressbook_pb2

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

class Benchmark(object):

  def __init__(self):
    # Add 100,000 people to addressbook
    print 'Adding 100,000 people to address book...'

    self._schema_avro = avro.schema.parse(open('avro_schemas/addressbook.avsc').read())

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
      pphone   = person.phone.add()
      resphone = {}
      pphone.number = resphone['number'] = ''.join(random.choice('0123456789') for x in range(0, 10))
      
      phoneEnumVal  = random.choice([0, 1, 2])
      pphone.type   = phoneEnumVal
      resphone['type'] = str(phoneEnumVal)
      res['phone'].append(resphone)

    return res

  def write(self, format):
    time_start = time.time()

    if format == 'json':
      with open('./output/output.json', 'w') as file:
          file.write(json.dumps(self._data_dict, separators=(',', ':')))

    elif format == 'avro':
      writer = DataFileWriter(open('./output/output.avro', 'w'), DatumWriter(), self._schema_avro)
      for user in self._data_dict:
        writer.append(user)
      writer.close()

    elif format == 'protobuf':
      with open('./output/output.pb', 'wb') as file:
          file.write(self._data_pb.SerializeToString())

    elif format == 'gzjson':
      with gzip.open('./output/output.jsz', 'wb') as file:
          file.write(json.dumps(self._data_dict, separators=(',', ':')))

    time_end = time.time()

    return time_end - time_start

  def read(self, format):
    time_start = time.time()

    if format == 'json':
      with open('./output/output.json') as file:
        json.loads(file.read())

    elif format == 'avro':
      reader = DataFileReader(open('./output/output.avro', 'r'), DatumReader())
      for user in reader:
        pass
      reader.close()

    elif format == 'protobuf':
      with open('./output/output.pb', 'rb') as file:
          addressbook_pb2.AddressBook().ParseFromString(file.read())

    elif format == 'gzjson':
      with gzip.open('./output/output.jsz', 'rb') as file: 
          json.loads(file.read())

    time_end = time.time()

    return time_end - time_start

  def size(self, format):
    extension = {'json': 'json', 'protobuf': 'pb', 'gzjson': 'jsz', 'avro': 'avro'}
    return float(os.stat('./output/output.%s' % extension[format]).st_size)


    
benchmark = Benchmark()

# Write Benchmarks
print 'Running write benchmarks...'
json_write   = benchmark.write('json')
avro_write   = benchmark.write('avro')
proto_write  = benchmark.write('protobuf')
gzjson_write = benchmark.write('gzjson')


# Read Benchmarks
print 'Running read benchmarks...'
json_read   = benchmark.read('json')
avro_read   = benchmark.read('avro')
proto_read  = benchmark.read('protobuf')
gzjson_read = benchmark.read('gzjson')


# File Size Benchmarks
print 'Running file size benchmarks...\n'
json_size   = benchmark.size('json')
avro_size   = benchmark.size('avro')
proto_size  = benchmark.size('protobuf')
gzjson_size = benchmark.size('gzjson')


#Print Results
print 'Results:'
print '\t%s\t%s\t%s'                     % ('Write (s)', 'Read (s)', 'Size (bytes)')
print 'json   \t%0.2f\t\t%0.2f\t\t%0.2f' % (json_write  , json_read  , json_size)
print 'avro   \t%0.2f\t\t%0.2f\t\t%0.2f' % (avro_write  , avro_read  , avro_size)
print 'proto  \t%0.2f\t\t%0.2f\t\t%0.2f' % (proto_write , proto_read , proto_size)
print 'json.gz\t%0.2f\t\t%0.2f\t\t%0.2f' % (gzjson_write, gzjson_read, gzjson_size)
print


#Print Results indexed to JSON
print 'Results (indexed to JSON):'
print '\t%s\t%s\t%s'                 % ('Write', 'Read', 'Size')
print 'json   \t%0.2f\t%0.2f\t%0.2f' % (json_write   / json_write, json_read   / json_read, json_size   / json_size)
print 'avro   \t%0.2f\t%0.2f\t%0.2f' % (avro_write   / json_write, avro_read   / json_read, avro_size   / json_size)
print 'proto  \t%0.2f\t%0.2f\t%0.2f' % (proto_write  / json_write, proto_read  / json_read, proto_size  / json_size)
print 'json.gz\t%0.2f\t%0.2f\t%0.2f' % (gzjson_write / json_write, gzjson_read / json_read, gzjson_size / json_size)
