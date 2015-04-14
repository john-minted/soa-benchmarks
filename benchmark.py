import os
import time
import gzip
import random
import json

from jsonschema import validate

from schemas import addressbook_pb2

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

NUM_RECORDS = 100000

# Internal (non-IDL) representation of person object
class Person(object):
  def __init__(self, name, id, email, phone_type, phone_number_list):
    self.name = name
    self.id = id
    self.email = email
    self.phone_type = phone_type
    self.phone_number_list = phone_number_list

class Benchmark(object):

  def __init__(self):
    # Add people to addressbook
    print 'Adding %d people to address book...' % NUM_RECORDS

    self._schema_avro = avro.schema.parse(open('schemas/person.avsc').read())

    with open('schemas/addressbook.json') as file:
      self._schema_json = json.loads(file.read())

    self._base_person_list = []
    self._data_protobuf_list = []
    self._data_dict = []

    self._data_pb   = addressbook_pb2.AddressBook()

    for x in range(0, NUM_RECORDS):
      base_person_obj = self._create_base_person()
      self._base_person_list.append(base_person_obj)

      person = self._data_pb.person.add()
      data   = self._createPerson(person)
      self._data_dict.append(data)

  def _get_json_person(self, base_person):
    json_person = {}
    json_person['name'] = base_person.name
    json_person['id'] = base_person.id
    json_person['email'] = base_person.email
    json_person['phone'] = base_person.phone_number_list
    return json_person

  def _get_proto_buf_person(self, base_person):
    protobuf_person = addressbook_pb2.Person()
    protobuf_person.name = base_person.name
    protobuf_person.id = base_person.id
    protobuf_person.email = base_person.email
    phone = protobuf_person.phone.add()
    phone.number = base_person.phone_number_list[0]
    phone.type = addressbook_pb2.Person.HOME
    phone = protobuf_person.phone.add()
    phone.number = base_person.phone_number_list[1]
    phone.type = addressbook_pb2.Person.HOME
    return protobuf_person

  def _create_base_person(self):
    name = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for x in range(0, 20))
    id = random.randint(0, NUM_RECORDS)
    email = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz@.') for x in range(0, 30))
    phone_type = random.choice([0, 1, 2])
    phone_number_list = []
    for x in range(0,2):
      phone_number_list.append(''.join(random.choice('0123456789') for x in range(0, 10)))

    return Person(name, id, email, phone_type, phone_number_list)

  def _createPerson(self, person):
    """Generates a person in Python dictionary and protobuf (which is passed in as a parameter)
    Input parameter person is a protobuf that is mutated
    Returns a dictionary representation of protobuf schema
    """
    res = {}

    #Add Person's Information
    person.name  = res['name']  = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for x in range(0, 20))
    person.id    = res['id']    = random.randint(0, NUM_RECORDS)
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

    if format == 'json' or format == 'jsch':
      with open('./output/output.json', 'w') as file:
        for base_person_obj in self._base_person_list:
          file.write(json.dumps(self._get_json_person(base_person_obj), separators=(',', ':')))
        # file.write(json.dumps(self._data_dict, separators=(',', ':')))

    elif format == 'avro':
      writer = DataFileWriter(open('./output/output.avro', 'wb'), DatumWriter(), self._schema_avro)
      for user in self._data_dict:
        writer.append(user)
      writer.close()

    elif format == 'protobuf':
      with open('./output/output.pb', 'wb') as file:
        for base_person_obj in self._base_person_list:
          protobuf_person = self._get_proto_buf_person(base_person_obj)
          file.write(protobuf_person.SerializeToString())

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

    if format == 'jsch':
      with open('./output/output.json') as file:
        validate(json.loads(file.read()), self._schema_json)

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
    extension = {'json': 'json', 'jsch': 'json', 'protobuf': 'pb', 'gzjson': 'jsz', 'avro': 'avro'}
    return float(os.stat('./output/output.%s' % extension[format]).st_size)

benchmark = Benchmark()

# Write Benchmarks
print 'Running write benchmarks...'
proto_write  = benchmark.write('protobuf')
avro_write   = benchmark.write('avro')
# jsch_write   = benchmark.write('jsch')
gzjson_write = benchmark.write('gzjson')
json_write   = benchmark.write('json')


# Read Benchmarks
# print 'Running read benchmarks...'
# proto_read  = benchmark.read('protobuf')
# avro_read   = benchmark.read('avro')
# jsch_read   = benchmark.read('jsch')
# gzjson_read = benchmark.read('gzjson')
# json_read   = benchmark.read('json')


# File Size Benchmarks
print 'Running file size benchmarks...\n'
proto_size  = benchmark.size('protobuf')
avro_size   = benchmark.size('avro')
# jsch_size   = benchmark.size('jsch')
# gzjson_size = benchmark.size('gzjson')
json_size   = benchmark.size('json')


# # Print Results with read
# print 'Results:'
# print '\t\t%s\t%s\t%s\t%s'                         % ('Write (s)', 'Write (s) / record' , 'Read (s)' , 'Size (bytes)')
# print 'json       \t%0.2f\t\t%0.5f\t\t\t%0.2f\t\t%0.2f' % (json_write  , json_write / NUM_RECORDS,   json_read  , json_size)
# print 'json.gzjson\t%0.2f\t\t%0.5f\t\t\t%0.2f\t\t%0.2f' % (gzjson_write, gzjson_write / NUM_RECORDS, gzjson_read, gzjson_size)
# print 'json-schema\t%0.2f\t\t%0.5f\t\t\t%0.2f\t\t%0.2f' % (json_write  , json_write / NUM_RECORDS,   jsch_read  , jsch_size)
# print 'avro       \t%0.2f\t\t%0.5f\t\t\t%0.2f\t\t%0.2f' % (avro_write  , avro_write / NUM_RECORDS,   avro_read  , avro_size)
# print 'proto      \t%0.2f\t\t%0.5f\t\t\t%0.2f\t\t%0.2f' % (proto_write , proto_write / NUM_RECORDS,  proto_read , proto_size)
# print

#Print Results
print 'Results:'
print '\t\t%s\t%s\t%s'                         % ('Write (s)', 'Write (s) / record', 'Size (bytes)')
print 'json       \t%0.2f\t\t%0.5f\t\t\t%0.2f' % (json_write  , json_write / NUM_RECORDS, json_size)
# print 'json.gzjson\t%0.2f\t\t%0.5f\t\t\t%0.2f' % (gzjson_write, gzjson_write / NUM_RECORDS, gzjson_size)
# print 'json-schema\t%0.2f\t\t%0.5f\t\t\t%0.2f' % (json_write  , json_write / NUM_RECORDS, jsch_size)
print 'avro       \t%0.2f\t\t%0.5f\t\t\t%0.2f' % (avro_write  , avro_write / NUM_RECORDS, avro_size)
print 'proto      \t%0.2f\t\t%0.5f\t\t\t%0.2f' % (proto_write , proto_write / NUM_RECORDS, proto_size)
print


# #Print Results indexed to JSON with read
# print 'Results (indexed to JSON):'
# print '\t\t%s\t%s\t%s'                   % ('Write'                  , 'Read'                 , 'Size')
# print 'json       \t%0.2f\t%0.2f\t%0.2f' % (json_write   / json_write, json_read   / json_read, json_size   / json_size)
# print 'json.gzjson\t%0.2f\t%0.2f\t%0.2f' % (gzjson_write / json_write, gzjson_read / json_read, gzjson_size / json_size)
# print 'json-schema\t%0.2f\t%0.2f\t%0.2f' % (json_write   / json_write, jsch_read   / json_read, jsch_size   / json_size)
# print 'avro       \t%0.2f\t%0.2f\t%0.2f' % (avro_write   / json_write, avro_read   / json_read, avro_size   / json_size)
# print 'proto      \t%0.2f\t%0.2f\t%0.2f' % (proto_write  / json_write, proto_read  / json_read, proto_size  / json_size)

#Print Results indexed to JSON
print 'Results (indexed to JSON):'
print '\t\t%s\t%s'                   % ('Write'                         , 'Size')
print 'json       \t%0.2f\t%0.2f' % (json_write   / json_write, json_size   / json_size)
# print 'json.gzjson\t%0.2f\t%0.2f' % (gzjson_write / json_write, gzjson_size / json_size)
# print 'json-schema\t%0.2f\t%0.2f' % (json_write   / json_write, jsch_size   / json_size)
print 'avro       \t%0.2f\t%0.2f' % (avro_write   / json_write, avro_size   / json_size)
print 'proto      \t%0.2f\t%0.2f' % (proto_write  / json_write, proto_size  / json_size)
