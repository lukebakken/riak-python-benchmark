#!/usr/bin/env python

import getopt
import logging
import os
import random
import riak
import socket
import sys
import time
import uuid

logging.basicConfig(level=logging.DEBUG,
        format='%(asctime)s [%(levelname)s] '
               '%(message)s')

def errexit(msg, *args):
    logging.error(msg, *args)
    sys.exit(1)

# HOSTS - comma separated list of Riak host IPs/Names
# RECORD_COUNT - total number of rows to write to Riak TS
# WORKER_COUNT - number of Threads to use, each with own RiakClient
# BATCH_SIZE - number of rows to write in each Put request
# POOL_SIZE - multiput pool size
usage = 'usage: python main.py HOSTS RECORD_COUNT WORKER_COUNT BATCH_SIZE [POOL_SIZE]'

if len(sys.argv) == 5:
    logging.debug('argv: %s', sys.argv)
    hosts = sys.argv[1].split(',')
    record_count = int(sys.argv[2])
    worker_count = int(sys.argv[3])
    batch_size   = int(sys.argv[4])
    pool_size    = 128
    if len(sys.argv) == 6:
        pool_size = int(sys.argv[5])
else:
    errexit(usage)

hostname = socket.getfqdn()

nodes = []
for i, host in enumerate(hosts):
    node, pb_port = host.split(':')
    # tuple format is:
    #     HOST    HTTP PORT  PB PORT
    node = (node, 8098, pb_port)
    nodes.append(node)

logging.debug('hostname: %s', hostname)
logging.debug('nodes: %s', nodes)
logging.debug('record_count: %s', record_count)
logging.debug('worker_count: %s', worker_count)
logging.debug('batch_size: %s', batch_size)
logging.debug('pool_size: %s', pool_size)

try:
    from gevent import monkey
    monkey.patch_all()
    monkey.patch_socket(aggressive=True, dns=True)
    monkey.patch_select(aggressive=True)
    logging.info('using gevent.monkey')
except ImportError as e:
    logging.debug(e)

ycsb_row_size = 100
ycsb_row_count = 10

def randstr(length):
    out = ''
    for i in range(length):
        out += chr(random.randint(ord('a'), ord('z')))
    return out

def generateTsValue(workerId, startTimestamp, batchSize):
    timestamp = startTimestamp
    batch = []
    for i in xrange(batchSize):
        cells = []
        cells.append(hostname)
        cells.append(workerId)
        cells.append(timestamp)
        for i in xrange(10):
            cells.append(randstr(ycsb_row_size))
        timestamp += 1
        batch.append(cells)
    return batch

client = riak.RiakClient(
        protocol='pbc',
        nodes=nodes,
        multiget_pool_size=pool_size,
        multiput_pool_size=pool_size)

counter = 0
start_time = time.time()

# report_interval = batch_sz * 100
# log('batch_sz: {} pool_sz: {} report_interval: {}'.format(
#     batch_sz, pool_sz, report_interval))
# for x in xrange(0, NUM_INS, batch_sz):
#     objs = []
#     for i in xrange(0, batch_sz):
#         rand_str = uuid.uuid4().hex
#         rand_int = random.randint(10, 1000)
#         json_object = {}
#         json_object['locationId'] = rand_str
#         json_object['locationDescription'] = rand_str
#         json_object['discoveryInterval'] = rand_int
#         json_object['locationName'] = rand_str
#         json_object['destinationDirectory'] = rand_str
#         json_object['modalityName'] = rand_str
#         json_object['mountPoint'] = rand_str
#         json_object['locationPath'] = rand_str
#
#         key = "{}-{}".format(pid, counter)
#         counter = counter + 1
#         obj = riak.RiakObject(client, bucket, key)
#         obj.data = json_object
#         objs.append(obj)
#     client.multiput(objs)
#     if x > 0 and x % report_interval == 0:
#         log('Current insert throughput for pid{} is {} op/sec'.format(os.getpid(),int(report_interval/(time.time()-start_time))))
#         start_time = time.time()
