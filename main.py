#!/usr/bin/env python

import datetime
import logging
import random
import riak
import socket
import sys
import time

try:
    from gevent import monkey
    monkey.patch_all()
    monkey.patch_socket(aggressive=True, dns=True)
    monkey.patch_select(aggressive=True)
    sys.stdout.write('using gevent.monkey\n')
except ImportError:
    sys.stderr.write('NOT using gevent.monkey\n')

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

if len(sys.argv) >= 5:
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

ycsb_row_size = 100
ycsb_row_count = 10

randstr = ''
for i in range(ycsb_row_size):
    randstr += chr(random.randint(ord('a'), ord('z')))

def generate_rows(worker_id, start_timestamp, batch_sz):
    timestamp = start_timestamp
    batch = []
    for i in xrange(batch_sz):
        cells = []
        cells.append(hostname)
        cells.append(worker_id)
        cells.append(timestamp)
        for i in xrange(10):
            cells.append(randstr)
        timestamp += 1
        batch.append(cells)
    return batch

client = riak.RiakClient(
        protocol='pbc',
        nodes=nodes,
        multiget_pool_size=pool_size,
        multiput_pool_size=pool_size)

records_written = 0
ops_count = 0
start_time = time.time()
start_ms = riak.util.unix_time_millis(datetime.datetime.utcnow())

table_name = 'tsycsb'
table = client.table(table_name)

while records_written < record_count:
    ts_objs = []
    for i in xrange(worker_count):
        wid = 'worker-{}'.format(i)
        rows = generate_rows(wid, start_ms, batch_size)
        ts_obj = table.new(rows)
        ts_objs.append(ts_obj)
    results = client.multiput(ts_objs)
    # TODO check results
    # if result != True:
    #     logger.error('got non-True result when storing batch')
    # TODO: orly?
    # https://github.com/BrianMMcClain/riak-java-benchmark/blob/master/src/main/java/com/basho/riak/BenchmarkWorker.java#L78
    batch_count = batch_size * worker_count
    records_written += batch_count # TODO Java increments by 1
    ops_count += len(ts_objs)
    start_ms += batch_count
    if records_written % 1000 == 0:
        logging.info('records_written: %d', records_written)

client.close()
end_time = time.time()
elapsed_secs = end_time - start_time

logging.info('wrote %d records in %d seconds', records_written, elapsed_secs)
logging.info('throughput: %d recs/sec', record_count // elapsed_secs)
logging.info('throughput: %d ops/sec', ops_count // elapsed_secs)
