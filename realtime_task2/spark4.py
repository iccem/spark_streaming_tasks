from hdfs import Config
import subprocess
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import hyperloglog
import time
import os
from ua_parser import user_agent_parser


client = Config().get_client()

with open( os.devnull, 'w' ) as devnull:
    nn_address = subprocess.check_output('hdfs getconf -confKey dfs.namenode.http-address', \
    stderr=open("/dev/null", "w"), shell=True).strip().decode("utf-8")
devnull.close()

sc = SparkContext(master='yarn-client')

DATA_PATH = "/data/realtime/uids"

batches = [sc.textFile(os.path.join(*[nn_address, DATA_PATH, path])) for path in client.list(DATA_PATH)[:30]]

BATCH_TIMEOUT = 2
ssc = StreamingContext(sc, BATCH_TIMEOUT)
dstream = ssc.queueStream(rdds=batches)


finished = False
printed = False


def set_ending_flag(rdd):
    global finished
    if rdd.isEmpty():
        finished = True


def print_only_at_the_end(rdd):
    global printed

    if finished and not printed:
        for seg, cnt in rdd.takeOrdered(10, key=lambda x: -x[1]):
            print("{} {}".format(seg, cnt))
        printed = True


dstream.foreachRDD(set_ending_flag)


def get_segment_2(x):
    user, agent = x.split('\t')

    device_family = user_agent_parser.ParseDevice(agent)
    device = device_family['family']

    browser_family = user_agent_parser.ParseUserAgent(agent)
    browser = browser_family['family']

    os_family = user_agent_parser.ParseOS(agent)
    os = os_family['family']

    l_phone = None
    l_ff = None
    l_win = None

    if device == 'iPhone':
        l_phone = user
    elif device != 'iPhone':
        l_phone = 'null'

    if browser == 'Firefox':
        l_ff = user
    elif browser != 'Firefox':
        l_ff = 'null'

    if os == 'Windows':
        l_win = user
    elif os != 'Windows':
        l_win = 'null'

    return [('seg_iphone', l_phone), ('seg_firefox', l_ff), ('seg_windows', l_win)]


hll_seg_iphone = hyperloglog.HyperLogLog(0.01)
hll_seg_firefox = hyperloglog.HyperLogLog(0.01)
hll_seg_windows = hyperloglog.HyperLogLog(0.01)

stateRDD = sc.parallelize([
    ('seg_iphone', hll_seg_iphone),
    ('seg_firefox', hll_seg_firefox),
    ('seg_windows', hll_seg_windows),
])


def ff(x):
    a, b = x[0], len(x[1])
    return (a, b)


def update_func(values, hll):
    for i in values:
        hll.add(i)
    return hll


dstream.flatMap(lambda line: line.split('\n')) \
    .flatMap(get_segment_2) \
    .updateStateByKey(update_func, initialRDD=stateRDD) \
    .map(ff) \
    .foreachRDD(print_only_at_the_end)


ssc.checkpoint('./checkpoint{}'.format(time.strftime("%Y_%m_%d_%H_%M_%s", time.gmtime())))
ssc.start()
while not printed:
     time.sleep(0.1)
ssc.stop()
sc.stop()
