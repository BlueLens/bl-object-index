import numpy as np
import time
import faiss
import pickle
import signal
from multiprocessing import Process

import uuid
import redis
import os
from util import s3
from bluelens_spawning_pool import spawning_pool
from bluelens_log import Logging

STR_BUCKET = "bucket"
STR_STORAGE = "storage"
STR_CLASS_CODE = "class_code"
STR_NAME = "name"
STR_FORMAT = "format"

AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']

DATA_SOURCE = os.environ['DATA_SOURCE']
DATA_SOURCE_QUEUE = 'REDIS_QUEUE'
DATA_SOURCE_DB = 'DB'

REDIS_OBJECT_FEATURE_QUEUE = 'bl:object:feature:queue'
REDIS_OBJECT_INDEX_QUEUE = 'bl:object:index:queue'
REDIS_OBJECT_LIST = 'bl:object:list'
REDIS_OBJECT_HASH = 'bl:object:hash'

SPAWNING_CRITERIA = 50

AWS_BUCKET = 'bluelens-style-index'
INDEX_FILE = 'faiss2.index'

options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-index')
rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)
storage = s3.S3(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)

def spawn_indexer(uuid):

  pool = spawning_pool.SpawningPool()

  project_name = 'bl-image-indexer-' + uuid
  log.info('spawn_indexer: ' + project_name)

  pool.setServerUrl(REDIS_SERVER)
  pool.setServerPassword(REDIS_PASSWORD)
  pool.setApiVersion('v1')
  pool.setKind('Pod')
  pool.setMetadataName(project_name)
  pool.setMetadataNamespace('index')
  pool.addMetadataLabel('name', project_name)
  pool.addMetadataLabel('group', 'bl-image-indexer')
  pool.addMetadataLabel('SPAWN_ID', uuid)
  container = pool.createContainer()
  pool.setContainerName(container, project_name)
  pool.addContainerEnv(container, 'AWS_ACCESS_KEY', AWS_ACCESS_KEY)
  pool.addContainerEnv(container, 'AWS_SECRET_ACCESS_KEY', AWS_SECRET_ACCESS_KEY)
  pool.addContainerEnv(container, 'REDIS_SERVER', REDIS_SERVER)
  pool.addContainerEnv(container, 'REDIS_PASSWORD', REDIS_PASSWORD)
  pool.addContainerEnv(container, 'SPAWN_ID', uuid)
  pool.setContainerImage(container, 'bluelens/bl-image-indexer:latest')
  pool.addContainer(container)
  pool.setRestartPolicy('Never')
  pool.spawn()

def start_index():
  file = os.path.join(os.getcwd(), INDEX_FILE)
  index_file = load_index_file(file)
  if DATA_SOURCE == DATA_SOURCE_QUEUE:
    load_from_queue(index_file)
  elif DATA_SOURCE == DATA_SOURCE_DB:
    load_from_db(index_file)

def load_from_db():
  log.info('load_from_db')
  # Need to implement

def save_index_file(file):
  log.info('save_index_file')
  storage.upload_file_to_bucket(AWS_BUCKET, file, INDEX_FILE, is_public=True)

def load_index_file(file):
  log.info('load_index_file')
  try:
    storage.download_file_from_bucket(AWS_BUCKET, file, INDEX_FILE)
  except:
    log.error('download error')
    file = None
  return file

def load_from_queue(index_file):
  log.info('load_from_queue')
  VECTOR_SIZE = 2048

  if index_file is None:
    log.debug('Create a new index file')
    index = faiss.IndexFlatL2(VECTOR_SIZE)
    index2 = faiss.IndexIDMap(index)
  else:
    log.debug('Load from index file')
    index2 = faiss.read_index(index_file)

  def items():
    while True:
      yield rconn.blpop([REDIS_OBJECT_FEATURE_QUEUE])

  def request_stop(signum, frame):
    log.info('stopping')
    rconn.connection_pool.disconnect()
    log.info('connection closed')

  signal.signal(signal.SIGINT, request_stop)
  signal.signal(signal.SIGTERM, request_stop)

  i = 0
  for item in items():
    key, obj_data = item
    obj = pickle.loads(obj_data)
    # log.debug(obj)

    feature = obj['feature']
    xb = np.expand_dims(np.array(feature, dtype=np.float32), axis=0)
    obj['feature'] = None
    log.debug(obj)
    rconn.rpush(REDIS_OBJECT_LIST, obj['name'])
    d = pickle.dumps(obj)
    rconn.hset(REDIS_OBJECT_HASH, obj['name'], obj['product_id'])

    # xb = np.array(features)
    id_num = rconn.llen(REDIS_OBJECT_LIST)
    id_array = []
    id_array.append(id_num)
    id_set = np.array(id_array)
    # print(xb)
    # print(np.shape(xb))
    # print(id_set)
    # print(xb.shape)
    # print(id_set.shape)
    # print(id_set)
    start_time = time.time()
    index2.add_with_ids(xb, id_set)
    elapsed_time = time.time() - start_time
    log.info('indexing time: ' + str(elapsed_time))
    file = os.path.join(os.getcwd(), INDEX_FILE)
    if i % 50 == 0:
      faiss.write_index(index2, file)
      save_index_file(file)
    i = i + 1
    log.info('index done')

    # ToDo:
    # save_to_db()

def dispatch_indexer(rconn):
  while True:
    len = rconn.llen(REDIS_OBJECT_INDEX_QUEUE)
    if len > 0:
      spawn_indexer(str(uuid.uuid4()))
    time.sleep(60)

if __name__ == '__main__':
  Process(target=dispatch_indexer, args=(rconn,)).start()
  start_index()

