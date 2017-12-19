import numpy as np
import time
import faiss
import pickle
import signal
import sys
from multiprocessing import Process

import uuid
import redis
import os
from util import s3
from bluelens_spawning_pool import spawning_pool
from stylelens_object.objects import Objects
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
RELEASE_MODE = os.environ['RELEASE_MODE']
FEATURE_GRPC_HOST = os.environ['FEATURE_GRPC_HOST']
FEATURE_GRPC_PORT = os.environ['FEATURE_GRPC_PORT']
DATA_SOURCE = os.environ['DATA_SOURCE']

DB_OBJECT_HOST = os.environ['DB_OBJECT_HOST']
DB_OBJECT_PORT = os.environ['DB_OBJECT_PORT']
DB_OBJECT_NAME = os.environ['DB_OBJECT_NAME']
DB_OBJECT_USER = os.environ['DB_OBJECT_USER']
DB_OBJECT_PASSWORD = os.environ['DB_OBJECT_PASSWORD']

DATA_SOURCE_QUEUE = 'REDIS_QUEUE'
DATA_SOURCE_DB = 'DB'

REDIS_OBJECT_FEATURE_QUEUE = 'bl:object:feature:queue'
REDIS_OBJECT_INDEX_QUEUE = 'bl:object:index:queue'
REDIS_OBJECT_LIST = 'bl:object:list'
REDIS_OBJECT_HASH = 'bl:object:hash'

REDIS_INDEX_RESTART_QUEUE = 'bl:index:restart:queue'


SPAWNING_CRITERIA = 50
INTERVAL_TIME = 60

AWS_BUCKET = 'bluelens-style-index'
INDEX_FILE = 'faiss.index'

options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-object-index')
rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)
storage = s3.S3(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)

object_api = None

def spawn_indexer(uuid):

  pool = spawning_pool.SpawningPool()

  project_name = 'bl-image-indexer-' + uuid
  log.info('spawn_indexer: ' + project_name)

  pool.setServerUrl(REDIS_SERVER)
  pool.setServerPassword(REDIS_PASSWORD)
  pool.setApiVersion('v1')
  pool.setKind('Pod')
  pool.setMetadataName(project_name)
  pool.setMetadataNamespace(RELEASE_MODE)
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
  pool.addContainerEnv(container, 'RELEASE_MODE', RELEASE_MODE)
  pool.addContainerEnv(container, 'FEATURE_GRPC_HOST', FEATURE_GRPC_HOST)
  pool.addContainerEnv(container, 'FEATURE_GRPC_PORT', FEATURE_GRPC_PORT)
  pool.addContainerEnv(container, 'DB_OBJECT_HOST', DB_OBJECT_HOST)
  pool.addContainerEnv(container, 'DB_OBJECT_PORT', DB_OBJECT_PORT)
  pool.addContainerEnv(container, 'DB_OBJECT_USER', DB_OBJECT_USER)
  pool.addContainerEnv(container, 'DB_OBJECT_PASSWORD', DB_OBJECT_PASSWORD)
  pool.addContainerEnv(container, 'DB_OBJECT_NAME', DB_OBJECT_NAME)
  pool.setContainerImage(container, 'bluelens/bl-image-indexer:' + RELEASE_MODE)
  pool.addContainer(container)
  pool.setRestartPolicy('Never')
  pool.spawn()

def save_objects_to_db(objects):
  log.info('save_object_to_db')
  global object_api
  try:
    api_response = object_api.update_objects(objects)
    log.debug(api_response)
  except Exception as e:
    log.warn("Exception when calling update_object: %s\n" % e)

def start_index(rconn):
  global  object_api
  object_api = Objects()
  file = os.path.join(os.getcwd(), INDEX_FILE)
  # index_file = load_index_file(file)
  index_file = None
  if DATA_SOURCE == DATA_SOURCE_QUEUE:
    load_from_queue(index_file)
  elif DATA_SOURCE == DATA_SOURCE_DB:
    load_from_db(index_file)

def load_from_db(index_file):
  log.info('load_from_db')
  VECTOR_SIZE = 2048

  if index_file is None:
    log.debug('Create a new index file')
    index = faiss.IndexFlatL2(VECTOR_SIZE)
    index2 = faiss.IndexIDMap(index)
  else:
    log.debug('Load from index file')
    index2 = faiss.read_index(index_file)

  offset = 0
  limit = 50
  id_num = 1

  try:
    while True:
      res = object_api.get_objects_with_null_index(offset=offset, limit=limit)

      if len(res) == 0:
        time.sleep(INTERVAL_TIME)
        continue

      objects = []
      for obj in res:
        xb = np.expand_dims(np.array(obj['feature'], dtype=np.float32), axis=0)
        id_array = []
        id_array.append(id_num)
        id_set = np.array(id_array)
        index2.add_with_ids(xb, id_set)

        new_obj = {}
        new_obj['name'] = obj['name']
        new_obj['index'] = id_num
        objects.append(new_obj)
        id_num = id_num + 1

      save_objects_to_db(objects)

      if limit > len(res):
        break
      else:
        offset = offset + limit

  except Exception as e:
    log.error(str(e))

def save_index_file(file):
  log.info('save_index_file')
  storage.upload_file_to_bucket(AWS_BUCKET, file, RELEASE_MODE + '/' + INDEX_FILE, is_public=True)

def load_index_file(file):
  log.info('load_index_file')
  try:
    storage.download_file_from_bucket(AWS_BUCKET, file, RELEASE_MODE + '/' + INDEX_FILE)
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
    sys.exit()

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
    rconn.rpush(REDIS_OBJECT_LIST, obj['name'])
    d = pickle.dumps(obj)
    rconn.hset(REDIS_OBJECT_HASH, obj['name'], obj['product_id'])

    # xb = np.array(features)
    id_num = rconn.llen(REDIS_OBJECT_LIST)
    # log.debug(id_num)
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
    # log.info('indexing time: ' + str(elapsed_time))
    if i % 50 == 0:
      file = os.path.join(os.getcwd(), INDEX_FILE)
      faiss.write_index(index2, file)
      save_index_file(file)
    i = i + 1
    # log.info('index done')

    # ToDo:
    # save_to_db()

def dispatch_indexer(rconn):
  def request_stop(signum, frame):
    log.info('stopping')
    rconn.connection_pool.disconnect()
    log.info('connection closed')
    sys.exit()

  signal.signal(signal.SIGINT, request_stop)
  signal.signal(signal.SIGTERM, request_stop)

  while True:
    len = rconn.llen(REDIS_OBJECT_INDEX_QUEUE)
    if len > 0:
      spawn_indexer(str(uuid.uuid4()))
    time.sleep(60)

def restart(rconn, pids):
  while True:
    key, value = rconn.blpop([REDIS_INDEX_RESTART_QUEUE])
    for pid in pids:
      os.kill(pid, signal.SIGTERM)
    sys.exit()

if __name__ == '__main__':
  pids = []
  p1 = Process(target=dispatch_indexer, args=(rconn,))
  p1.start()
  pids.append(p1.pid)
  p2 = Process(target=start_index, args=(rconn,))
  p2.start()
  pids.append(p2.pid)
  Process(target=restart, args=(rconn, pids)).start()