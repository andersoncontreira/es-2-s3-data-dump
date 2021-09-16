import json
import os
import sys
import time
from datetime import datetime
from math import ceil, floor
from os import path
from queue import Queue

from services.aws_helper import get_elasticsearch_client, get_s3_client
from services.logging import get_logger
from services.thread_executor import ThreadExecutor


class ImportService:
    def __init__(self, index=None, logger=None, es_client=None, s3_client=None):

        self.block_result = []
        self.logger = logger if logger is not None else get_logger()
        self.es_client = es_client if es_client is not None else get_elasticsearch_client(with_params=True)
        self.s3_client = s3_client if s3_client is not None else get_s3_client(with_params=False)

        # elasticsearch index
        self.index = index
        # temp target
        self.target = "/tmp/{}".format(index)
        # num max of threads
        self.threads_count = 8
        # thread execution counter
        self.threads_item_counter = 0
        # queue size
        self.threads_queue_size = 500
        # execution key
        self.execution_key = "backup_{}".format(datetime.now().strftime('%Y%m%d%H%M%S'))
        # execution total items
        self.total_items = 0
        # total of requests
        self.total_request = 0
        # process queue
        self.queue = Queue()
        # final result
        self.results = {
            'items': [],
            'total_items': self.total_items,
            'total_request': 0,
            'total_items_per_file': self.threads_queue_size
        }
        # preserve data ?
        self.preserve_tmp_data = False

        # execute upload ?
        self.execute_upload_to_s3 = True

    def import_data(self, custom_filter, custom_sort=None):
        self.total_items = self.get_elastic_count(custom_filter)
        if self.total_items == 0:
            return self._finish_callback([])

        # limit of the search item
        es_max_items = 10000

        # max loop iteraction
        max_loop_interactions = ceil(self.total_items / es_max_items)

        self.total_request = max_loop_interactions * 10

        for b in range(0, max_loop_interactions):
            search_after = b * es_max_items
            for i in range(0, 10):
                _from = i * 1000
                search_filter = self.get_default_filter()
                search_filter['from'] = 0
                search_filter["size"] = self.threads_queue_size
                search_filter["search_after"] = [search_after + _from]
                search_filter["sort"] = self.get_default_sort()
                if custom_filter:
                    del search_filter['query']['match_all']
                    search_filter['query'].update(custom_filter)

                if custom_sort:
                    search_filter["sort"] = custom_sort
                self.queue.put(search_filter)
            thread_executor = ThreadExecutor(queue=self.queue, logger=self.logger)
            thread_executor.set_max_works(self.threads_count)
            thread_executor.execute(future_fn=self._do_request, finish_callback=self._block_callback)
            self.logger.info('slepping...')
            time.sleep(5)

        result = []
        for items in self.block_result:
            for item in items:
                result.append(item)

        self.results = {
            'items': result,
            'total_items': self.total_items,
            'total_request': self.total_request,
            'total_items_per_file': self.threads_queue_size
        }

    def get_default_sort(self):
        return [
            {"datetime": "asc"}
        ]

    def _block_callback(self, results):
        self.block_result.append(results)
        # print(results)

    def get_default_filter(self):
        search_filter = {
            "query": {
                "match_all": {},
            }
        }
        return search_filter

    def get_elastic_count(self, custom_filter=None):
        search_filter = self.get_default_filter()
        if custom_filter:
            del search_filter['query']['match_all']
            search_filter['query'].update(custom_filter)

        self.logger.info('filter: {}'.format(search_filter))

        result = self.es_client.count(index=self.index, body=search_filter)
        # result = self.es_client.count(index=self.index)

        # self.logger.info("Result: {}".format(result))
        self.logger.info("Counting {} items in index {}".format(result['count'], self.index))

        return result['count'] or 0

    def _do_request(self):
        self.threads_item_counter = self.threads_item_counter + 1
        response = True
        search_fields = self.queue.get()
        try:
            response = self.es_client.search(index=self.index, body=search_fields)

            items = json.dumps(response['hits']['hits'])
            response = self._do_upload(items)
        except Exception as err:
            self.logger.error(err)
            self.logger.error('search filter: {}'.format(search_fields))
        self.logger.info('Processing: {} from {} items....'.format(self.threads_item_counter, self.total_request))
        return response

    def _finish_callback(self, result):
        self.logger.info('result')
        self.results = {
            'items': result,
            'total_items': self.total_items,
            'total_request': self.total_request,
            'total_items_per_file': self.threads_queue_size
        }

    def _do_upload(self, items):
        result = True
        bucket = os.environ['BUCKET_NAME'] if 'BUCKET_NAME' in os.environ else None

        if bucket == '':
            raise Exception('Bucket name empty')

        now = datetime.now()
        file_name = 'process.{}.{}.{}.{}.json'.format(
            self.index, self.threads_item_counter, now.strftime('%Y%m%d%H%M%S'), now.timestamp())

        file_type = self.index

        bucket_file_name = path.join(file_type, self.execution_key, file_name)

        temp_file = self._create_file(items, bucket, file_name, bucket_file_name)

        try:
            if self.execute_upload_to_s3:
                self.logger.info('Uploading to S3 %s %s %s' % (temp_file, bucket, bucket_file_name))
                self.s3_client.upload_file(temp_file, bucket, bucket_file_name)
            else:
                self.logger.info('Ignoring upload to S3 %s %s %s' % (temp_file, bucket, bucket_file_name))
        except Exception as err:
            self.logger.error(err)
            result = False
        finally:
            if self.preserve_tmp_data is False:
                self.logger.info('Removing temp file {}'.format(temp_file))
                try:
                    os.remove(temp_file)
                    # todo aplicar solução final para remover a pasta do bucket no tmp e excluir tudo
                    # self.logger.info('rmv')
                except Exception as err:
                    self.logger.error('Unable to remove file {}'.format(temp_file))
                    self.logger.error(err)

        return {"uploaded": result, "bucket_file_name": bucket_file_name, "bucket": bucket}

    def _create_file(self, body, bucket, file_name, bucket_file_name):
        tmp_root = '/tmp'
        tmp_bucket_folder = '{}/{}'.format(tmp_root, bucket)
        bucket_folder = bucket_file_name.replace('/' + file_name, '')
        destination_folder = '/tmp/{}/{}'.format(bucket, bucket_folder)

        # create the tmp root folder
        if not path.isdir(tmp_bucket_folder):
            os.mkdir(tmp_bucket_folder)

        if not path.isdir(destination_folder):
            current_dir = tmp_bucket_folder

            destination_folder_parts = destination_folder.replace(tmp_bucket_folder, '').split('/')
            for folder in destination_folder_parts:
                if folder is '':
                    continue
                current_dir = '{}/{}'.format(current_dir, folder)
                print(current_dir)
                if not path.isdir(current_dir):
                    os.mkdir(current_dir)

        # temp_file = path.join('/tmp/{}'.format(bucket), file_name)
        # temp_file = path.join('/tmp/{}/{}'.format(bucket, bucket_folder), file_name)
        temp_file = path.join(destination_folder, file_name)
        self.logger.info('temp_file: {}'.format(file_name))
        with open(temp_file, 'w') as f:
            f.write(body)
            f.close()

        return temp_file

    def get_results(self):
        return self.results
