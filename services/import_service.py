import json
import os
from datetime import datetime
from math import ceil
from os import path
from queue import Queue

from services.aws_helper import get_elasticsearch_client, get_s3_client
from services.logging import get_logger
from services.thread_executor import ThreadExecutor


class ImportService:
    def __init__(self, index=None, logger=None, es_client=None, s3_client=None):

        self.logger = logger if logger is not None else get_logger()
        self.es_client = es_client if es_client is not None else get_elasticsearch_client(with_params=True)
        self.s3_client = s3_client if s3_client is not None else get_s3_client(with_params=True)

        # elasticsearch index
        self.index = index
        # temp target
        self.target = "/tmp/{}".format(index)
        # num max of threads
        self.threads_count = 8
        # thread execution counter
        self.threads_item_counter = 0
        # queue size
        self.threads_queue_size = 5
        # execution key
        self.execution_key = "backup_{}".format(datetime.now().strftime('%Y%m%d%H%M%S'))
        # execution total items
        self.total_items = 0
        # process queue
        self.queue = Queue()
        # final result
        self.results = {
            'items': [],
            'total_items': 0,
            'total_index_items': self.total_items,
            'total_items_per_file': self.threads_queue_size
        }

    def import_data(self):
        self.total_items = self.get_elastic_count()
        if self.total_items == 0:
            return self._finish_callback([])

        item_per_search = self.threads_queue_size
        total_search_request = ceil(self.total_items / item_per_search)
        # total_search_request = 5

        for i in range(0, total_search_request):
            search_filter = {
                "from": i * item_per_search,
                "size": item_per_search,
                "query": {
                    "match_all": {}
                }
            }

            self.queue.put(search_filter)

        thread_executor = ThreadExecutor(queue=self.queue, logger=self.logger)
        thread_executor.set_max_works(self.threads_count)
        thread_executor.execute(future_fn=self._do_request, finish_callback=self._finish_callback)


    def get_elastic_count(self):
        result = self.es_client.count(index=self.index)

        # self.logger.info("Result: {}".format(result))
        self.logger.info("Counting {} items in index {}".format(result['count'], self.index))

        return result['count'] or 0

    def _do_request(self):
        self.threads_item_counter = self.threads_item_counter + 1
        search_fields = self.queue.get()
        response = self.es_client.search(index=self.index, body=search_fields)

        items = json.dumps(response['hits']['hits'])
        response = self._do_upload(items)

        return response

    def _finish_callback(self, result):
        self.logger.info('result')
        self.results = {
            'items': result,
            'total_items': len(result),
            'total_index_items': self.total_items,
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

        temp_file = self._create_file(items, file_name)

        file_type = self.index

        bucket_file_name = path.join(file_type, self.execution_key, file_name)

        self.logger.info('Uploading to S3 %s %s %s' % (temp_file, bucket, bucket_file_name))
        try:
            self.s3_client.upload_file(temp_file, bucket, bucket_file_name)

        except Exception as err:
            self.logger.error(err)
            result = False
        finally:
            self.logger.info('Removing temp file {}'.format(temp_file))
            try:
                os.remove(temp_file)
            except Exception as err:
                self.logger.error('Unable to remove file {}'.format(temp_file))
                self.logger.error(err)

        return {"uploaded": result, "bucket_file_name": bucket_file_name, "bucket": bucket}

    def _create_file(self, body, file_name):
        temp_file = path.join('/tmp', file_name)
        self.logger.info('temp_file: {}'.format(file_name))
        with open(temp_file, 'w') as f:
            f.write(body)
            f.close()

        return temp_file

    def get_results(self):
        return self.results
