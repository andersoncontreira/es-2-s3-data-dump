import os
from datetime import datetime

import pytz as pytz

from services.boot import load_env
from services.logging import get_logger

load_env()
from services.import_service import ImportService


def handler():
    logger = get_logger()
    index = os.environ['ELASTIC_INDEX']
    service = ImportService(index=index, logger=logger)

    logger.info("---------------------------------------------------------")
    logger.info("Beginning at {}".format(datetime.now(tz=pytz.timezone("America/Sao_Paulo"))))
    logger.info("---------------------------------------------------------")

    custom_filter = {
        "bool": {
            "filter": [
                {"range": {"datetime": {"gte": "2021-03-15T03:58:46.560Z"}}}
            ]
        }
    }

    event_name = "PURCHASE_ORDER_TEMPORARY_BREAK"
    custom_filter = {
            "bool": {
                "must": [{
                    "match": {
                        "name": {
                            "query": event_name
                        }
                    }
                },
                    {
                        "range": {
                            "date": {
                                "gte": "2021-09-01T00:00:00",
                                "lte": "2021-09-14T00:00:00"
                            }
                        }
                    }]
            }
        }

    custom_sort = [
            {"date": "asc"}
        ]
    service.execution_key = "{}/{}".format(event_name, service.execution_key)
    service.import_data(custom_filter, custom_sort)
    result = service.get_results()

    logger.info("---------------------------------------------------------")
    logger.info("Finishing at {}".format(datetime.now(tz=pytz.timezone("America/Sao_Paulo"))))
    logger.info("---------------------------------------------------------")

    if len(result['items']) > 0:
        logger.info('Total processed items: {}'.format(result['total_items']))
        logger.info('Total processed requests: {}'.format(result['total_request']))
        logger.info('Total items per file: {}'.format(result['total_items_per_file']))
        logger.info("---------------------------------------------------------")
        logger.info('{} - {} - {}'.format('bucket', 'bucket_file_name', 'uploaded?'))
        logger.info("---------------------------------------------------------")

        for res in result['items']:
            logger.info('{} - {} - {}'.format(res['bucket'], res['bucket_file_name'], res['uploaded']))
    else:
        logger.info('No items and no files processed')

    logger.info("---------------------------------------------------------")
    logger.info("Exiting ...")
    logger.info("---------------------------------------------------------")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    handler()
