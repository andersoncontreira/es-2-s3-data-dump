import os

import boto3
from services.logging import get_logger
from elasticsearch import Elasticsearch
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection

logger = get_logger()


def get_sqs_client():
    client = boto3.client(
        service_name="sqs",
        region_name=os.environ["REGION_NAME"]
    )
    return client


def get_s3_client(with_params=False):
    s3 = None
    aws_access_key_id = None
    aws_secret_access_key = None
    if with_params:
        aws_access_key_id = os.environ['ELASTICS_AWS_ACCESS_KEY_ID']
        aws_secret_access_key = os.environ['ELASTICS_AWS_SECRET_ACCESS_KEY']

    try:
        profile = os.environ['AWS_PROFILE'] if 'AWS_PROFILE' in os.environ else None
        logger.info('profile: {}'.format(profile))
        if profile:
            session = boto3.session.Session(profile_name=profile)
            s3 = session.client(
                's3',
                region_name="sa-east-1",
                aws_access_key_id = aws_access_key_id,
                aws_secret_access_key = aws_secret_access_key
            )

        else:
            s3 = boto3.client(
                's3',
                region_name="sa-east-1",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )
    except Exception as err:
        logger.error(err)

    return s3


def get_elasticsearch_client(with_params=False):
    # credentials = boto3.Session().get_credentials() awsauth = AWS4Auth(credentials.access_key,
    # credentials.secret_key, region, service, session_token=credentials.token)
    awsauth = AWS4Auth(
        os.environ['ELASTICS_AWS_ACCESS_KEY_ID'],
        os.environ['ELASTICS_AWS_SECRET_ACCESS_KEY'],
        os.environ['REGION_NAME'],
        'es'
    )

    if with_params:
        return Elasticsearch([os.environ["ELASTIC_URL"]],
                             use_ssl=is_https(),
                             verify_certs=True,
                             scheme=get_protocol(),
                             port=os.environ["ELASTIC_PORT"],
                             connection_class=RequestsHttpConnection,
                             http_auth=awsauth,
                             )
    else:
        return Elasticsearch([os.environ["ELASTIC_URL"]],
                             use_ssl=is_https()
                             )


def get_protocol():
    return 'https://' if is_https() else 'http://'


def is_https():
    return True if 'ELASTIC_HTTPS' in os.environ and str(os.getenv('ELASTIC_HTTPS')).lower() == 'true' else False
