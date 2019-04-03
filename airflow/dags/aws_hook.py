import os
import boto3
from configparser import ConfigParser


def _get_credentials(self, region_name):
    config = ConfigParser.RawConfigParser()
    config.read(os.path.expanduser('/root/.aws/credentials'))
    aws_access_key_id = config.get('dev', 'aws_access_key_id')
    aws_secret_access_key = config.get('dev', 'aws_secret_access_key')
    aws_session_token = config.get('dev', 'aws_session_token')
    region_name = config.get('dev', 'region')
    endpoint_url = None

    return boto3.session.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        region_name=region_name), endpoint_url
