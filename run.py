"""Running the Dewp ETL application"""
import argparse
import logging
import logging.config

import yaml

from dewp.common.s3 import S3BucketConnector
from dewp.transformers.dewp_transformers import DewpETL, DewpSourceConfig, DewpTargetConfig


def main():
    """
        entry point to run the Dewp ETL job.
    """
    # Parsing YML file
    parser = argparse.ArgumentParser(description='Run the Dewp ETL Job.')
    parser.add_argument('config', help='A configuration file in YAML format.')
#    args = parser.parse_args()

    config = 'C:/Emir/Projects/DEWP/dewp1/config/dewp_report1_config.yaml'
    config = yaml.safe_load(open(file=config,encoding='utf8'))

#    config = yaml.safe_load(open(args.config))
    # configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    # reading s3 configuration
    s3_config = config['s3']
    # creating the S3BucketConnector classes for source and target
    s3_bucket_src = S3BucketConnector(access_key=s3_config['access_key'],
                                      secret_key=s3_config['secret_key'],
                                      endpoint_url=s3_config['src_endpoint_url'],
                                      bucket=s3_config['src_bucket'])
    s3_bucket_trg = S3BucketConnector(access_key=s3_config['access_key'],
                                      secret_key=s3_config['secret_key'],
                                      endpoint_url=s3_config['trg_endpoint_url'],
                                      bucket=s3_config['trg_bucket'])
    # reading source configuration
    source_config = DewpSourceConfig(**config['source'])
    # reading target configuration
    target_config = DewpTargetConfig(**config['target'])
    # reading meta file configuration
    meta_config = config['meta']
    # creating DewpETL class
    logger = logging.getLogger(__name__)
    logger.info('Dewp ETL job started.')
    dewp_etl = DewpETL(s3_bucket_src, s3_bucket_trg,source_config, target_config)
    # running etl job for Dewp report 1
    dewp_etl.etl_report1()
    logger.info('Dewp ETL job finished.')


if __name__ == '__main__':
    main()
