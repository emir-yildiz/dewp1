"""Dewp ETL Component"""
import logging
from datetime import datetime
from typing import NamedTuple

import pandas as pd

from dewp.common.s3 import S3BucketConnector

class DewpSourceConfig(NamedTuple):
    """
    Class for source configuration data
    
    src_columns: source column names
    src_col_year: column name for date in source
    src_col_industry_aggregation_knzsioc: column name for industry_aggregation_knzsioc in source
    src_col_industry_code_nzsioc: column name for industry_code_nzsio in source
    src_col_value: column name for value in source
    src_col_year_filter: year column filter in source
    src_files_prefix: source files prefix name
    src_format: file format of the target file
    """
    src_columns: list
    src_col_year: str
    src_col_industry_aggregation_knzsioc: str
    src_col_industry_code_nzsioc: str
    src_col_value: str
    src_col_year_filter: str
    src_files_prefix: str
    src_format:str


class DewpTargetConfig(NamedTuple):
    """
    Class for target configuration data
    
    trg_columns: target column names
    trg_col_year: column name for year in target
    trg_col_industry_aggregation_knzsioc: column name for industry_aggregation_knzsioc in target
    trg_col_industry_code_nzsioc: column name for industry_code_nzsioc in target
    trg_col_min value: column name for minimum value in target
    trg_col_max_value: column name for maximum value in target
    trg_key: basic key of target file
    trg_key_date_format: date format of target file key
    trg_format: file format of the target file
    """
    trg_columns: list
    trg_col_year: str
    trg_col_industry_aggregation_knzsioc: str
    trg_col_industry_code_nzsioc: str
    trg_col_min_value: str
    trg_col_max_value: str
    trg_key: str
    trg_key_date_format: str
    trg_format: str

class DewpETL():
    """
    Reads the Dewp data, transforms and writes the transformed to target
    """

    def __init__(self, s3_bucket_src: S3BucketConnector,
                 s3_bucket_trg: S3BucketConnector,
                 src_args: DewpSourceConfig, trg_args: DewpTargetConfig):
        """
        Constructor for DewpTransformer

        :param s3_bucket_src: connection to source S3 bucket
        :param s3_bucket_trg: connection to target S3 bucket
        :param meta_key: used as self.meta_key -> key of meta file
        :param src_args: NamedTouple class with source configuration data
        :param trg_args: NamedTouple class with target configuration data
        """
        self._logger = logging.getLogger(__name__)
        self.s3_bucket_src = s3_bucket_src
        self.s3_bucket_trg = s3_bucket_trg
        self.src_args = src_args
        self.trg_args = trg_args

    def extract(self):
        """
        Read the source data and concatenates them to one Pandas DataFrame

        :returns:
          data_frame: Pandas DataFrame with the extracted data
        """
        self._logger.info('Extracting Dewp source files started...')
        files = self.s3_bucket_src.list_files_in_prefix(self.src_args.src_files_prefix)
        if not files:
            data_frame = pd.DataFrame()
        else:
            data_frame = pd.concat([self.s3_bucket_src.read_csv_to_df(file)\
                for file in files], ignore_index=True)
        self._logger.info('Extracting Dewp source files finished.')
        return data_frame

    def transform_report1(self, data_frame: pd.DataFrame):
        """
        Applies the necessary transformation to create report 1

        :param data_frame: Pandas DataFrame as Input

        :returns:
          data_frame: Transformed Pandas DataFrame as Output
        """
        if data_frame.empty:
            self._logger.info('The dataframe is empty. No transformations will be applied.')
            return data_frame
        self._logger.info('Applying transformations to Dewp source data for report 1 started...')
        # Filtering necessary source columns
        data_frame = data_frame.loc[:, self.src_args.src_columns]
        # Changing Non-Numeric values in Value column to 0
        data_frame[self.src_args.src_col_value]=pd.to_numeric(data_frame[self.src_args.src_col_value],errors='coerce')
        data_frame[self.src_args.src_col_value]=data_frame[self.src_args.src_col_value].fillna(0)
        # Filtering data by year>2015
        data_frame=data_frame.query(self.src_args.src_col_year+'>='+self.src_args.src_col_year_filter)
        # Aggregating per year, per Industry_aggregation_NZSIOC and Industry_code_NZSIOC
        # > min value and max value
        data_frame=data_frame.groupby([self.src_args.src_col_year,self.src_args.src_col_industry_aggregation_knzsioc,\
                       self.src_args.src_col_industry_code_nzsioc],as_index=False)\
                        .agg(min_value=(self.src_args.src_col_value,'min'),\
                             max_value=(self.src_args.src_col_value,'max'))
        # Renaming columns
        data_frame.rename(columns={
            'min_value': self.trg_args.trg_col_min_value,
            'max_value': self.trg_args.trg_col_max_value
            }, inplace=True)
        self._logger.info('Applying transformations to Dewp source data finished...')
        return data_frame

    def load(self, data_frame: pd.DataFrame):
        """
        Saves a Pandas DataFrame to the target

        :param data_frame: Pandas DataFrame as Input
        """
        # Creating target key
        target_key = (
            f'{self.trg_args.trg_key}'
            f'{datetime.today().strftime(self.trg_args.trg_key_date_format)}.'
            f'{self.trg_args.trg_format}'
        )
        # Writing to target
        self.s3_bucket_trg.write_df_to_s3(data_frame, target_key, self.trg_args.trg_format)
        self._logger.info('Dewp target data successfully written.')
        # Updating meta file
        return True

    def etl_report1(self):
        """
        Extract, transform and load to create report 1
        """
        # Extraction
        data_frame = self.extract()
        # Transformation
        data_frame = self.transform_report1(data_frame)
        # Load
        self.load(data_frame)
        return True
