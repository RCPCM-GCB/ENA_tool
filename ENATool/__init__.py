from .extract_samples_info import get_samples_info_by_ena_project_id
from .safe_samples_downloader import download_samples

import pandas as pd
import os

@pd.api.extensions.register_dataframe_accessor("ena")
class ENATool:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj
        self.table_type = None
        if all(column in self._obj.columns for column in ['SAMPLE__IDENTIFIERS__PRIMARY_ID', 'ENA-FASTQ-FILES']):
            self.table_type = 'raw'
        if all(column in self._obj.columns for column in ['sample_name', 'url', 'md5sum', 'filepath']):
            self.table_type = 'ready'
        self.id = None
        self.path = None
     
    def download(self):
        '''
        Download fastq files
        '''
        if self.id != None:
            if self.table_type == 'raw':
                report_table = download_samples(self.id, ena_sample_info_table = self._obj,
                                               destination_folder = self.path)
                report_table.ena.id = self.id
                report_table.ena.path = self.path
                return report_table
            if  self.table_type == 'ready':                 
                report_table = download_samples(self.id, downoad_info_table = self._obj)
                report_table.ena.id = self.id
                report_table.ena.path = self.path
                return report_table
        else:
            raise ValueError('''PandasDataFrame.ena.id is None. It seems like you forgot to reinitialize your table with the existing features: 
        new_table.ena.reinitialize(table) 
                    or 
        new_table.ena.id = project_id
        new_table.ena.path = path''')
            
    def reinit(self, obj):
        '''
        Reinitialize Ena tool features of pandas table
        Features are: id, path
        '''
        self.id = obj.ena.id
        self.path = obj.ena.path        

        
def fetch(project_id, path = None, download = False):
    '''
    Fetches all metadata from ENA browser for the correspondinf project ID and also can download all raw files.
    
    project_id: accession ID, ex. PRJEB35665
    path: a path where all the data will be stored, by default it creates the %project_id% folder
    download: if TRUE, automatically downloads all raw files from dataset.
              if FALSE (default) you can make any changes to the output table and then use table.ena.download()
    '''
    if path is None:
        path = project_id
    path = os.path.abspath(path)
    info_table = get_samples_info_by_ena_project_id(project_id, path)
    info_table.ena.id = project_id
    info_table.ena.path = path
    if download:
        return info_table, info_table.download()
    return info_table