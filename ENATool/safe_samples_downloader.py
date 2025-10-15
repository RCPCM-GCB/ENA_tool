import os
import subprocess
import pandas as pd
import requests
import io
# from tqdm import tqdm as tq
from tqdm import tqdm_notebook as tq
from time import sleep
import warnings

# no_progress_bar=True
no_progress_bar=False

class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class InputError(Error):
    """Exception raised for errors in the input.

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
        

class InfoError(Error):
    """Exception raised for errors in the input.

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
        

def download_samples(project_id, ena_sample_info_table=None, downoad_info_table=None, 
                            destination_folder = None):
    '''
    ena_sample_info_table: a table returned by get_samples_info_by_ena_project_id function,
                             mandatory clumns are ['sample_accession', 'run_accession', 'fastq_ftp']
    downoad_info_table: a table with columns ['sample_name', 'url', 'md5sum', 'filepath'], 
                        has a priority above the ena_sample_info_table
    
    return:
          downoad_info_table: a table with columns ['sample_name', 'url', 'md5sum', 'filepath', 'number_of_files', 'download_status']
                              if this table was not provided by user, 'sample_name' is taken from  
                              ena_sample_info_table['sample_accession']
    '''
    if destination_folder is None:
        destination_folder = "{pwd}/{project_id}/input_data".format(
                                                                pwd = os.getcwd(),
                                                                project_id = project_id)
    else:
        destination_folder = '%s/input_data'%destination_folder
        
    if (downoad_info_table is None) and (ena_sample_info_table is not None):
        downoad_info_table = pd.DataFrame(columns=['sample_name', 'accession', 'filepath', 'number_of_files'])
        for name, accession, fastq_ftp in tq(ena_sample_info_table[['sample_accession', 'run_accession', 'fastq_ftp']].values, disable=True):
            if fastq_ftp == fastq_ftp:
                number_of_files = fastq_ftp.count(';')+1
            else:
                number_of_files = 0
            accession = accession
            link = fastq_ftp.split(';') if number_of_files > 1 else fastq_ftp
            destination_file = None if number_of_files == 0 else\
                               '%s/%s/%s'%(destination_folder, accession, link.split('/')[-1]) if number_of_files == 1 else\
                               ['%s/%s/%s'%(destination_folder, accession, link_.split('/')[-1]) for link_ in link]
            downoad_info_table.loc[len(downoad_info_table)] = [name, accession, destination_file, number_of_files] 
        if not os.path.isdir(os.path.dirname(destination_folder)):
            os.makedirs(os.path.dirname(destination_folder))
        downoad_info_table.to_csv('%s/downoad_info_table.csv'%os.path.dirname(destination_folder), index=None, sep='\t')
    elif (downoad_info_table is None) and (ena_sample_info_table is None):
        raise InputError('You should provide one of the tables: either ena_sample_info_table or downoad_info_table')
        
    download_status = [download_and_check_data(project_id, accession, eval(str(destination_path)), number_of_files)
                       if number_of_files > 1
                       else download_and_check_data(project_id, accession, [destination_path], number_of_files)
                       if number_of_files > 0  else None
                      for accession, destination_path, number_of_files in tq(downoad_info_table[[
                          'accession', 'filepath', 'number_of_files']].values, desc='Downloading fastq', disable=no_progress_bar)]
    downoad_info_table['download_status'] = download_status
    downoad_info_table.to_csv('%s/downoad_info_table.csv'%os.path.dirname(destination_folder), index=None, sep='\t')
    return downoad_info_table      



def download_and_check_data(project_id, accession, 
                            destination_path, 
                            number_of_files):
    
    destination_folder = destination_path[0].split(accession)[0]
    if not os.path.isdir(destination_folder):
        os.makedirs(destination_folder)
    
    res_status = []
    for file in destination_path:        
        if os.path.exists(file):  
            res_status.append('Exists')
    if res_status == ['Exists']*number_of_files:
        if number_of_files == 1:
            return res_status[0]
        else:
            return res_status
    print('enaDataGet -f fastq -d "{destination_folder}" {accession}'.format(
    destination_folder=destination_folder, accession=accession))
    enatool_output = subprocess.call('enaDataGet -f fastq -d "{destination_folder}" {accession}'.format(
    destination_folder=destination_folder, accession=accession), shell=True)
    
    res_status = []
    for file in destination_path:
        if not os.path.exists(file):  
            print('ERROR')
            res_status.append('Error')
        else:
            res_status.append('OK')
    if number_of_files == 1:
        return res_status[0]
    else:
        return res_status