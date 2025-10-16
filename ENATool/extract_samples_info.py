import pandas as pd
import numpy as np
import os
try:
    _in_ipython_session = __IPYTHON__
    from tqdm import tqdm_notebook as tq
    no_progress_bar=False
except NameError:
    from tqdm import tqdm as tq
    no_progress_bar=True
import xml.etree.cElementTree as et
import requests
try:
    from pandas.io.parsers.base_parser import ParserBase
except:
    from pandas.io.parsers import ParserBase



  
####### EXAMPLE #######  
#  folder='xml_files'
#  samples_table = get_samples_info_by_ena_project_id('PRJNA335681', folder=folder)
#######################

def check_no_progress_bar():
    return no_progress_bar

def download_samples_file(project_id, folder = ''):
#     url = "https://www.ebi.ac.uk/ena/portal/api/filereport?accession=%s&result=read_run&fields=study_accession,secondary_study_accession,sample_accession,secondary_sample_accession,experiment_accession,run_accession,submission_accession,tax_id,scientific_name,instrument_platform,instrument_model,library_name,nominal_length,library_layout,library_strategy,library_source,library_selection,read_count,base_count,center_name,first_public,last_updated,experiment_title,study_title,study_alias,experiment_alias,run_alias,fastq_bytes,fastq_md5,fastq_ftp,fastq_aspera,fastq_galaxy,submitted_bytes,submitted_md5,submitted_ftp,submitted_aspera,submitted_galaxy,submitted_format,sra_bytes,sra_md5,sra_ftp,sra_aspera,sra_galaxy,cram_index_ftp,cram_index_aspera,cram_index_galaxy,sample_alias,broker_name,sample_title,nominal_sdev,first_created&format=tsv&download=true"%project_id
    url = "https://www.ebi.ac.uk/ena/portal/api/filereport?accession=%s&result=read_run&fields=study_accession,secondary_study_accession,sample_accession,secondary_sample_accession,experiment_accession,run_accession,submission_accession,tax_id,scientific_name,instrument_platform,instrument_model,library_name,nominal_length,library_layout,library_strategy,library_source,library_selection,read_count,base_count,center_name,first_public,last_updated,experiment_title,study_title,study_alias,experiment_alias,run_alias,fastq_bytes,fastq_md5,fastq_ftp,fastq_aspera,fastq_galaxy,submitted_bytes,submitted_md5,submitted_ftp,submitted_aspera,submitted_galaxy,submitted_format,sra_bytes,sra_md5,sra_ftp,sra_aspera,sra_galaxy,sample_alias,broker_name,sample_title,nominal_sdev,first_created&format=tsv&download=true"%project_id
    r = requests.get(url, allow_redirects=True)
    filename = '%s_samples_info.tsv'%project_id
    filepath = (folder+'/'+filename).replace('//', '/') if folder != '' else filename
    if folder != '' and not os.path.isdir(folder):
        os.makedirs(folder)
    if not os.path.isdir(os.path.dirname(filepath)):
        os.makedirs(os.path.dirname(filepath))
    with open(filepath, 'wb') as f:
        f.write(r.content)
    table = pd.read_csv(filepath, sep='\t')
    return table

def download_file(url, filename=None, folder=''):
    r = requests.get(url, allow_redirects=True)
    filename = filename if filename is not None else url.split('/')[-1].replace('?download=true', '')+'.xml'
    filepath = (folder+'/xml_files/'+filename).replace('//', '/') if folder != '' else 'xml_files/'+filename
    if folder != '' and not os.path.isdir(folder):
        os.makedirs(folder)
    if not os.path.isdir(os.path.dirname(filepath)):
        os.makedirs(os.path.dirname(filepath))
    with open(filepath, 'wb') as f:
        f.write(r.content)
    return filepath

def get_files_form_links(links, folder=''):
    return [download_file(link, folder=folder) for link in links]


def generate_prelimenary_table(filenames):
    tables_list = []
    column_pairs = {}
    for file in filenames:
        parsed_xml = et.parse(file)
        table = []
        col_prefix = ''
        col_prefix_ = ''
        attr_dict = {}
        help_df = pd.DataFrame()
        i = -1
        flag = 0
        base_name = ''
        prev_col = ''
        for elem in parsed_xml.iter():
            if 'SET' in elem.tag:
                base_name = elem.tag[:-4]
            if elem.tag == base_name:
                i += 1
                flag = 0
                col_prefix = ''
                attr_dict = {}
                table.append(help_df.copy())
                help_df = pd.DataFrame()
                prev_col = ''
            if elem.text is not None:
                if elem.text.strip() == '':
                    if flag:
                        col_prefix = ''
                        flag = 0
                    col_prefix = col_prefix + '__' + elem.tag
                else: 
                    col_prefix_ = col_prefix[2:] + '__' + elem.tag
                    for key in attr_dict.keys():
                            if key.endswith(col_prefix_) and (col_prefix_ not in attr_dict):
                                attr_dict[col_prefix_] = attr_dict.pop(key)
                                help_df = help_df.rename(columns={key:col_prefix_})
                                if key in column_pairs:
                                    part_to_del = key.replace(col_prefix_, '')
                                    column_pairs[col_prefix_] = column_pairs[key].replace(part_to_del, '')
                                break
                    if col_prefix_ in attr_dict:
                        attr_dict[col_prefix_] = attr_dict[col_prefix_]+1
                        col_prefix_ = col_prefix_+'__'+str(attr_dict[col_prefix_])
                    else:
                        attr_dict[col_prefix_] = 0

                    help_df.loc[i, col_prefix_] = elem.text.strip()
                    len_s = 2 if col_prefix_.split('__')[-1].isdigit() else 1 
                    if flag and col_prefix_.split('__')[:-len_s] == prev_col.split('__')[:-len_s]:
                        column_pairs[prev_col] = col_prefix_
                    flag = 1
                    prev_col = col_prefix_
                    
        table.append(help_df.copy())
        tables_list.append(pd.concat(table))
    table_all = pd.concat(tables_list)
    return table_all, column_pairs, attr_dict


def fix_table(table_all, column_pairs, attr_dict):
    ##### find repeated columns, delete & rename them
    table_final = []
    base_cols = [column for column, iteration in attr_dict.items() if iteration <= 1]
    for i, row in table_all.iterrows():
        column_names = [x for x, y in column_pairs.items() if ((x in row.index.values) & (y in row.index.values) & 
                                                              (x not in base_cols) & (y not in base_cols))]
        data_values = [y for x, y in column_pairs.items() if ((x in row.index.values) & (y in row.index.values) & 
                                                             (x not in base_cols) & (y not in base_cols))]
        base_cols_ = [x for x in base_cols if x in row.index.values]
        column_names = np.array(base_cols_ + row[column_names].values.tolist())
        data_values = base_cols_ + data_values
        data_values = row[data_values].values
        not_nan_inds = (column_names == column_names) & (column_names != 'nan')
        column_names = column_names[not_nan_inds]
        data_values = data_values[not_nan_inds]   
        table_final.append(pd.DataFrame([data_values], columns=column_names))
    table_final = pd.concat(table_final)
    deduplicated_columns = ParserBase({'names':table_final.columns,
                                       'usecols':table_final.columns})._maybe_dedup_names(table_final.columns)
    table_final.columns = deduplicated_columns
    return table_final


def extract_xml_from_urls(links, folder=''):
    if type(links) == str:
        links = [links]
    filenames = get_files_form_links(links, folder)
    table_all, column_pairs, attr_dict = generate_prelimenary_table(filenames)
    return fix_table(table_all, column_pairs, attr_dict)

def process_study_table(table, folder):
    try:
        samples = table.loc[0, 'ENA-SAMPLE']
        samples = samples.split(',')    
        all_samples_tables = [extract_xml_from_urls('https://www.ebi.ac.uk/ena/browser/api/xml/%s?download=true'%sample, 
                                                    folder)
                          for sample in tq(samples, desc='Processing project pages', disable=no_progress_bar)]
        return pd.concat(all_samples_tables)
    except:
        return None

def make_html(table):
    template_header = '''<html><head>
<meta http-equiv="Content-type" content="text/html; charset=utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,user-scalable=no">
<title>DataTables example - Select integration - export selected rows</title>
<link rel="alternate" type="application/rss+xml" title="RSS 2.0" href="http://www.datatables.net/rss.xml">
<link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.10.22/css/jquery.dataTables.min.css">
<link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/buttons/1.6.4/css/buttons.dataTables.min.css">
<link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/select/1.3.1/css/select.dataTables.min.css">
<link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/searchbuilder/1.0.0/css/searchBuilder.dataTables.min.css">
<style type="text/css" class="init">

</style>
<script type="text/javascript" language="javascript" src="https://code.jquery.com/jquery-3.5.1.js"></script>
<script type="text/javascript" language="javascript" src="https://cdn.datatables.net/1.10.22/js/jquery.dataTables.min.js"></script>
<script type="text/javascript" language="javascript" src="https://cdn.datatables.net/buttons/1.6.4/js/dataTables.buttons.min.js"></script>
<script type="text/javascript" language="javascript" src="https://cdn.datatables.net/buttons/1.6.4/js/buttons.flash.min.js"></script>
<script type="text/javascript" language="javascript" src="https://cdnjs.cloudflare.com/ajax/libs/jszip/3.1.3/jszip.min.js"></script>
<script type="text/javascript" language="javascript" src="https://cdnjs.cloudflare.com/ajax/libs/pdfmake/0.1.53/pdfmake.min.js"></script>
<script type="text/javascript" language="javascript" src="https://cdnjs.cloudflare.com/ajax/libs/pdfmake/0.1.53/vfs_fonts.js"></script>
<script type="text/javascript" language="javascript" src="https://cdn.datatables.net/buttons/1.6.4/js/buttons.html5.min.js"></script>
<script type="text/javascript" language="javascript" src="https://cdn.datatables.net/buttons/1.6.4/js/buttons.print.min.js"></script>
<script type="text/javascript" language="javascript" src="https://cdn.datatables.net/select/1.3.1/js/dataTables.select.min.js"></script>
<script type="text/javascript" language="javascript" src="https://cdn.datatables.net/searchbuilder/1.0.0/js/dataTables.searchBuilder.min.js"></script>
<script type="text/javascript">
$(document).ready(function() {
    $('#filter_table').DataTable( {
        scrollX:        '80vw',
        scrollY:        '60vh',
        scrollCollapse: true,
        scrollX: true,
        paging:         false,
        dom: 'BQfrtip',
        deferRender: true,
        columns: %s,
        buttons: [
            'copy',
            'csv'
        ],
        searchBuilder: {
            greyscale: true
        }
    } );
} );
</script>
</head>
<body>    
    '''
    template_footer = '''
</body>
</html>
    '''
    columns_string = ['{data:"%s"}'%col for col in table.columns.values]
    columns_string = '['+', '.join(columns_string)+']'
    table_html = template_header%columns_string+\
table.to_html(index=False).replace('<table border="1" class="dataframe">', '<table id="filter_table" class="display nowrap" style="width:100%">')+\
template_footer
    return table_html

def correct_columns(table):
    uni, counts = np.unique(table.columns.values, return_counts=True)
    duplicated_columns = uni[counts > 1]
    
    def increment(column):
        iter_ = 1
        column = column
        while True:
            if iter_ == 1:
                yield column
            else:
                yield column + '_%d'%iter_
            iter_ += 1
    
    increment_gen = {col:increment(col)
                     for col in table.columns.values
                     if col in duplicated_columns}
    table.columns = [next(increment_gen[col]) if col in increment_gen.keys() else col 
                     for col in table.columns.values]
    return table

def get_ncbi_info(sample_accessions):
    sample_info_table = []
    for sample_accession in tq(sample_accessions, desc='Getting NCBI Info', disable=no_progress_bar):
        sample_info = pd.read_html("https://www.ncbi.nlm.nih.gov/biosample/%s"%sample_accession, 
                                   index_col=0)[0].transpose()
        sample_info.index = [sample_accession]
        sample_info_table.append(correct_columns(sample_info))
    return pd.concat(sample_info_table)
    

def get_samples_info_by_ena_project_id(id_, folder='', save_table=True, return_table=True, return_html = False, return_path=False, sep='\t'):
    study_table = extract_xml_from_urls('https://www.ebi.ac.uk/ena/browser/api/xml/%s'%id_, folder=folder)
    samples_table = process_study_table(study_table, folder)
    samples_info = download_samples_file(id_, folder = folder)    
    samples_info = correct_columns(samples_info)
    if samples_table is not None:
        samples_table = correct_columns(samples_table)
        samples_table.index = samples_table['SAMPLE__IDENTIFIERS__EXTERNAL_ID'].values
    else:
        samples_table = get_ncbi_info(samples_info['sample_accession'].values)
    samples_info.index = samples_info['sample_accession'].values
    #other_rows = list(set(samples_table.index.values).difference(set(samples_info.index.values)))
    #samples_table2 = pd.DataFrame(index=samples_info.index.values.tolist()+other_rows, 
    #                              columns=list(samples_table.columns)+list(samples_info.columns))
# Correction!!!!
    if np.unique(samples_info.index.values).shape[0] != samples_info.shape[0]:
        samples_table2 = pd.concat([samples_info, samples_table])
    else:
        samples_table2 = pd.concat([samples_info, samples_table], axis=1)
    #samples_table2.loc[samples_table.index.values, samples_table.columns.values] = samples_table.values
    #samples_table2.loc[~samples_table2.index.isin(other_rows), samples_info.columns.values] = samples_info.values
    filename = id_+'.csv'
    filepath = (folder+'/'+filename).replace('//', '/') if folder != '' else filename
    if save_table:
        samples_table2.to_csv(filepath, index=False, sep=sep)
        samples_table2.to_html(filepath.replace('.csv', '.html'), index=False)
        with open(filepath.replace('.csv', '.html'), 'w') as f:
            f.write(make_html(samples_table2))
    to_return = []
    if return_table:
        to_return.append(samples_table2)
    if return_html:
        to_return.append(make_html(samples_table2))
    if return_path:    
        to_return.append(filepath.replace('.csv', '.html'))
    if len(to_return) == 1:
        return to_return[0]
    else:
        return to_return
