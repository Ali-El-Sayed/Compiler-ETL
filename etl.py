from codegenerator import *
import json


CONFIGS = {
    'log': False,
    'print': False,
    'path': './sqlp.log'
}


LOGING = False

# hold all the datasources types that the app can handel
DATASOURCES = {
    'files': ['csv', 'tsv', 'json', 'html', 'xml', 'excel', 'hdf', 'feather', 'parquet', 'stata', 'pickle'],
    'dbms': ['mysql', 'postgres', 'sqlite', 'mssql']
}

# map each type to the correct class
DATASOURCES_MAP = {
    'csv': lambda x: CSVDS(x),
    'tsv': lambda x: TSVDS(x),
    'json': lambda x: JSONDS(x),
    'html': lambda x: HTMLDS(x),
    'xml': lambda x: XMLDS(x),
    'excel': lambda x: ExcelDS(x),
    'hdf': lambda x: HDFDS(x),
    'feather': lambda x: FeatherDS(x),
    'parquet': lambda x: ParquetDS(x),
    'stata': lambda x: StataDS(x),
    'pickle': lambda x: PickleDS(x),

    'mysql': lambda x: MysqlDS(x),
    'postgres': lambda x: PostgresDS(x),
    'sqlite': lambda x: sqliteDS(x),
    'mssql': lambda x: MssqlDS(x),
}


def new_data_source(connection_string):
    features = connection_string.split(';')
    if len(features) == 1:
        raise Exception('missing datasource type')
    ds_type = features.pop(0).lower()
    ds_path = features[0]

    # if the datasource is a database
    # then we have to check for database name and the schema
    if ds_type in DATASOURCES['dbms']:
        ds_path = ';'.join(features)

    return DATASOURCES_MAP[ds_type](ds_path)


EVENTS_MAP = """EVENTS_MAP = {
    'load': lambda df_name: f'loading {df_name}',
    'sort': lambda df_name: f'sorting {df_name}',
    'group': lambda df_name: f'grouping {df_name}',
    'merge': lambda df_name: f'merging {df_name}',
    'select': lambda df_name: f'retriving columns {df_name}',
    'distinct': lambda df_name: f'remove duplicated columns {df_name}',
    'agg': lambda df_name: f'aggregation functions {df_name}',
    'save': lambda df_name: f'saving all changes of the data {df_name}',
    'insert': lambda df_name: f'insert new data {df_name}',
    'delete': lambda df_name: f'delete data {df_name}',
    'update': lambda df_name: f'update data {df_name}',

}"""


def log():
    code = ['from timeit import default_timer as timer',
            'from datetime import timedelta']
    code += [f'{EVENTS_MAP}']
    code += ['def sqlp_log(type, df_name, event, start_time, end_time = None):']
    if CONFIGS['print']:
        code += ['\t\tprint(EVENTS_MAP[event](df_name), timedelta(seconds=end_time - start_time))']
    if CONFIGS['log']:
        code += [f'\t\tlog_file = open({CONFIGS["path"]}, "a")']
        code += [
            '\t\tlog_file.write(f"{EVENTS_MAP[event](df_name)}, {timedelta(seconds= end_time - start_time)}\\n")']
        code += ['\t\tlog_file.close()']

    return code


def start_event():
    return [f'_start_time = timer()']


def end_event(df_name, event):
    return [f'_end_time = timer()'] + [f'sqlp_log("end", "{df_name}", "{event}", _start_time, _end_time)']


def select(select_info):
    ds = None
    pandas_code = []

    # check if the datasource is normal file or nested query
    if isinstance(select_info['ds']['src'], dict):
        ds, pandas_code = select(
            select_info['ds']['src']['statement_parser'])
    else:
        ds = new_data_source(select_info['ds']['src'][2:-2])

        pandas_code += start_event() if LOGING else []
        pandas_code += ds.open(select_info['ds'].get('name', ''))
        pandas_code += end_event(ds.df_name, 'load') if LOGING else []

    # list of all targeted columns
    columns_names_list = ds.columns_names(select_info['columns_list'])
    # list of all renaming of the columns
    columns_alias_list = ds.columns_alias(select_info['columns_list'])
    columns_agg_list = ds.columns_agg(select_info['columns_list'])

    # perform select (retrive diseared columns with the given conditions)
    pandas_code += start_event() if LOGING else []
    pandas_code += ds.select(columns_names_list, select_info['conditions'])
    pandas_code += end_event(ds.df_name, 'select') if LOGING else []

    # remove duplicate columns
    if select_info.get('distinct', None):
        pandas_code += start_event() if LOGING else []
        pandas_code += ds.unique()
        pandas_code += end_event(ds.df_name,
                                 'distinct') if LOGING else []

    # sort columns
    if select_info.get('order', None):
        pandas_code += start_event() if LOGING else []
        pandas_code += ds.order(select_info['order'])
        pandas_code += end_event(ds.df_name, 'sort') if LOGING else []

    if select_info['group']:
        # grouping results
        pandas_code += start_event() if LOGING else []
        pandas_code += ds.group(select_info['group'])
        pandas_code += end_event(ds.df_name, 'group') if LOGING else []

        # perform aggregation function
        pandas_code += start_event() if LOGING else []
        pandas_code += ds.agg(columns_agg_list)
        pandas_code += end_event(ds.df_name, 'agg') if LOGING else []

    # slice result to return only the desired portion
    pandas_code += ds.slice(select_info['offset'], select_info['limit'])

    # finally rename all the columns
    # pandas_code += ds.rename(columns_alias_list)

    return [ds, pandas_code]


def select_join(select_info):

    main_df = None
    pandas_code = []

    # check if the datasource is normal file or nested query
    if isinstance(select_info['ds']['src'], dict):
        main_df, pandas_code = ETL.select(
            select_info['ds']['src']['statement_parser'])
    else:
        main_df = new_data_source(
            select_info['ds']['src'][2:-2])

        pandas_code += start_event() if LOGING else []
        pandas_code += main_df.open(select_info['ds'].get('name', ''))
        pandas_code += end_event(main_df.df_name, 'load') if LOGING else []

    columns_names_list = main_df.columns_names(select_info['columns_list'])
    columns_alias_list = main_df.columns_alias(select_info['columns_list'])
    columns_agg_list = main_df.columns_agg(select_info['columns_list'])

    for join in select_info['joins']:
        joined_df = ''

        # check if the datasource is normal file or nested query
        if isinstance(join['ds']['src'], dict):
            joined_df, code = select(
                join['ds']['src']['statement_parser'])
            pandas_code += code
        else:
            joined_df = new_data_source(
                join['ds']['src'][2:-2])

            pandas_code += start_event() if LOGING else []
            pandas_code += joined_df.open(join['ds'].get('name', ''))
            pandas_code += end_event(joined_df.df_name,
                                     'load') if LOGING else []

        pandas_code += start_event() if LOGING else []
        pandas_code += main_df.join(joined_df.df_name,
                                    join['join_type'], join['conditions'])
        pandas_code += end_event(joined_df.df_name,
                                 'join') if LOGING else []

    # select wanted columns
    pandas_code += start_event() if LOGING else []
    pandas_code += main_df.select(columns_names_list,
                                  select_info['conditions'])
    pandas_code += end_event(main_df.df_name, 'select') if LOGING else []

    # remove duplicates
    if select_info.get('distinct', None):
        pandas_code += start_event() if LOGING else []
        pandas_code += main_df.unique()
        pandas_code += end_event(main_df.df_name,
                                 'distinct') if LOGING else []

    # sort
    if select_info.get('order', None):
        pandas_code += start_event() if LOGING else []
        pandas_code += main_df.order(select_info['order'])
        pandas_code += end_event(main_df.df_name,
                                 'sort') if LOGING else []

    if select_info.get('group', None):
        # group
        pandas_code += start_event() if LOGING else []
        pandas_code += main_df.group(select_info['group'])
        pandas_code += end_event(main_df.df_name,
                                 'group') if LOGING else []

        # aggregation functions
        pandas_code += start_event() if LOGING else []
        pandas_code += main_df.agg(
            columns_agg_list) if select_info['group'] else []
        pandas_code += end_event(main_df.df_name,
                                 'agg') if LOGING else []

    return [main_df, pandas_code]


def select_into(select_info):

    pandas_code = []
    from_ds = None

    # check if the datasource is normal file or nested query
    if isinstance(select_info['ds2']['src'], dict):
        from_ds, pandas_code = select(
            select_info['ds2']['src']['statement_parser'])
    else:
        from_ds = new_data_source(
            select_info['ds2']['src'][2:-2])

        # load data source
        pandas_code += start_event() if LOGING else []
        pandas_code += from_ds.open(select_info['ds2'].get('name', ''))
        pandas_code += end_event(from_ds.df_name, 'load') if LOGING else []

    columns_names_list = from_ds.columns_names(select_info['columns_list'])

    # select columns
    pandas_code += start_event() if LOGING else []
    pandas_code += from_ds.select(columns_names_list,
                                  select_info['conditions'])
    pandas_code += end_event(from_ds.df_name, 'select') if LOGING else []

    # slice the result
    pandas_code += from_ds.slice(select_info['offset'], select_info['limit'])

    # create object of the second datasource
    to_ds = new_data_source(select_info['ds1']['src'][2:-2])

    # load datasource
    pandas_code += start_event() if LOGING else []
    pandas_code += to_ds.open(select_info['ds1'].get('name', ''))
    pandas_code += end_event(to_ds.df_name, 'load') if LOGING else []

    # insert rows
    pandas_code += start_event() if LOGING else []
    pandas_code += to_ds.insert_select(from_ds.df_name, None)
    pandas_code += end_event(to_ds.df_name, 'insert') if LOGING else []

    # save the file
    pandas_code += start_event() if LOGING else []
    pandas_code += to_ds.close(type='insert')
    pandas_code += end_event(to_ds.df_name, 'save') if LOGING else []

    return [to_ds, pandas_code]


def insert(insert_info):
    pandas_code = []
    ds = new_data_source(insert_info['ds']['src'][2:-2])

    # load datasource
    pandas_code += start_event() if LOGING else []
    pandas_code += ds.open(insert_info['ds'].get('name', ''))
    pandas_code += end_event(ds.df_name, 'load') if LOGING else []

    # insert rows
    pandas_code += start_event() if LOGING else []
    pandas_code += ds.insert(insert_info['rows'],
                             insert_info.get('columns_list', None))
    pandas_code += end_event(ds.df_name, 'insert') if LOGING else []

    # save changes
    pandas_code += start_event() if LOGING else []
    pandas_code += ds.close(type='insert')
    pandas_code += end_event(ds.df_name, 'save') if LOGING else []

    return [ds, pandas_code]


def insert_select(insert_info):

    ds = new_data_source(insert_info['ds']['src'][2:-2])
    pandas_code = []
    if not insert_info['select_statement']['statement_parser']['ds'].get('name', ''):
        insert_info['select_statement']['statement_parser']['ds'][
            'name'] = f'df_{next(tempfile._get_candidate_names())}'

    # select the wanted columns
    pandas_code += select(insert_info['select_statement']
                          ['statement_parser'])[1]

    # load datasource
    pandas_code += start_event() if LOGING else []
    pandas_code += ds.open(insert_info['ds'].get('name', ''))
    pandas_code += end_event(ds.df_name, 'load') if LOGING else []
    # insert rows
    pandas_code += start_event() if LOGING else []
    pandas_code += ds.insert_select(insert_info['select_statement']['statement_parser']
                                    ['ds']['name'], insert_info.get('columns_list', None))
    pandas_code += end_event(ds.df_name, 'insert') if LOGING else []

    # save changes to file
    pandas_code += start_event() if LOGING else []
    pandas_code += ds.close(type='insert')
    pandas_code += end_event(ds.df_name, 'save') if LOGING else []

    return [ds, pandas_code]


def delete(delete_info):

    ds = new_data_source(delete_info['ds']['src'][2:-2])
    pandas_code = []

    # load data
    pandas_code += start_event() if LOGING else []
    pandas_code += ds.open(delete_info['ds'].get('name', ''))
    pandas_code += end_event(ds.df_name, 'load') if LOGING else []

    # delete
    pandas_code += start_event() if LOGING else []
    pandas_code += ds.delete(delete_info['conditions'])
    pandas_code += end_event(ds.df_name, 'delete') if LOGING else []

    # save changes
    pandas_code += start_event() if LOGING else []
    pandas_code += ds.close(type='delete')
    pandas_code += end_event(ds.df_name, 'save') if LOGING else []

    return [ds, pandas_code]


def update(update_info):

    ds = new_data_source(update_info['ds']['src'][2:-2])
    pandas_code = []

    # load
    pandas_code += start_event() if LOGING else []
    pandas_code += ds.open(update_info['ds'].get('name', ''))
    pandas_code += end_event(ds.df_name, 'load') if LOGING else []

    # update
    pandas_code += start_event() if LOGING else []
    pandas_code += ds.update(update_info['conditions'],
                             update_info['rec_set'])
    pandas_code += end_event(ds.df_name, 'update') if LOGING else []

    # save
    pandas_code += start_event() if LOGING else []
    pandas_code += ds.close(type='update')
    pandas_code += end_event(ds.df_name, 'save') if LOGING else []

    return [ds, pandas_code]
