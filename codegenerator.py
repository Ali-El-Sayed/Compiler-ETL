import tempfile


# the main class that all the files will inherit from
#
class DataSource():
    def __init__(self, path):
        self.df_name = None
        self._file_path = path

    # each class have to make its own implementation
    def open(self):
        raise NotImplementedError

    # each class have to make its own implementation
    def close(self, type=None):
        raise NotImplementedError

    def __column_from_index(self, index):
        return f'''{self.df_name}.columns[{index[1:-1]}]'''

    # retrun all columns names without aliases or functions
    def columns_names(self, columns_list):
        columns = []

        for column in columns_list:
            if isinstance(column['column'], dict):
                col = column['column']['column']
            else:
                col = column['column']
            if col not in columns:
                columns.append(f'{col}')

        return columns

    # retrun list of tuples include column name and the desired function for it
    def columns_agg(self, columns_list):
        columns = {}

        for column in columns_list:
            if isinstance(column['column'], dict):
                agg = 'mean' if column['column']['function'] == 'avg' else column['column']['function']
                if columns.get(column['column']['column'], None):
                    columns[column['column']['column']].append(agg)
                else:
                    columns[column['column']['column']] = [
                        column['column']['function']]
        return columns

    def columns_alias(self, columns_list):
        aliases = []

        for column in columns_list:
            if column.get('as'):
                aliases.append(column['as'])
            elif isinstance(column['column'], dict):
                aliases.append(
                    f'''{column['column']['function']}({column['column']['column']})''')
            else:
                aliases.append(column['column'])
        return aliases

    # agg function
    def agg(self, agg_list):
        if len(agg_list) == 0:
            return [f'''{self.df_name} = {self.df_name}.filter(lambda x: True)''']

        agg_dict = '{'

        for column in agg_list:
            agg_dict += f'''\'{column}\':{agg_list[column]},'''
        agg_dict = agg_dict[:-1] + '}'

        return [f'''{self.df_name} = {self.df_name}.agg({agg_dict}).reset_index()''']

    def rename(self, aliases):
        if len(aliases) == 0:
            return []
        elif len(aliases) == 1 and aliases[0] == '*':
            return []

        return [f'''{self.df_name}.columns = {aliases}''']

    def select(self, columns_list, conditions):

        cond = self.__conditions(conditions)
        if len(columns_list) == 1:
            if columns_list[0] == '*':
                columns_list = ''

        tmp_list = []
        for col in columns_list:
            if col.startswith('['):
                tmp_list.append(self.__column_from_index(col))
            else:
                tmp_list.append(f"'{col}'")

        if columns_list != '':
            columns_list = '[' + ', '.join(tmp_list) + ']'

        return [f'''{self.df_name} = {self.df_name}.loc[{':' if not cond else cond}, {columns_list}]''']

    #
    #  example df['likes'] >= 2000000 & df['likes'] <= 20000000
    #
    def __conditions(self, conditions):
        if not conditions:
            return ''

        conditions_str = ''

        for cond in conditions:
            t = cond.get('sd', '')
            # for list union and intersection
            if t:
                t = '|' if t == 'or' else '&'

            left = cond['left']
            if left.startswith('['):
                left = self.__column_from_index(left)
                left = f"{self.df_name}[{left}]"
            else:
                left = f"{self.df_name}['{left}']"

            conditions_str += f'''{t}({left} {cond['op'] if cond['op'] != '=' else '=='} {cond['right']})'''

        return conditions_str

    # sql alternative of group by
    def group(self, column_list):
        if not column_list:
            return []

        t_column_list = []
        for col in column_list:
            t = col
            if t.startswith('['):
                t = self.__column_from_index(t)
                t_column_list.append(t)
            else:
                t_column_list.append(f"'{t}'")

        columns_str = ', '.join(t_column_list)
        return [f'''{self.df_name} = {self.df_name}.groupby([{columns_str}])''']

    # handels sql limit and offset
    def slice(self, offset, limit):
        if not offset and not limit:
            return []

        offset = 0 if not offset else offset
        return [f'''{self.df_name} = {self.df_name}.iloc[{offset}: {'' if not limit else offset + limit}]''']

    # handel sorting
    def order(self, orders):
        if not orders:
            return []

        columns = []
        ascending = []
        for order in orders:
            col = order["column"]
            t = col
            if t.startswith('['):
                t = self.__column_from_index(t)
                columns.append(f'{t}')
            else:
                columns.append(f'\'{t}\'')

            ascending.append(str(order['type'] == 'asc'))

        columns = ', '.join(columns)
        ascending = ', '.join(ascending)

        return [f'''{self.df_name} = {self.df_name}.sort_values([{columns}], ascending=[{ascending}])''']

    def unique(self):
        return [f'''{self.df_name} = {self.df_name}.drop_duplicates()''']

    # join two dataframes
    def join(self, df_name, join_type, conditions):
        left_cond = []
        right_cond = []

        # split the on condition 2 list
        for cond in conditions:
            if cond['left'].startswith(self.df_name):
                left_cond.append(f"'{cond['left'].split('.')[1]}'")
            elif cond['right'].startswith(self.df_name):
                left_cond.append(f"'{cond['left'].split('.')[1]}'")

            if cond['left'].startswith(df_name):
                right_cond.append(f"'{cond['left'].split('.')[1]}'")
            elif cond['right'].startswith(df_name):
                right_cond.append(f"'{cond['right'].split('.')[1]}'")

        if len(left_cond) != len(right_cond):
            raise Exception('cannot perform ON with the giving conditions')

        left_cond = ', '.join(left_cond)
        right_cond = ', '.join(right_cond)

        return [f'''{self.df_name} = {self.df_name}.merge({df_name}, how='{join_type}',  left_on=[{left_cond}], right_on=[{right_cond}])''']

    def insert(self, rows,  columns=None):
        tmp_df = f'df_{next(tempfile._get_candidate_names())}'

        # convert rows to string
        # str fail coz it double the quotes as string ex 'test' -> "'test'"
        inserted_rows = '['
        for row in rows:
            row = [str(r) for r in row]
            t = '[' + ', '.join(row) + '],'
            inserted_rows += t
        inserted_rows = inserted_rows[:-1] + ']'

        # create a tmp df to append at the end
        insert_code = [
            f'''{tmp_df} = pd.DataFrame({inserted_rows}, columns={columns if columns else f'{self.df_name}.columns'})''']

        insert_code += [
            f'''{self.df_name} = {self.df_name}.append({tmp_df}, ignore_index=True)''']
        return insert_code

    def insert_select(self, df,  columns=None):
        insert_code = []

        # columns names are not specified
        if not columns:
            insert_code += [f'''{df}.columns = {self.df_name}.columns''']

        insert_code += [
            f'''{self.df_name} = {self.df_name}.append({df}, ignore_index=True)''']
        return insert_code

    # delete rows if the condition is applicable
    def delete(self, conditions):
        np_conditions = ''
        if not conditions:
            #  where with the first column will return all the columns
            np_conditions += f'{self.df_name}[{self.df_name}.columns[0]]'
        else:
            np_conditions = self.__conditions(conditions)

        return ['import numpy as np', f'''{self.df_name} = {self.df_name}.drop(np.where({np_conditions})[0])''']

    # update rows of the dataframe
    def update(self, conditions, rec_set):
        np_conditions = ''
        if not conditions:
            #  where with the first column will return all the columns
            np_conditions += f'{self.df_name}[{self.df_name}.columns[0]]'
        else:
            np_conditions = self.__conditions(conditions)

        code = ['import numpy as np']

        # run the operation or every column
        for obj in rec_set:
            code += [
                f'''{self.df_name}_{obj['column'].replace(' ', '_')} = np.where({np_conditions}, {obj['value']}, {self.df_name}['{obj['column']}'])''']

        for obj in rec_set:
            code += [
                f'''{self.df_name}['{obj['column']}'] = {self.df_name}_{obj['column'].replace(' ', '_')}''']

        return code


# class of csv files
class CSVDS(DataSource):
    # return string that open csv file
    def open(self, name=None):
        if not name:
            name = f'df_{next(tempfile._get_candidate_names())}'

        self.df_name = name

        return ['import pandas as pd', f'''{self.df_name} = pd.read_csv('{self._file_path}', sep=',')''']

    # return string that save csv file
    def close(self, type=None):
        return [f'''{self.df_name}.to_csv('{self._file_path}', sep=',', index=False)''']


# tsv file class
class TSVDS(DataSource):
    # return string that open tsv file
    def open(self, name=None):
        if not name:
            name = f'df_{next(tempfile._get_candidate_names())}'

        self.df_name = name

        return ['import pandas as pd', f'''{self.df_name} = pd.read_csv('{self._file_path}', sep='\\t')''']

    # return string that save tsv file
    def close(self, type=None):
        return [f'''{self.df_name}.to_csv('{self._file_path}', sep='\\t', index=False)''']


# json file class
class JSONDS(DataSource):
    # return string that open json file
    def open(self, name=None):
        if not name:
            name = f'df_{next(tempfile._get_candidate_names())}'

        self.df_name = name

        return ['import pandas as pd', f'''{self.df_name} = pd.read_json('{self._file_path}')''']

    # return string that save json file
    def close(self, type=None):
        return [f'''{self.df_name}.to_json('{self._file_path}', index=False)''']


# HTML file class
class HTMLDS(DataSource):
    # return string that open HTML file
    def open(self, name=None):
        if not name:
            name = f'df_{next(tempfile._get_candidate_names())}'

        self.df_name = name

        return ['import pandas as pd', f'''{self.df_name} = pd.read_html('{self._file_path}')''']

    # return string that save HTML file
    def close(self, type=None):
        return [f'''{self.df_name}.to_html('{self._file_path}', index=False)''']


# XML file class
class XMLDS(DataSource):
    # return string that open XML file
    def open(self, name=None):
        if not name:
            name = f'df_{next(tempfile._get_candidate_names())}'

        self.df_name = name

        return ['import pandas as pd', f'''{self.df_name} = pd.read_xml('{self._file_path}')''']

    # return string that save XML file
    def close(self, type=None):
        return [f'''{self.df_name}.to_xml('{self._file_path}', index=False)''']


# excel file class
class ExcelDS(DataSource):
    # return string that open excel file
    def open(self, name=None):
        if not name:
            name = f'df_{next(tempfile._get_candidate_names())}'

        self.df_name = name

        return ['import pandas as pd', f'''{self.df_name} = pd.read_excel('{self._file_path}')''']

    # return string that save excel file
    def close(self, type=None):
        return [f'''{self.df_name}.to_excel('{self._file_path}', index=False)''']


# hdf file class
class HDFDS(DataSource):
    # return string that open hdf file
    def open(self, name=None):
        if not name:
            name = f'df_{next(tempfile._get_candidate_names())}'

        self.df_name = name

        return ['import pandas as pd', f'''{self.df_name} = pd.read_hdf('{self._file_path}')''']

    # return string that save hdf file
    def close(self, type=None):
        return [f'''{self.df_name}.to_hdf('{self._file_path}', index=False)''']


# feather file class
class FeatherDS(DataSource):
    # return string that open feather file
    def open(self, name=None):
        if not name:
            name = f'df_{next(tempfile._get_candidate_names())}'

        self.df_name = name

        return ['import pandas as pd', f'''{self.df_name} = pd.read_feather('{self._file_path}')''']

    # return string that save feather file
    def close(self, type=None):
        return [f'''{self.df_name}.to_feather('{self._file_path}', index=False)''']


# parquet file class
class ParquetDS(DataSource):
    # return string that open parquet file
    def open(self, name=None):
        if not name:
            name = f'df_{next(tempfile._get_candidate_names())}'

        self.df_name = name

        return ['import pandas as pd'], f'''{self.df_name} = pd.read_parquet('{self._file_path}')'''

    # return string that save parquet file
    def close(self, type=None):
        return [f'''{self.df_name}.to_parquet('{self._file_path}', index=False)''']


# stata file class
class StataDS(DataSource):
    # return string that open stata file
    def open(self, name=None):
        if not name:
            name = f'df_{next(tempfile._get_candidate_names())}'

        self.df_name = name

        return ['import pandas as pd', f'''{self.df_name} = pd.read_stata('{self._file_path}')''']

    # return string that save stata file
    def close(self, type=None):
        return [f'''{self.df_name}.to_stata('{self._file_path}', index=False)''']


# pickle file class
class PickleDS(DataSource):
    # return string that open pickle file
    def open(self, name=None):
        if not name:
            name = f'df_{next(tempfile._get_candidate_names())}'

        self.df_name = name

        return ['import pandas as pd', f'''{self.df_name} = pd.read_pickle('{self._file_path}')''']

    # return string that save pickle file
    def close(self, type=None):
        return [f'''{self.df_name}.to_pickle('{self._file_path}', index=False)''']


class PostgresDS(DataSource):
    def open(self, name=None):
        if not name:
            name = f'df_{next(tempfile._get_candidate_names())}'

        self.df_name = name
        self.engine = f'eng_{next(tempfile._get_candidate_names())}'
        splitted_string = self._file_path.split(';')
        self.table = splitted_string.pop().lower()
        connection_string = ';'.join(splitted_string)

        return ['import pandas as pd', 'from sqlalchemy import create_engine', f'''{self.engine} = create_engine('postgresql://{connection_string}')''', f'''{self.df_name} = pd.read_sql_table('{self.table}',  con={self.engine})''']

    def close(self, type=None):
        return [f'''{self.df_name}.to_sql(name='{self.table}', con={self.engine}, if_exists = 'replace', index=False)''']


class MysqlDS(DataSource):
    def open(self, name=None):
        if not name:
            name = f'df_{next(tempfile._get_candidate_names())}'

        self.df_name = name
        self.engine = f'eng_{next(tempfile._get_candidate_names())}'
        splitted_string = self._file_path.split(';')
        self.table = splitted_string.pop().lower()
        connection_string = ';'.join(splitted_string)

        return ['import pandas as pd', 'from sqlalchemy import create_engine', 'import pymysql', f'''{self.engine} = create_engine('mysql+pymysql://{connection_string}')''', f'''{self.df_name} = pd.read_sql_table('{self.table}',  con={self.engine})''']

    def close(self, type=None):
        return [f'''{self.df_name}.to_sql(name='{self.table}', con={self.engine}, if_exists = 'replace', index=False)''']


class sqliteDS(DataSource):
    def open(self, name=None):
        if not name:
            name = f'df_{next(tempfile._get_candidate_names())}'

        self.df_name = name
        self.engine = f'eng_{next(tempfile._get_candidate_names())}'
        splitted_string = self._file_path.split(';')
        self.table = splitted_string.pop().lower()
        connection_string = ';'.join(splitted_string)

        return ['import pandas as pd', 'from sqlalchemy import create_engine', f'''{self.engine} = create_engine('sqlite:///{connection_string}')''', f'''{self.df_name} = pd.read_sql_table('{self.table}',  con={self.engine})''']

    def close(self, type=None):
        return [f'''{self.df_name}.to_sql(name='{self.table}', con={self.engine}, if_exists = 'replace', index=False)''']


class MssqlDS(DataSource):
    def open(self, name=None):
        if not name:
            name = f'df_{next(tempfile._get_candidate_names())}'

        self.df_name = name
        self.engine = f'eng_{next(tempfile._get_candidate_names())}'
        splitted_string = self._file_path.split(';')
        self.table = splitted_string.pop().lower()
        connection_string = ';'.join(splitted_string)

        return ['import pandas as pd', 'from sqlalchemy import create_engine', 'import pyodbc', f'''{self.engine} = create_engine('mssql+pyodbc://{connection_string}')''', f'''{self.df_name} = pd.read_sql_table('{self.table}',  con={self.engine})''']

    def close(self, type=None):
        return [f'''{self.df_name}.to_sql(name='{self.table}', con={self.engine}, if_exists = 'replace', index=False)''']
