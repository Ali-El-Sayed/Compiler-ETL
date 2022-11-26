#!/usr/local/bin/python
from lexer import lexer
import ply.yacc as yacc


class parser():

    tokens = lexer.tokens

    precedence = (
        ('left', 'OR'),
        ('left', 'AND'),
        ('left', 'NOT'),
        ('left', '=', 'NE', '<', '>', 'LE', 'GE'),
        ('left', '*'),
    )

    def p_start(self, p):
        '''
        start : statements_list
            | configs ';' statements_list

        '''
        configs = {
            'log': False,
            'print': False,
            'path': './sqlp.log'
        }

        if len(p) == 2:
            p[0] = {
                'configs': configs,
                'statements_list': p[1]
            }
        else:
            configs.update(p[1])

            p[0] = {
                'configs': configs,
                'statements_list': p[3]
            }

    def p_configs(self, p):
        '''
        configs : config
                | configs ';' config

        '''
        if len(p) == 2:
            p[0] = p[1]
        elif p[2] == ';':
            p[1].update(p[3])
            p[0] = p[1]

    def p_config(self, p):
        '''
          config : SET LOG TRUE
                | SET LOG FALSE
                | SET LOG PATH STRING
                | SET LOG PRINT TRUE
                | SET LOG PRINT FALSE

        '''
        if len(p) == 4:
            p[0] = {'log': True if p[3] == 'true' else False}
        elif len(p) == 5 and p[3] == 'path':
            p[0] = {'path': p[4]}
        elif len(p) == 5 and p[3] == 'print':
            p[0] = {'print': True if p[4] == 'true' else False}

    def p_statements_list(self, p):
        '''
        statements_list : statement
                        |  statements_list ';' statement

        '''

        if len(p) == 2:
            p[0] = [p[1]]
        else:
            p[0] = p[1] + [p[3]]

    def p_statement(self, p):
        '''
        statement : select_statement
                    | insert_statement
                    | update_statement
                    | delete_statement
        '''

        if len(p) == 2:
            p[0] = p[1]

    def p_select_statement(self, p):
        '''
        select_statement : SELECT DISTINCT agg_columns_list FROM ds conditions group order limit offset
                        | SELECT agg_columns_list FROM ds conditions group order limit offset
                        | SELECT agg_columns_list INTO ds FROM ds conditions limit offset
                        | SELECT DISTINCT agg_columns_list FROM ds joins conditions group order
                        | SELECT agg_columns_list FROM ds joins conditions group order

        '''

        if len(p) == 11 and p[4] == 'from':
            p[0] = {'type': 'select',
                    'statement_parser': {'distinct': p[2],
                                         'columns_list': p[3],
                                         'ds': p[5],
                                         'conditions': p[6],
                                         'group': p[7],
                                         'order': p[8],
                                         'limit': p[9],
                                         'offset': p[10]}
                    }
        elif len(p) == 10 and p[3] == 'from':
            p[0] = {'type': 'select',
                    'statement_parser': {'columns_list': p[2],
                                         'ds': p[4],
                                         'conditions': p[5],
                                         'group': p[6],
                                         'order': p[7],
                                         'limit': p[8],
                                         'offset': p[9]}
                    }
        elif len(p) == 10 and p[3] == 'into':
            p[0] = {'type': 'select_into',
                    'statement_parser': {'columns_list': p[2],
                                         'ds1': p[4],
                                         'ds2': p[6],
                                         'conditions': p[7],
                                         'limit': p[8],
                                         'offset': p[9]}}
        elif len(p) == 10:
            p[0] = {'type': 'select_joins',
                    'statement_parser': {'distinct': p[2],
                                         'columns_list': p[3],
                                         'ds': p[5],
                                         'joins': p[6],
                                         'conditions': p[7],
                                         'group': p[8],
                                         'order': p[9]}}
        elif len(p) == 9:
            p[0] = {'type': 'select_joins',
                    'statement_parser': {'columns_list': p[2],
                                         'ds': p[4],
                                         'joins': p[5],
                                         'conditions': p[6],
                                         'group': p[7],
                                         'order': p[8]}}

    def p_insert_statement(self, p):
        '''
        insert_statement : INSERT INTO ds '(' columns_list ')' VALUES rows
                        | INSERT INTO ds VALUES rows
                        | INSERT INTO ds '(' columns_list ')' select_statement
                        | INSERT INTO ds select_statement
        '''

        if len(p) == 9:
            p[0] = {'type': 'insert',
                    'statement_parser': {'ds': p[3],
                                         'columns_list': p[5],
                                         'rows': p[8]}}
        elif len(p) == 6:
            p[0] = {'type': 'insert',
                    'statement_parser': {'ds': p[3],
                                         'rows': p[5]}}
        elif len(p) == 8:
            p[0] = {'type': 'insert_select',
                    'statement_parser': {'ds': p[3],
                                         'columns_list': p[5],
                                         'select_statement': p[7]}}
        elif len(p) == 5:
            p[0] = {'type': 'insert_select',
                    'statement_parser': {'ds': p[3],
                                         'select_statement': p[4]}}

    def p_update_statement(self, p):
        '''
        update_statement : UPDATE ds SET rec_set conditions

        '''

        p[0] = {'type': 'update',
                'statement_parser': {'ds': p[2],
                                     'rec_set': p[4],
                                     'conditions': p[5]}}

    def p_delete_statement(self, p):
        '''
        delete_statement : DELETE FROM ds conditions

        '''

        p[0] = {'type': 'delete',
                'statement_parser': {'ds': p[3],
                                     'conditions': p[4]}}

    def p_rec_set(self, p):
        '''
        rec_set : column '=' value
                | rec_set ',' column '=' value

        '''
        if len(p) == 4:
            p[0] = [{'column': p[1], 'value': p[3]}]
        elif len(p) == 6 and p[2] == ',':
            p[0] = p[1] + [{'column': p[3], 'value': p[5]}]

    def p_joins(self, p):
        '''
        joins : join_type ds ON conditions
                | joins join_type ds ON conditions

        '''

        if len(p) == 5:
            p[0] = [{'join_type': p[1], 'ds': p[2],
                    'conditions': p[4]}]
        elif len(p) == 6:
            p[0] = p[1] + [{'join_type': p[2], 'ds': p[3],
                            'conditions': p[5]}]

    def p_join_type(self, p):
        '''
        join_type : JOIN
                | INNER JOIN
                | LEFT JOIN
                | RIGHT JOIN
                | FULL JOIN

        '''

        if len(p) == 2:
            p[0] = 'inner'
        elif len(p) == 3:
            p[0] = p[1]

    def p_rows(self, p):
        '''
        rows : '(' rec_values ')'
                |  '(' rec_values ')' ',' rows

        rec_values : value
                    | rec_values ',' value
        '''

        if len(p) == 2:
            p[0] = [p[1]]
        elif len(p) == 4 and p[2] != ',':
            p[0] = [p[2]]
        elif len(p) == 4 and p[2] == ',':
            p[0] = p[1] + [p[3]]
        elif len(p) == 6:
            p[0] = [p[2]] + p[5]

    def p_columns_list(self, p):
        '''
        columns_list : '*'
                     | rec_columns

        '''

        if len(p) == 2 and p[1] == '*':
            p[0] = [p[1]]
        else:
            p[0] = p[1]

    def p_agg_columns_list(self, p):
        '''
            agg_columns_list : '*'
                     | agg_rec_columns
        '''
        if len(p) == 2 and p[1] == '*':
            p[0] = [{
                'column': p[1]}]
        else:
            p[0] = p[1]

    def p_agg_rec_columns(self, p):
        '''
            agg_rec_columns : t_column
                        | agg_rec_columns ',' t_column
        '''

        if len(p) == 4:
            p[0] = p[1] + [p[3]]
        else:
            p[0] = [p[1]]

    def p_rec_columns(self, p):
        '''
            rec_columns : column
                        | rec_columns ',' column
        '''

        if len(p) == 4:
            p[0] = p[1] + [p[3]]
        else:
            p[0] = [p[1]]

    def p_t_column(self, p):
        '''
        t_column : column
                | function
                | column AS NAME
                | function AS NAME

        '''
        if len(p) == 2:
            p[0] = {
                'column': p[1]
            }
        elif len(p) == 4:

            p[0] = {
                'column': p[1],
                'as': p[3]
            }
        else:
            p[0] = {'column': p[1]}

    def p_column(self, p):
        '''
        column : NAME
                | INDEX
        '''
        p[0] = p[1]

    def p_function(self, p):
        '''
        function : function_name '(' column ')'

        '''

        p[0] = {
            'function': p[1],
            'column': p[3]
        }

    def p_function_name(self, p):
        '''
        function_name : SUM
                        | MAX
                        | MIN
                        | COUNT
                        | AVG
        '''

        p[0] = p[1]

    def p_ds(self, p):
        '''
            ds : '(' select_statement ')'
                | '(' select_statement ')' AS NAME
                | src
                | src AS NAME

        '''

        if len(p) == 2:
            p[0] = {'src': p[1]}
        elif len(p) == 4 and p[1] == '(':
            p[0] = {'src': p[2]}
        elif len(p) == 6:
            p[0] = {'src': p[2], 'name': p[5]}
        elif len(p) == 4 and p[1] != '(':
            p[0] = {'src': p[1], 'name': p[3]}

    def p_src(self, p):
        '''
            src : NAME
                | DATASOURCE
        '''

        p[0] = p[1]

    def p_limit(self, p):
        '''
            limit : LIMIT NUMBER
                    | empty
        '''

        if len(p) == 3:
            p[0] = p[2]

    def p_offset(self, p):
        '''
            offset : OFFSET NUMBER
                    | empty
        '''

        if len(p) == 3:
            p[0] = p[2]

    def p_conditions(self, p):
        '''
        conditions : WHERE rec_conditions
                    | rec_conditions
                    | empty
        rec_conditions : column op value
                        | rec_conditions AND column op value
                        | rec_conditions OR column op value
        '''

        if len(p) == 2:
            p[0] = p[1]
        elif len(p) == 3:
            p[0] = p[2]
        elif len(p) == 4:
            p[0] = [{'left': p[1], 'op': p[2], 'right': p[3]}]
        elif len(p) == 6:
            p[0] = p[1] + \
                [{'sd': p[2], 'left': p[3], 'op': p[4], 'right': p[5]}]

    def p_value(self, p):
        '''
        value : NAME
                | STRING
                | FLOAT
                | NUMBER
                | column
        '''
        if len(p) == 2:
            p[0] = p[1]

    def p_empty(self, p):
        'empty :'
        pass

    def p_op(self, p):
        '''
        op : '>'
            | '<'
            | '='
            | LE
            | GE
            | NE
            | LIKE
        '''

        p[0] = p[1]

    def p_group(self, p):
        '''
        group : GROUP BY columns_list
            | empty
        '''
        if len(p) == 4:
            p[0] = p[3]

    def p_order(self, p):
        '''
        order : ORDER BY orders
            | empty
        '''
        if len(p) == 4:
            p[0] = p[3]

    def p_orders(self, p):
        '''
        orders : column sort_type
            | orders ',' column sort_type
        '''
        if len(p) == 3:
            if not p[2]:
                p[0] = [{'column': p[1], 'type': 'asc'}]
            else:
                p[0] = [{'column': p[1], 'type': p[2]}]
        elif len(p) == 5:
            if not p[4]:
                p[0] = p[1] + [{'column': p[3], 'type': 'asc'}]
            else:
                p[0] = p[1] + [{'column': p[3], 'type': p[4]}]

    def p_sort_type(self, p):
        '''
        sort_type : ASC
                | DESC
                | empty
        '''

        p[0] = p[1]

    def p_error(self, p):
        if p:
            raise Exception(
                f"please check you syntax near '{p.value}' line {p.lineno} char {p.lexpos}")
        else:
            raise Exception(f'please check you syntax')

    def __init__(self):
        self.lexer = lexer()
        self.parser = yacc.yacc(module=self)

    def parse(self, sql_stateements):
        return self.parser.parse(sql_stateements)
