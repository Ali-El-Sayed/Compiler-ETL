from flask import Flask
from flask import render_template
from CompilerYacc import parser
import etl
from flask import request

app = Flask(__name__)

@app.route('/')
def dark():
    return render_template("index.html")

@app.route('/light')
def light():
    return render_template("light.html")


@app.route('/sqlp',  methods = ['POST'])
def sqltopandas():
    My_parser = parser()

    statements = request.form['sql'].strip()
    statements = statements[:-1] if statements[-1] == ';' else statements

    try:
        ast = My_parser.parse(statements)
    except Exception as e:
        return str(e)
        
    result_code = []

    # set configs
    etl.CONFIGS = ast['configs']
    etl.LOGING = etl.CONFIGS['log'] | etl.CONFIGS['print']

    if etl.LOGING:
        result_code += etl.log() + [''] * 4

    for statement in ast['statements_list']:

        if statement['type'] == 'select':
            result_code += etl.select(statement['statement_parser'])[1]
        if statement['type'] == 'select_joins':
            result_code += etl.select_join(statement['statement_parser'])[1]
        if statement['type'] == 'insert':
            result_code += etl.insert(statement['statement_parser'])[1]
        if statement['type'] == 'insert_select':
            result_code += etl.insert_select(statement['statement_parser'])[1]
        if statement['type'] == 'delete':
            result_code += etl.delete(statement['statement_parser'])[1]
        if statement['type'] == 'update':
            result_code += etl.update(statement['statement_parser'])[1]
        if statement['type'] == 'select_into':
            result_code += etl.select_into(statement['statement_parser'])[1]
        result_code += [''] * 4

    return  '\n'.join(result_code)

