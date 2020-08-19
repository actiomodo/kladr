import os, json
import connect as db
import datetime
import logging
import logging.config
import yaml
import pprint

from flask import Flask, jsonify, request, redirect, render_template

with open('l_config.yaml', 'r') as f:
	config = yaml.safe_load(f.read())
	logging.config.dictConfig(config)

logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = 'alligator'

@app.route('/')
def upload_form():
	return render_template('autocomplete.html')

@app.route('/search', methods=['POST'])
def search():
	timeStart = datetime.datetime.today()
	res = []
	if request.method == 'POST':
		term = request.form['q']
		if request.form['v'] == 'm' and term.find(']') != -1:
			term = term.split(']')[1].strip()
		res = getStreets(term)
	#print(request.form)
	#print(f'term: {term}, res: {len(res)}, time: {datetime.datetime.today() - timeStart}')
	#pprint.pprint(res)
	resp = jsonify(res)
	resp.headers['Access-Control-Allow-Origin'] = '*'
	resp.status_code = 200
	return resp

def getStreets(code):
	""" query data """
	query = """
        SELECT street_full AS street FROM msk_shot_tbl 
        WHERE street_shot LIKE %(code)s 
        LIMIT 10
    """
	queryParameters = {'code': ''.join(['%', code, '%'])}
	query = """
        SELECT code, street_full AS street, similarity(street_shot, %(code)s) AS sml
		FROM msk_shot_tbl 
        WHERE street_shot %% %(code)s 
		ORDER BY sml DESC
        LIMIT 10
    """
	_query = """
        SELECT street_full AS street, street_shot <-> %(code)s AS dist
		FROM msk_shot_tbl 
		ORDER BY dist
        LIMIT 10
    """
	queryParameters = {'code': code }
	conn = None
	res = []
	if request.form['_h_m'] == 'y':
		#res.append({'p': '_s_m', 'value': request.form['_s_m']})
		res.append({'_s_m': request.form['_s_m']})
	try:
		params = db.configDbConnection()
		conn = db.psycopg2.connect(**params)
		cur = conn.cursor(cursor_factory = db.RealDictCursor)
		cur.execute('SELECT set_limit(0)')
		cur.execute(query, queryParameters)
		for i, row in enumerate(db.iterRow(cur, 10)):
			code = ''.join([str(i), row['code']])
			sml = row['sml']
			street = row['street']
			#res.append(f'[{sml:.6f}] {street}')
			#res[code] = f'[{sml:.6f}] {street}'
			res.append({'key': code, 'value': f'[{sml:.6f}] {street}'})
		cur.close()
	except (Exception, db.psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()
	return res

if __name__ == '__main__':
	app.run()