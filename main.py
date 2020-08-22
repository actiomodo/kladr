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
		res = getStreets(request)
	#pprint.pprint(f'response: {str(res)[:40]}')
	#print(request.form)
	#print(f'term: {term}, res: {len(res)}, time: {datetime.datetime.today() - timeStart}')
	#pprint.pprint(res)
	resp = jsonify(res)
	resp.headers['Access-Control-Allow-Origin'] = '*'
	resp.status_code = 200
	return resp

def getStreets(request):
	timeStart = datetime.datetime.today()
	""" query data """
	query = """
        SELECT street_full AS street FROM msk_shot_tbl 
        WHERE street_shot LIKE %(value)s 
        LIMIT 10
    """
	query = """
        SELECT code, street_full AS street, street_shot <-> %(value)s AS dist
		FROM msk_shot_tbl 
		ORDER BY dist
        LIMIT 10
    """
	query = """
        SELECT code, street_full AS street, similarity(street_shot, %(value)s) AS sml
		FROM msk_shot_tbl 
        WHERE street_shot %% %(value)s 
		ORDER BY sml DESC
        LIMIT 10
    """
	value = request.form['q']
	if request.form['v'] == 'r_metaphone':
		query = """
			SELECT code, street_full AS street FROM rus_shot_tbl 
			WHERE metaphone(street_shot) LIKE 
			'%%'||regexp_replace(metaphone(%(value)s),'\s','%%','g')||'%%'
			LIMIT 10
		"""
	elif request.form['v'] == 'm_trgm':
		if value.find(']') != -1:
			value = value.split(']')[1].strip()
		value = ''.join(['%', value, '%'])
	queryParameters = {'value': value}
	conn = None
	res = []
	"""
	q : request.term,
	v : "m_trgm",
	_id : searchTrgm.code,
	_s  : searchTrgm.street,
	_vs : searchTrgm.v_street,
	_h  : searchTrgm.house,
	_vh : searchTrgm.v_house,
	_f  : searchTrgm.full
	"""
	_code = request.form['_id']
	_street = request.form['_s']
	_v_street = request.form['_vs']
	_house = request.form['_h']
	_v_house = request.form['_vh']
	_full = request.form['_f']
	if _house == 'y' and request.form['q'].find(_v_house) == -1:
		_house = 'n'
		_v_house = 'n'
		_full = 'n'
	elif _street == 'y' and request.form['q'].find(_v_street) == -1:
		_code = 'n'
		_street = 'n'
		_v_street = 'n'
		_house = 'n'
		_v_house = 'n'
		_full = 'n'
	if _street == 'y':
		#res.append({'key': 'house', 'value': 'found later'})
		value = request.form['q'].split(_v_street)[-1].strip()
		start = 0
		while start < len(value) and not value[start].isdigit():
			start += 1
		value = value[start:]
		value = (f'({value})|({value}.)', '.')[value == '']
		query = """
			WITH houses AS (
				SELECT
				array_to_string(regexp_match(regexp_split_to_table(t.houses, ','),%(value)s||'.'), '') AS dom,
				r.code
				FROM rus_shot_tbl AS r
				LEFT JOIN td_tbl AS t ON t.code = r.code
				WHERE r.code = %(code)s
				)
			SELECT DISTINCT dom, code
			FROM houses
			WHERE dom != ''
			ORDER BY dom
			LIMIT 10
		"""
		queryParameters = {'value': value, 'code': _code}
	res.append({'key': '_id', 'value': _code})
	res.append({'key': '_s', 'value': _street})
	res.append({'key': '_vs', 'value': _v_street})
	res.append({'key': '_h', 'value': _house})
	res.append({'key': '_vh', 'value': _v_house})
	res.append({'key': '_f', 'value': _full})
	try:
		params = db.configDbConnection()
		conn = db.psycopg2.connect(**params)
		cur = conn.cursor(cursor_factory = db.RealDictCursor)
		if request.form['v'] == 'm_trgm':
			cur.execute('SELECT set_limit(0)')
		#logger.debug(cur.mogrify(query, queryParameters))
		cur.execute(query, queryParameters)
		for i, row in enumerate(db.iterRow(cur, 10)):
			code = ''.join([str(i), row['code']])
			if request.form['v'] == 'm_trgm':
				if _street == 'y':
					street = ', д '.join([_v_street, row['dom']])
				else:
					sml = row['sml']
					v_street = row['street']
					street = f'[{sml:.6f}] {v_street}'
			elif request.form['v'] == 'r_metaphone':
				if _street == 'y':
					street = ', д '.join([_v_street, row['dom']])
				else:
					street = row['street']
			else:
				street = row['street']
			res.append({'key': code, 'value': f'{street}'})
		cur.close()
	except (Exception, db.psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()
	timeStop = datetime.datetime.today()
	res.append({'key': '_t', 'value': f'{(timeStop.microsecond - timeStart.microsecond) // 1000}'})
	return res

if __name__ == '__main__':
	app.run()