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
	pprint.pprint(f'response: {str(res)[:40]}')
	#print(request.form)
	#print(f'term: {term}, res: {len(res)}, time: {datetime.datetime.today() - timeStart}')
	#pprint.pprint(res)
	resp = jsonify(res)
	resp.headers['Access-Control-Allow-Origin'] = '*'
	resp.status_code = 200
	return resp

def getStreets(request):
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
	if request.form['v'] == 'm_trgm' and value.find(']') != -1:
		value = value.split(']')[1].strip()
	queryParameters = {'value': ''.join(['%', value, '%'])}
	if request.form['v'] == 'r_metaphone':
		query = """
			SELECT code, street_full AS street FROM rus_shot_tbl 
			WHERE metaphone(street_shot) LIKE 
			'%%'||regexp_replace(metaphone(%(value)s),'\s','%%','g')||'%%' LIMIT 10
		"""
	conn = None
	res = []
	if request.form['v'] == 'm_trgm':
		"""
		q : request.term,
		v : "m_trgm",
		_s_m : selected_m,
		_v_m : v_selected_m,
		_h_m : house_m
		"""
		selected_m = request.form['_s_m']
		v_selected_m = request.form['_v_m']
		house_m = request.form['_h_m']
		complete_m = request.form['_c_m']
		if v_selected_m != 'n' and request.form['q'].find(v_selected_m) == -1:
			selected_m = 'n'
			v_selected_m = 'n'
			house_m = 'n'
			complete_m = 'n'
		if house_m == 'y':
			#res.append({'key': 'house', 'value': 'found later'})
			value = request.form['q'].split(v_selected_m)[-1].strip()
			start = 0
			while start < len(value) and not value[start].isdigit():
				start += 1
			value = value[start:]
			value = (value, '.')[value == '']
			query = """
				WITH houses AS (
					SELECT 
					array_to_string(regexp_match(regexp_split_to_table(t.houses, ','),%(value)s||'+'), '') AS dom,
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
			queryParameters = {'value': value, 'code': selected_m}
		res.append({'key': '_s_m', 'value': selected_m})
		res.append({'key': '_v_m', 'value': v_selected_m})
		res.append({'key': '_h_m', 'value': house_m})
		res.append({'key': '_c_m', 'value': complete_m})
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
				if house_m == 'y':
					street = ', д '.join([v_selected_m, row['dom']])
					res.append({'key': selected_m, 'value': f'{street}'})
				else:
					sml = row['sml']
					street = row['street']
					res.append({'key': code, 'value': f'[{sml:.6f}] {street}'})
			else:
				street = row['street']
				res.append({'key': code, 'value': f'{street}'})
		cur.close()
	except (Exception, db.psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()
	return res

if __name__ == '__main__':
	app.run()