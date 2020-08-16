import os, json
import connect as db
import datetime
import logging
import logging.config
import yaml

from flask import Flask, jsonify, request, redirect, render_template

with open('l_config.yaml', 'r') as f:
	config = yaml.safe_load(f.read())
	logging.config.dictConfig(config)

logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = "secret key"

@app.route("/")
def upload_form():
	return render_template("autocomplete.html")

@app.route("/search", methods=["POST"])
def search():
	timeStart = datetime.datetime.today()
	res = []
	if request.method == "POST":
		term = request.form["q"]
		res = getStreets(term)
	print(f'term: {term}, res: {len(res)}, time: {datetime.datetime.today() - timeStart}')
	resp = jsonify(res)
	resp.headers["Access-Control-Allow-Origin"] = "*"
	resp.status_code = 200
	return resp

def getStreets(code):
	""" query data """
	query = """
        SELECT DISTINCT street FROM moscow_street_list_tbl 
        WHERE street LIKE %(code)s 
        LIMIT 10
    """
	queryParameters = {'code': ''.join(['%', code, '%'])}
	query = """
        SELECT DISTINCT street, similarity(street, %(code)s) AS sml
		FROM moscow_street_list_tbl 
        WHERE street %% %(code)s 
		ORDER BY sml DESC, street
        LIMIT 10
    """
	_query = """
        SELECT DISTINCT street, street <-> %(code)s AS dist
		FROM moscow_street_list_tbl 
		ORDER BY dist
        LIMIT 10
    """
	queryParameters = {'code': code }
	conn = None
	res = []
	try:
		params = db.config()
		conn = db.psycopg2.connect(**params)
		cur = conn.cursor(cursor_factory = db.RealDictCursor)
		cur.execute('SELECT set_limit(0)')
		cur.execute(query, queryParameters)
		for row in db.iterRow(cur, 10):
			res.append(row['street'])
		cur.close()
	except (Exception, db.psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()
	return res

if __name__ == "__main__":
	app.run()