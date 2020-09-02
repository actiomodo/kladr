import os
import json
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
	timeStart = datetime.datetime.now()
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
	value = transliteration(queryNormalization(request.form['q'])).strip()
	if request.form['v'] == 'r_metaphone':
		if ' ' in value:
			pos = value.find(' ')
			query = """
				WITH first_word AS (SELECT code, sort, street_full, street_metaphone
							FROM rus_shot_tbl
							WHERE street_metaphone
							LIKE '%%'||regexp_replace(metaphone(%(value1)s),'\s','%%','g')||'%%'
							)
				SELECT code, sort, street_full AS street
				FROM first_word
				WHERE street_metaphone
				LIKE '%%'||regexp_replace(metaphone(%(value2)s),'\s','%%','g')||'%%'
				LIMIT 10
			"""
			queryParameters = {'value1': value[:pos], 'value2': value[pos + 1:]}
		else:
			query = """
				SELECT code, sort, street_full AS street FROM rus_shot_tbl
				WHERE street_metaphone LIKE
				'%%'||regexp_replace(metaphone(%(value)s),'\s','%%','g')||'%%'
				LIMIT 10
			"""
			queryParameters = {'value': value}
	elif request.form['v'] == 'm_trgm':
		if ']' in value:
			value = value.split(']')[1].strip()
		value = ''.join(['%', value, '%'])
		queryParameters = {'value': transliteration(queryNormalization(value))}
	conn = None
	res = []
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
		value = request.form['q'].split(_v_street)[-1].strip()
		start = 0
		while start < len(value) and not value[start].isdigit():
			start += 1
		value = value[start:]
		query = """
			WITH houses AS (
				SELECT
				array_to_string(regexp_match(regexp_split_to_table(t.houses, ','), %(value)s||'[^ ]*', 'i'), '')
				AS dom, r.code
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
		queryParameters = {'value': value.lower(), 'code': _code}
	res.append({'key': '_id', 'value': _code})
	res.append({'key': '_s', 'value': _street})
	res.append({'key': '_vs', 'value': _v_street})
	res.append({'key': '_h', 'value': _house})
	res.append({'key': '_vh', 'value': _v_house})
	res.append({'key': '_f', 'value': _full})
	resFoundCount = 0
	try:
		params = db.configDbConnection()
		conn = db.psycopg2.connect(**params)
		cur = conn.cursor(cursor_factory = db.RealDictCursor)
		if request.form['v'] == 'm_trgm':
			cur.execute('SELECT set_limit(0)')
		#logger.debug(cur.mogrify(query, queryParameters))
		cur.execute(query, queryParameters)
		for i, row in enumerate(db.iterRow(cur, 10)):
			resFoundCount += 1
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
		if resFoundCount == 0:
			res.append({'key': '-1', 'value': f'Адрес не найден'})
	except (Exception, db.psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()
	timeDelta = datetime.datetime.now() - timeStart
	timeDeltaMicroseconds = int(timeDelta.microseconds // 1e3 + timeDelta.seconds * 1e3)
	res.append({'key': '_t', 'value': f'{timeDeltaMicroseconds}'})
	return res

def transliteration(query):
	literas = {'z': 'я', 'x':' ч', 'c': 'с', 'v': 'м', 'b': 'и', 'n': 'т', 'm': 'ь', ',': 'б',
			   '.': 'ю', 'a': 'ф', 's': 'ы', 'd': 'в', 'f': 'а', 'g': 'п', 'h': 'р', 'j': 'о',
			   'k': 'л', 'l': 'д', ';': 'ж', '\'': 'э', 'q': 'й', 'w': 'ц', 'e': 'у', 'r': 'к',
			   't': 'е', 'y': 'н', 'u': 'г', 'i': 'ш', 'o': 'щ', 'p': 'з', '[': 'х', ' ': ' '}
	return ''.join([literas.get(v, v) for v in query])

def queryNormalization(query):
	scname = ['Аобл','АО','г','г.ф.з.','край','обл','Респ','АО','пл-ка','пл','пл.',
			  'вн.тер. г.','г.о.','м.р-н','р-н','у','дп','п.','п/ст','проезд',
			  'кп','пгт','п/о','рп','с/а','с/о','с/мо','с/п','с/с','п-к','п/о',
			  'аал','автодорога','высел','г-к','гп','дп','км','коса','мгстр.',
			  'дп.','д.','жилзона','стр','сзд.','туп','заезд','кв-л','просека','пр-кт',
			  'жилрайон','зим.','кв-л','киш.','кордон','кп','лпх','пер','п/р','платф',
			  'м','мкр','нп','нп.','п/р','п','пгт','п/ст','п. ж/д ст.',
			  'пос.рзд','починок','п/о','промзона','рп','снт','с.','сп','сп.','сл',
			  'ст-ца','ст','а/я','б-р','взв.','гск','днп','д','дор','ж/д_оп',
			  'м','местность','месторожд.','мкр','мост','наб','нп','н/п','парк','обл.',
			  'рзд','р-н','ряд','ряды','сад','снт','с/т','с','сл','спуск','ст','ул.',
			  'ул','ус.','уч-к','ф/х','х','ш','ю.','влд.','ДОМ','двлд.','зд.','г.','ш.',
			  'к.','кот.','ОНС','пав.','соор.','стр.','шахта','арбан','аул',
			  'балка','берег','бугор','вал','взвоз','владение','волость','въезд',
			  'выселки','город','городок','городской округ','деревня','дом','домовладение',
			  'дорога','заимка','здание','зимовье','зона','казарма','квартал','километр','кишлак',
			  'кольцо','кордон','корпус','коса','котельная','край','курортный поселок','леспромхоз',
			  'магистраль','массив','маяк','местечко','микрорайон','населенный пункт','округ','остров',
			  'павильон','переезд','переулок','площадь','полустанок','порт','поселение','поселок',
			  'проселок','проспект','проулок','район','республика','село','сквер','станица','станция',
			  'строение','съезд','территория','тупик','улица','улус','усадьба','участок','ферма',
			  'хутор','шоссе','юрты']
	for v in scname:
		for x in range(1, len(v) + 1):
			toReplace = ''.join([' ', v[:x], ' '])
			query = query.replace(toReplace, ' ')
		query = query.replace('.', '').replace(',', '')
		toReplace = ''.join([v, ' '])
		if query.find(v) == 0:
			query = query.replace(toReplace, '')
	res = query.strip().lower()
	return res

if __name__ == '__main__':
	app.run()