import json
import os
import ast
import re
from functools import wraps

from dotenv import load_dotenv, find_dotenv

from flask import Flask, g, request, abort, request_started
from flask_restful import Resource, reqparse
from flask_cors import CORS
from flask_json import FlaskJSON, json_response
from flask_restful_swagger_2 import Api, swagger, Schema

from neo4j import GraphDatabase, basic_auth

from serialize import *

"""
The credits are 
https://github.com/neo4j-examples/neo4j-movies-template/blob/master/flask-api/app.py
"""

# For environmental variables that represents the connection
load_dotenv(find_dotenv())

app = Flask(__name__)

CORS(app)
FlaskJSON(app)

api = Api(app, title='Neo4J Covid Free Demo API', api_version='0.0.10')


@api.representation('application/json')
def output_json(data, code, headers=None):
    return json_response(data_=data, headers_=headers, status_=code)


def env(key, default=None, required=True):
    """
    Retrieves environment variables and returns Python natives. The (optional)
    default will be returned if the environment variable does not exist.
    """
    try:
        value = os.environ[key]
        return ast.literal_eval(value)
    except(SyntaxError, ValueError):
        return value
    except KeyError:
        if default or not required:
            return default
        raise RuntimeError("Missing required environment variable '%s'" % key)


# DATABASE_USERNAME = env('COVID_FREE_DATABASE_USERNAME')
DATABASE_USERNAME = "neo4j"
# DATABASE_PASSWORD = env('COVID_FREE_DATABASE_PASSWORD')
DATABASE_PASSWORD = "blankets-ride-firefighting"
# DATABASE_URL = env('COVID_FREE_DATABASE_URL')
DATABASE_URL = "bolt://54.205.87.249:"

driver = GraphDatabase.driver(DATABASE_URL, auth=basic_auth(DATABASE_USERNAME, str(DATABASE_PASSWORD)))

# app.config['SECRET_KEY'] = env('SECRET_KEY')
app.config['SECRET_KEY'] = "very secret key"


def get_db():
    if not hasattr(g, 'neo4j_db'):
        g.neo4j_db = driver.session()
    return g.neo4j_db


@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'neo4j_db'):
        g.neo4j_db.close()


def set_user(sender, **extra):
    auth_header = request.headers.get('Authorization')
    if not auth_header:
        g.user = {'id': None}
        return
    match = re.match(r'^Token (\S+)', auth_header)
    if not match:
        abort(401, 'invalid authorization format. Follow `Token <token>`')
        return
    token = match.group(1)

    # get users from the system
    def get_user_by_token(tx, token):
        return tx.run(
            '''
            MATCH (user: User {api_key: $api_key}) RETURN user
            ''', {'api_key': token}
        ).single()

    db = get_db()
    result = db.read_transaction(get_user_by_token, token)

    try:
        g.user = result['user']
    except (KeyError, TypeError):
        abort(401, 'invalid authorization key')
    return


# request_started.connect(set_user, app)


def login_required(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            return {'message': 'no authorization provided'}, 401
        return f(*args, **kwargs)


# Q1
class PlaceQuarPeop(Resource):

    def get(self, place_name):
        def get_place_quar(tx, name_place):
            return list(tx.run('''MATCH
                (place:Place { name: $name_place} )-
                [hosted:HOSTED]->(person:Person)-[:GOT_AN]->(infection:Infection)
                WHERE hosted.entry_moment >= infection.date_of_infection
                RETURN person ''', {'name_place': name_place}))

        db = get_db()
        result = db.read_transaction(get_place_quar, place_name)

        return {"persons": [serialize_person(record['person']) for record in result]}


# Q2
class PlaceAmountPeop(Resource):
    def get(self):
        def get_amount_peop(tx):
            return list(tx.run('''MATCH (p:Place)-[:HOSTED]->(:Person)-[r:GOT_AN]->(:Infection)
                        WITH p, COUNT(r) AS cnt ORDER BY cnt desc
                        RETURN cnt,
                            collect(p) as places
                        LIMIT 10'''))

        db = get_db()
        result = db.read_transaction(get_amount_peop)

        for record in result:
            return {
                'count': record['cnt'],
                'places': [serialize_place(place) for place in record['places']]
            }
        return {'message': 'not found'}, 404


# Q3
class DailyInfected(Resource):
    def get(self):
        def get_cluster(tx):
            return list(tx.run('''MATCH (gp: GreenPass)
                    WITH gp.type as Vaccination_type, COUNT(gp) as count
                    ORDER BY count DESC
                    RETURN 
                        count, 
                        COLLECT(Vaccination_type) as type_of_vaccine 
                    '''))

        db = get_db()
        result = db.read_transaction(get_cluster)

        list_values = []

        for record in result:
            vaccines = []
            for vac in record['type_of_vaccine']:
                vaccines.append({"name": vac})
            list_values.append({'count': record['count'], 'vaccines': vaccines})
        return {
            'values': list_values
        }


# Q4
class InfectedHealed(Resource):
    def get(self):
        def get_healed(tx):
            return list(tx.run('''MATCH (pInfected:Person)-[:GOT_AN]->(:Infection) 
                                WITH COUNT(pInfected) AS infected 
                                MATCH (pHealthy:Person) 
                                WHERE EXISTS ((pHealthy)-[:GOT_AN]-(:Infection)) = FALSE 
                                WITH infected, COUNT(pHealthy) AS healthy 
                                RETURN infected, healthy, (infected / toFloat(healthy)) AS dailyRatio'''))

        db = get_db()
        result = db.read_transaction(get_healed)
        if result != []:
            return {
                'infected': result[0][0],
                'healthy': result[0][1],
                'ratio': result[0][2]
            }
        else:
            return {
                'infected': 0,
                'healthy': 0,
                'ratio': 0
            }


# Q5
class GetDailyStamp(Resource):
    def get(self):
        def get_daily_stamp(tx):
            return list(tx.run('''
                MATCH (pInfected:Person)-[:GOT_AN]->(i:Infection)
                WHERE date.truncate('day', i.date_of_infection) = date()
                WITH COUNT(pInfected) AS infected
                MATCH (pTested:Person)-[:HAS_A]->(gp:GreenPass)
                WHERE gp.type="Covid-19 Test" AND date.truncate('day', gp.date1) = date()
                WITH infected, COUNT(pTested) AS tested
                RETURN infected, tested, (infected/toFloat(tested)) AS dailyRatio
            '''))
        db = get_db()
        result = db.read_transaction(get_daily_stamp)
        if result != []:
            return {
                'infected': result[0][0],
                'tested': result[0][1],
                'ratio': result[0][2]
            }
        else:
            return {
                'infected': 0,
                'tested': 0,
                'ratio': 0
            }



# Q6
class MostVisited(Resource):
    def get(self):
        def get_history(tx):
            return list(tx.run('''match (:Person)-[r:WENT_TO]->(p:Place)
                                with count(r) as num, p
                                order by num desc limit 1
                                match (a:Person)-[r1:WENT_TO]->(p)<-[r2:WENT_TO]-(b:Person)
                                where r1.exit_moment.epochSeconds > r2.entry_moment.epochSeconds AND 
                                r1.entry_moment < r2.exit_moment
                                with count(a)+1 as number, p.name as place, r1, date(r1.entry_moment) as date, a
                                return date, number, place, collect(a) as persons order by number desc limit 1'''))

        db = get_db()
        result = db.read_transaction(get_history)
        for record in result:
            return {
                'date': str(record['date']),
                'count': record['number'],
                'placeName': record['place'],
                'persons': [serialize_person(person) for person in record['persons']]
            }
        return {'message': 'not found'}, 404


# C1
class SetPositive(Resource):
    def post(self):
        data = request.get_json()
        persons = data.get('persons')
        infections = data.get('infections')

        def get_set_positive(tx, people_positive, infections_positive):
            return tx.run("""MATCH (p: Person {taxCode: $taxCode, name: $name, surname: $surname})
                                CREATE (p)-[:GOT_AN]->(i:Infection {date_of_infection: $date_of_infection})
                                """, {
                                    'taxCode': people_positive.get('taxCode'),
                                    'name': people_positive.get('name'),
                                    'surname': people_positive.get('surname'),
                                    'date_of_infection': infections_positive.get('date_of_infection')}).single()

        db = get_db()
        for person, infection in zip(persons, infections):
            db.write_transaction(get_set_positive, person, infection)
        return {}, 201


# C2
class SetGreen(Resource):
    def post(self):
        data = request.args
        tax = data.get('taxCode')
        date_start = data.get('dateStart')
        date_end = data.get('dateEnd')
        type_vaccine = data.get('typeVac')

        def get_set_green(tx, tax_code, date1, date2, type_vac):
            return tx.run("""MATCH (a:Person)
                        WHERE a.taxCode=$taxCode 
                        CREATE (gp:GreenPass {date1: $dateStart, date2: $dateEnd, type: $typeVac}), 
                        (a)-[:HAS_A]->(gp)-[:BELONGS_TO]->(a)
                        WITH 1 as dummy
                        MATCH (a)-[r:GOT_AN]->(i:Infection)
                        WHERE a.taxCode=$taxCode
                        DETACH DELETE i
                        """, {'taxCode': tax_code, 'dateStart': date1, 'dateEnd': date2, 'typeVac': type_vac}).single()

        db = get_db()
        db.write_transaction(get_set_green, tax, date_start, date_end, type_vaccine)
        return {}, 201


# C3
class DataCleaning(Resource):
    def post(self):
        def get_clean(tx):
            return tx.run("""MATCH (a)-[r:HAS_A|BELONGS_TO|WENT_TO|HOSTED]->(p)
                            WHERE (datetime().epochSeconds - datetime(r.exit_moment).epochSeconds >= 86400*14)
                            OR
                            (datetime() > p.date2 OR datetime() > a.date2)
                            DELETE r
                            WITH 1 AS dummy
                            MATCH (gp:GreenPass) WHERE NOT EXISTS( (gp)<-[:HAS_A]-(:Person) )
                            DELETE gp""").single()

        db = get_db()
        db.write_transaction(get_clean)
        return {}, 204


class PersonCreate(Resource):
    def post(self):
        data = request.args
        tax = data.get('taxCode')
        name = data.get('name')
        surname = data.get('surname')

        def create_person(tx, tax_in, name_in, surname_in):
            return tx.run("""CREATE (person: Person {taxCode: $tax, name: $name, surname: $surname}
                            RETURN person
            """, {"tax": tax_in, "name": name_in, "surname": surname_in}).single()

        db = get_db()
        results =  db.write_transaction(create_person, tax, name, surname)
        user = results['person']
        return serialize_person(user), 201


api.add_resource(PlaceAmountPeop, '/api/v0/PlaceAmountPeop/')
api.add_resource(PlaceQuarPeop, '/api/v0/PlaceQuarPeop/<string:place_name>')  # Restaurant Alfredo
api.add_resource(DailyInfected, '/api/v0/DailyInfected/')
api.add_resource(InfectedHealed, '/api/v0/InfectedHealed/')
api.add_resource(GetDailyStamp, '/api/v0/GetDailyStamp/')
api.add_resource(MostVisited, '/api/v0/MostVisited/')
api.add_resource(SetPositive, '/api/v0/SetPositive')
api.add_resource(SetGreen, '/api/v0/SetGreen')
api.add_resource(DataCleaning, '/api/v0/clean/')
api.add_resource(PersonCreate, '/api/v0/PersonCreate/')
