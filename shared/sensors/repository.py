from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional

from shared.redis_client import RedisClient
from shared.mongodb_client import MongoDBClient
from shared.elasticsearch_client import ElasticsearchClient
from shared.timescale import Timescale
from shared.cassandra_client import CassandraClient
import json
from . import models, schemas
from datetime import datetime

def get_sensor(db: Session, sensor_id: int, mongodb: MongoDBClient) -> Optional[models.Sensor]:
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    mongo_sensor = mongodb.get({"id": sensor_id})
    
    sensor = {
        "id" : db_sensor.id,
        "name": db_sensor.name,
        "latitude": mongo_sensor["location"]["coordinates"][0],
        "longitude": mongo_sensor["location"]["coordinates"][1],
        "type": mongo_sensor["type"],
        "mac_address": mongo_sensor["mac_address"],
        "manufacturer": mongo_sensor["manufacturer"],
        "model":mongo_sensor["model"],
        "serie_number": mongo_sensor["serie_number"], 
        "firmware_version": mongo_sensor["firmware_version"], 
        "description": mongo_sensor["description"],
        "joined_at" : db_sensor.joined_at.strftime("%m/%d/%Y, %H:%M:%S")
    }

    return sensor

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(sensor: schemas.SensorCreate, db: Session, mongodb: MongoDBClient, es: ElasticsearchClient) -> dict:
    db_sensor = models.Sensor(name=sensor.name) #Afegir el sensor en la base SQL
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    mydoc = { #Crear document amb les dades del sensor
        "id": db_sensor.id,
        #"longitude": sensor.longitude,
        #"latitude": sensor.latitude,
        "location": {
            "type": "Point",
            "coordinates": [sensor.longitude, sensor.latitude]
        },
        "type": sensor.type,
        "mac_address": sensor.mac_address,
        "manufacturer": sensor.manufacturer,
        "model": sensor.model,
        "serie_number": sensor.serie_number,
        "firmware_version": sensor.firmware_version,
        "description" :  sensor.description
    }
    mongodb.set(mydoc) #Afegir el sensor a la base mongoDB

    es_index_name = 'sensors'

    mongo_sensor = mongodb.get({"id": db_sensor.id})

    if not es.index_exists(es_index_name):
        es.create_index(es_index_name)
        mapping = {
            'properties': {
                "id": {'type': 'keyword'},
                "name": {'type': 'keyword'},
                "type": {'type': 'keyword'},
                "description": {'type': 'text'}
            }
        }
        es.create_mapping(es_index_name,mapping)

    es_doc = {
        "id" : db_sensor.id,
        "name" : sensor.name,
        "type" : sensor.type,
        "description" : sensor.description
    }
    es.index_document(es_index_name,es_doc)

    sensor = {
        "id" : db_sensor.id,
        "name": sensor.name,
        "latitude": mongo_sensor["location"]["coordinates"][0],
        "longitude": mongo_sensor["location"]["coordinates"][1],
        "type": mongo_sensor["type"],
        "mac_address": mongo_sensor["mac_address"],
        "manufacturer": mongo_sensor["manufacturer"],
        "model":mongo_sensor["model"],
        "serie_number": mongo_sensor["serie_number"], 
        "firmware_version": mongo_sensor["firmware_version"], 
        "description": mongo_sensor["description"]
    }

    return sensor

#metode per registrar nous dades, per parametre pasem les dues bases de dades, la id del sensor i les noves dades
def record_data(db: Session,redis: RedisClient, sensor_id: int, data: schemas.SensorData, mongodb:MongoDBClient, ts:Timescale, cassandra:CassandraClient) -> Optional[schemas.Sensor]:



    try: #control d'excepcions
        serialized_data = json.dumps(data.dict()) #serialitzem les dades per a que es pugui fer el set en radis
        redis.set(sensor_id,serialized_data) #cridem el metode setter per actualizar les dades 
        db_sensordata_serie = redis.get(sensor_id) #cridem el metode getter per obtenir les dades actualitzades del sensor
        db_sensordata = schemas.SensorData.parse_raw(db_sensordata_serie) #deserialitzem les dades per poder accedir a elles
        db_sensor = get_sensor(db,sensor_id, mongodb) #cridem el metode per obtenir el sensor actual    
        mongo_sensor=mongodb.get({"id": sensor_id})
        sensor = schemas.Sensor(id = db_sensor["id"], name = db_sensor["name"],
                                    latitude = mongo_sensor["location"]["coordinates"][0], longitude=mongo_sensor["location"]["coordinates"][1],
                                    joined_at=db_sensor["joined_at"], 
                                    last_seen=db_sensordata.last_seen, type=mongo_sensor['type'], mac_address=mongo_sensor["mac_address"],
                                    temperature=db_sensordata.temperature, 
                                    humidity=db_sensordata.humidity, battery_level=db_sensordata.battery_level,
                                    velocity=db_sensordata.velocity,
                                    description=mongo_sensor["description"]) #creem un nou sensor amb totes les dades    
        temperature = "NULL"
        humidity = "NULL"
        velocity = "NULL"

        if data.temperature and data.humidity:
            temperature = data.temperature
            humidity = data.humidity
        if data.velocity:
            velocity = data.velocity
        query = f"INSERT INTO sensor_data (id, velocity, temperature, humidity, last_seen, battery_level) VALUES ({sensor_id}, {velocity}, {temperature}, {humidity}, '{data.last_seen}', {data.battery_level})"
        ts.execute(query)
        ts.conn.commit()   
        cassandra.create_tables()
        if data.temperature is not None:
            query_temp = f"INSERT INTO sensor.sensor_temperature (id, last_seen, temperature) VALUES ({sensor_id}, '{data.last_seen}', {data.temperature})"
            cassandra.execute(query_temp)
        query_type = f"INSERT INTO sensor.sensor_type (id, type) VALUES ({sensor_id}, '{mongo_sensor['type']}')"
        cassandra.execute(query_type)
        query_battery = f"INSERT INTO sensor.sensor_battery (id, battery_level) VALUES ({sensor_id}, {data.battery_level})"
        cassandra.execute(query_battery)
        return sensor 
    except:
        raise HTTPException(status_code=404, detail="Sensor not found") #excepció en cas de que no existi el sensor

def get_temperature_values(db: Session, cassandra:CassandraClient, mongodb: MongoDBClient):
    query = """
    SELECT id,
        MAX(temperature) AS max_value,
        MIN(temperature) AS min_value,
        AVG(temperature) AS avg_value
    FROM sensor.sensor_temperature
    GROUP BY id;
    """
    sensors = cassandra.execute(query)
    resultat = []
    for sensor in sensors:
        db_sensor = get_sensor(db,sensor[0], mongodb)
        resultat.append({"id": sensor[0], 
                         "name": db_sensor["name"], 
                         "latitude": db_sensor["latitude"], 
                         "longitude": db_sensor["longitude"], 
                         "type": db_sensor["type"], 
                         "mac_address": db_sensor["mac_address"], 
                         "manufacturer": db_sensor["manufacturer"], 
                         "model":db_sensor["model"], 
                         "serie_number": db_sensor["serie_number"], 
                         "firmware_version": db_sensor["firmware_version"], 
                         "description": db_sensor["description"], 
                         "values": [{"max_temperature": sensor[1],
                                    "min_temperature": sensor[2], 
                                    "average_temperature": sensor[3]}]})
    
    return {"sensors":resultat}

def get_sensors_quantity(db: Session, cassandra:CassandraClient):
    query = """SELECT type, COUNT(*) AS type_count
        FROM sensor.sensor_type
        GROUP BY type;"""
    result = cassandra.execute(query)
    types = []
    for type in result:
        types.append(
            {"type": type[0], "quantity": type[1]}
        )
    
    return {"sensors":types}

def get_low_battery_sensors(db: Session, cassandra:CassandraClient, mongodb: MongoDBClient):
    query = """SELECT *
        FROM sensor.sensor_battery
        WHERE battery_level < 0.2
        ALLOW FILTERING;"""
    result = cassandra.execute(query)
    resultat = []
    for sensor in result:
        db_sensor = get_sensor(db,sensor[0], mongodb)
        resultat.append({"id": sensor[0], 
                         "name": db_sensor["name"], 
                         "latitude": db_sensor["latitude"], 
                         "longitude": db_sensor["longitude"], 
                         "type": db_sensor["type"], 
                         "mac_address": db_sensor["mac_address"], 
                         "manufacturer": db_sensor["manufacturer"], 
                         "model": db_sensor["model"], 
                         "serie_number": db_sensor["serie_number"], 
                         "firmware_version": db_sensor["firmware_version"], 
                         "description": db_sensor["description"], 
                         "battery_level": sensor[1]})
    return {"sensors":resultat}

#metode per obtenir les dades del sensor
def get_data(db: Session,redis: RedisClient, sensor_id: int, mongodb:MongoDBClient, ts:Timescale, from_date:Optional[datetime], to_date:Optional[datetime], bucket:Optional[str]):
    try:
        # db_sensordata_serie = redis.get(sensor_id)
        # db_sensordata_serie = redis.get(sensor_id) #cridem el metode getter per obtenir les dades actualitzades del sensor
        # db_sensordata = schemas.SensorData.parse_raw(db_sensordata_serie) #deserialitzem les dades per poder accedir a elles
        # db_sensor = get_sensor(db,sensor_id,mongodb) #cridem el metode per obtenir el sensor actual
        # mongo_sensor=mongodb.get({"id":sensor_id})
        # if mongo_sensor is None:
        #     return "Sensor not found"
        # sensor = schemas.Sensor(id = db_sensor["id"], name = db_sensor["name"],
        #                             latitude = mongo_sensor["location"]["coordinates"][0], longitude=mongo_sensor["location"]["coordinates"][1],
        #                             joined_at=db_sensor["joined_at"], 
        #                             last_seen=db_sensordata.last_seen, type=mongo_sensor["type"], mac_address=mongo_sensor["mac_address"],
        #                             temperature=db_sensordata.temperature, 
        #                             humidity=db_sensordata.humidity, battery_level=db_sensordata.battery_level,
        #                             velocity=db_sensordata.velocity,
        #                             description=mongo_sensor["description"]) #creem un nou sensor amb totes les dades  
        
        query = f"""
            SELECT
                time_bucket('1{bucket}', last_seen) AS bucket_time
            FROM
                sensor_data
            WHERE
                id = {sensor_id} AND
                last_seen >= '{from_date}' AND
                last_seen <= '{to_date}'
            GROUP BY
                bucket_time
            ORDER BY
                bucket_time;
            """
        ts.execute(query)
        result = ts.cursor.fetchall()
        return result 
    except:
        raise HTTPException(status_code=404, detail="Sensor not found") #excepció en cas de que no existi el sensor```

def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor

def get_sensors_near(mongodb: MongoDBClient, latitude: float, longitude: float, radius: int, db: Session, redis: RedisClient):
    try:
        mongodb.getDatabase("sensors")
        collection = mongodb.getCollection("sensorsData")
        collection.create_index([("location","2dsphere")])
        sensors = collection.find(
                {
                "location":
                    { "$near" :
                        {
                            "$geometry":{"type": "Point",
                                        "coordinates": [ longitude, latitude ] 
                                        },
                            "$maxDistance": radius
                        }
                    }
                }
        )
        sensors = list(sensors)
        near_sensors = []
        for sensor in sensors:
            near_sensors.append(get_data(db=db, redis=redis, sensor_id=sensor["id"],mongodb=mongodb))
        if near_sensors is None:
            return []
        return near_sensors
    except:
        raise HTTPException(status_code=404, detail="Sensor not found") #excepció en cas de que no existi el sensor
    
def search_sensors(query: str, size: int, search_type: str, db: Session, mongodb: MongoDBClient, es: ElasticsearchClient):
    #db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    #mongo_sensor = mongodb.get({"id": sensor_id})
    result = []
    es_index = 'sensors' 
    
    query_dict = eval(query)
    query_type = list(query_dict.keys())[0]
    query_value = query_dict[query_type]

    if search_type == "similar":
        search_query = {
            "query":{
                "fuzzy" :{
                    query_type:{
                        "value": query_value,
                        "fuzziness": "AUTO"
                    }
                }
            }
        }
    else:
        search_query = {
            "query":{
                search_type :{
                    query_type:query_value
                }
            }
        }
    es_sensor = es.search(index_name=es_index, query=search_query)

    for hit in es_sensor['hits']['hits']:
        if len(result) == size:
            break
        data = hit['_source']
        id = data['id']
        sensor = get_sensor(db,id,mongodb)
        result.append(sensor)
    return result