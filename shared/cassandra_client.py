from cassandra.cluster import Cluster

class CassandraClient:
    def __init__(self, hosts):
        self.cluster = Cluster(hosts,protocol_version=4)
        self.session = self.cluster.connect()

    def get_session(self):
        return self.session

    def close(self):
        self.cluster.shutdown()

    def execute(self, query):
        return self.get_session().execute(query)
    
    def create_tables(self):
        self.execute(
            """CREATE KEYSPACE IF NOT EXISTS sensor WITH REPLICATION = { 
                'class' : 'SimpleStrategy', 
                'replication_factor' : 1 
                }
            """
        )

        self.execute(
            """CREATE TABLE IF NOT EXISTS sensor.sensor_temperature(
                id INT,
                last_seen TIMESTAMP,
                temperature FLOAT,
                PRIMARY KEY (id, last_seen))
            """
        )
        self.execute(
            """CREATE TABLE IF NOT EXISTS sensor.sensor_type(
                id INT,
                type text,
                PRIMARY KEY (type, id))
                """
        )
        self.execute(
            """CREATE TABLE IF NOT EXISTS sensor.sensor_battery(
                id INT PRIMARY KEY,
                battery_level decimal)
                """
        )
