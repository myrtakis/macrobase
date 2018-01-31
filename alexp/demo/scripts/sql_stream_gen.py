import random
import os
from urllib.parse import urlparse, parse_qs
import psycopg2
import sys
import time
import yaml


def parse_conf(conf_path):
    with open(conf_path, 'r') as f:
        conf = yaml.load(f)
    uri = conf['inputURI']
    uri = uri[uri.find(':')+1:]
    u = urlparse(uri)
    query = parse_qs(u.query)
    return u.path[1:], u.hostname, u.port, query['user'][0], query['password'][0], conf['table']


conf_path = os.path.dirname(os.path.realpath(__file__)) + '/../stream_sql.yaml'
if len(sys.argv) > 1:
    conf_path = sys.argv[1]

dbname, host, port, user, password, table = parse_conf(conf_path)

conn = psycopg2.connect("dbname='postgres'" +
                         (" host='" + host + "'") +
                         (" port="+str(port) if port else "") +
                         (" user="+user if user else "") +
                         (" password="+password if password else ""))

cur = conn.cursor()

firmwares = ["0.2.4", "0.3.1", "0.3.2", "0.4"]
models = ["M101", "M104", "M204", "M205", "M404", "M606"]
devices = [random.randint(100, 10000) for i in range(0, 100)]
if 2040 in devices:
    devices.remove(2040)

states = ["CA", "MA", "NY", "WY", "AR", "NV"]

print("Creating table...")

cur.execute("DROP TABLE IF EXISTS {0}; CREATE TABLE {0} ( reading_id bigint NOT NULL, device_id bigint NOT NULL, state varchar(2), model varchar(40), firmware_version varchar(40), temperature numeric, power_drain numeric, time bigint NOT NULL );".format(table))

print("...created!")

r = 0

while True:
    print("Inserting data")

    readings = 500

    for i in range(0, int(readings)+random.randint(1, readings / 2)):
        r += 1
        d_id = random.choice(devices)
        state = random.choice(states)
        model = random.choice(models)
        firmware_version = random.choice(firmwares)

        power_drain = .2+.2*random.random()

        if (state == "CA" and model == "M101" and firmware_version == "0.4"):
            temperature = 2 + random.random()*10
        else:
            temperature = 70+random.random()*10

        sql = "INSERT INTO %s VALUES ('%s', '%s', '%s', '%s', '%s', %f, %f, %d);" % (table, r, d_id, state, model, firmware_version, temperature, power_drain, time.time() * 1000)
        cur.execute(sql)

    d_id = 2040
    state = random.choice(states)
    model = random.choice(models)
    firmware_version = random.choice(firmwares)
    for i in range(0, int(readings*.01)+random.randint(1, readings / 2)):
        r += 1
        power_drain = .8 + random.random()*.2
        if (state == "CA" and model == "M101" and firmware_version == "0.4"):
            temperature = 2 + random.random()*10
        else:
            temperature = 70+random.random()*10

        sql = "INSERT INTO %s VALUES ('%s', '%s', '%s', '%s', '%s', %f, %f, %d);" % (table, r, d_id, state, model, firmware_version, temperature, power_drain, time.time() * 1000)
        cur.execute(sql)

    conn.commit()

    time.sleep(random.uniform(0.5, 2.5))
