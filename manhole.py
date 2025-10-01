import time
import requests
import mysql.connector
from pymodbus.client.sync import ModbusSerialClient as ModbusClient
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder
import random
import datetime

#---- Modbus Configure -----/

method = "rtu"
port = "COM7"
baud = 115200
uid = 1
databits = 8
stopbits = 1
parity = "N"

# --- database Configuration ---/
db = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "cbipa55wd01!",
    database = "manhole_fix"
    )
cur = db.cursor(dictionary=True)

#---- Setup Modbus Client ---/
client = ModbusClient(method=method, port=port, baudrate=baud,
                      parity=parity, stopbits=stopbits, bytesize=databits)

registers = [
    {
        "label":"ph",
        "address": 0,
        "kalib" : 1
    },
    {
        "label":"do",
        "address": 2,
        "kalib" : 1
    },
    {
        "label":"temp",
        "address": 4,
        "kalib" : 1
    },
    {
        "label":"amon",
        "address": 6,
        "kalib" : 1
    },
    {
        "label":"nitrat",
        "address": 8,
        "kalib" : 1
    }
]

# sensors reader
def read_sensor():
    result = {}
    for reg in registers:
        try:
            rr = client.read_input_registers(reg["address"], 2, unit=uid)
            if rr.isError():
                print("[ERROR] Gagal baca:", reg["label"])
                result[reg["label"]] = None
            else:
                decoder = BinaryPayloadDecoder.fromRegisters(
                    rr.registers, byteorder=Endian.Big, wordorder=Endian.Little
                )
                value = decoder.decode_32bit_float()
                result[reg["label"]] = value * reg["kalib"]
        except Exception as e:
            print("[ERROR]", reg["label"], e)
            result[reg["label"]] = None
    return result

#----sensor reader dummy---/
#def read_sensor_dummy():
 #   result = {}
  #  for reg in registers:
   #     # generate random val 0 - 100 (can be change as range u want)
    #    label = reg["label"].lower()
     #   if label == "ph":
      #      value = round(random.uniform(6.0, 8.5), 3)
       # elif label == "amon":
        #    value = round(random.uniform(0.1, 10.0), 3)
        #elif label == "temp":
         #   value = round(random.uniform(20.0, 35.0), 3)
        #elif label == "nitrat":
         #   value = round(random.uniform(0.5, 50.0), 3)
        #else:
         #   value = round(random.uniform(0, 100), 3)
        #result[reg["label"]] = value
    #return result

# --- save to mysql ---/
def save_to_db(sensor_data):
    sql = """
    INSERT INTO value (ph, do, temp, amon, nitrat, timestamp)
    VALUES (%s, %s, %s, %s, %s, NOW())
    """
    cur.execute(sql, (
        sensor_data.get("ph"),
        sensor_data.get("do"),
        sensor_data.get("temp"),
        sensor_data.get("amon"),
        sensor_data.get("nitrat")
    ))
    db.commit()

# --- API Endpoint Setter ---/
url = "https://cbi-ng.mdtapps.id/api/v1/ticks/create_multi"
headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjo1MTksImV4cCI6MTc4MzQxMTYzMiwicGF5bG9hZCI6eyJzdGF0aW9uX2lkIjoyNTZ9LCJleHBpcmVkIjoiMjAyNi0wNy0wNyAxNTowNzoxMiArMDcwMCJ9.RqWM6dsxVd62IRKUeNNTwE6thwU7roS8hls0Pzjdla4"
    }

# --- get latest data from DB ---/
def get_latest_data():
    cur.execute("SELECT * FROM value ORDER BY id DESC LIMIT 1")
    return cur.fetchone()

# --- Send to Server ---/
def send_to_server(record):
    payload = {"multi":[]}
    created_at = record["timestamp"].strftime("%Y-%m-%d %H:%M:%S +0700")

    for sensor in registers:
        sensor_name = sensor["label"]
        value = record[sensor_name] if record[sensor_name] is not None else 0.0

        payload["multi"].append({
            "tick":{
                "module_id": "262",
                "station_id": "264",
                "sensor_name": sensor_name,
                "value": float(value),
                "created_at": created_at,
                "updated_at": created_at
            }
        })

    try:
        res = requests.post(url, json=payload, headers=headers)
        print("[CBI Portal]:", res.status_code, res.text)
    except Exception as e:
        print("[ERROR] send failed:", e)

# --- Main loop ---/
if __name__ == "__main__":
    last_sent_minute = None
    last_save_minute = None
    while True:
        ayena = datetime.datetime.now()
        if ayena.second == 0 and last_save_minute != ayena.minute:
            sensor_data = read_sensor()
            print("Data Read:", sensor_data)

            save_to_db(sensor_data)
            print("------------------ Saved to DB -------------------")

            last_save_minute = ayena.minute

        # DB latest data getter
        latest = get_latest_data()
        print("LATEST:", latest)

        # realtime clock checker
        now = datetime.datetime.now()

        # send to server every hours in 30 minutes in
        print("realtime minute:", now.minute)
        print("realtime second:", now.second)
        if now.minute in [0, 30] and now.second == 0 and last_sent_minute != now.minute:  # prevent double send
            print("Send data to Server:", now.strftime("%Y-%m-%d %H:%M:%S"))
            send_to_server(latest)
            last_sent_minute = now.minute
        
        # Preventing double send just in case
        time.sleep(1)


        # send to server
        #if latest:
         #   send_to_server(latest)