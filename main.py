import os
import ssl
import time
import logging
import paho.mqtt.client as mqtt
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from datetime import datetime
from passlib.hash import bcrypt
import pymysql
import psycopg2
import threading

# Configure logging
_logger = logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# FastAPI app for the REST API
app = FastAPI()

# Dictionary to store client last activity timestamps (in-memory cache)
connected_clients = {}

# Database Configuration from Environment Variables
DB_TYPE = os.getenv("DB_TYPE", "mysql")  # Default to MySQL
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME", "client_data_db")

# MQTT Configuration from Environment Variables
MQTT_PROTOCOL = os.getenv("MQTT_PROTOCOL", "tcp")  # Use 'tcp' or 'ws' (WebSocket)
MQTT_BROKER = os.getenv("MQTT_BROKER", "mqtt.eclipseprojects.io")
MQTT_PORT = int(os.getenv("MQTT_PORT", "8883" if MQTT_PROTOCOL == "tcp" else "8083"))
MQTT_TOPIC = "devices/+/status"
MQTT_VERSION = os.getenv("MQTT_VERSION", "v3.1.1")  # Default to MQTT v3.1.1
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")  # MQTT username
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")  # MQTT password

# SSL Configuration from Environment Variables
MQTT_USE_SSL = os.getenv("MQTT_USE_SSL", "false").lower() == "true"  # Use SSL if true
MQTT_CA_CERT = os.getenv("MQTT_CA_CERT", "")  # Path to CA certificate
MQTT_CLIENT_CERT = os.getenv("MQTT_CLIENT_CERT", "")  # Path to client certificate
MQTT_CLIENT_KEY = os.getenv("MQTT_CLIENT_KEY", "")  # Path to client key

# Determine MQTT version constant
def get_mqtt_version(version: str):
    version_map = {
        "v3.1": mqtt.MQTTv31,
        "v3.1.1": mqtt.MQTTv311,
        "v5": mqtt.MQTTv5,
    }
    return version_map.get(version.lower(), mqtt.MQTTv311)  # Default to v3.1.1

# Database Connection Function
def get_db_connection():
    if DB_TYPE == "mysql":
        return pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
    elif DB_TYPE == "postgresql":
        return psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )
    else:
        raise ValueError(f"Unsupported DB_TYPE: {DB_TYPE}")

# Database Initialization
def initialize_database():
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            create_table_query = """
                CREATE TABLE IF NOT EXISTS connected_clients (
                    id SERIAL PRIMARY KEY,
                    client_name VARCHAR(255) NOT NULL,
                    hashed_password VARCHAR(255) NOT NULL,
                    last_timestamp TIMESTAMP NOT NULL
                )
            """
            cursor.execute(create_table_query)
        connection.commit()
        logging.info("Database initialized successfully.")
    finally:
        connection.close()

# Save or Update Client Data in the Database
def save_client_to_database(client_id, hashed_password, timestamp):
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            # Check if the client already exists
            select_query = "SELECT id FROM connected_clients WHERE client_name = %s"
            cursor.execute(select_query, (client_id,))
            result = cursor.fetchone()

            if result:
                # Update the client's last timestamp
                update_query = "UPDATE connected_clients SET last_timestamp = %s WHERE client_name = %s"
                cursor.execute(update_query, (timestamp, client_id))
            else:
                # Insert new client data
                insert_query = "INSERT INTO connected_clients (client_name, hashed_password, last_timestamp) VALUES (%s, %s, %s)"
                cursor.execute(insert_query, (client_id, hashed_password, timestamp))
        connection.commit()
        logging.info(f"Client data saved: {client_id}")
    finally:
        connection.close()

# MQTT Callback: When a connection to the broker is established
def on_connect(client, userdata, flags, rc):
    logging.info(f"Connected to MQTT Broker with result code {rc}")
    # Subscribe to the topic
    client.subscribe(MQTT_TOPIC)

# MQTT Callback: When a message is received
def on_message(client, userdata, msg):
    topic = msg.topic

    logging.info(f"Received message on topic {topic}: {payload}")

    # Extract client ID from the topic (e.g., devices/<client_id>/status)
    client_id = str(client)
    password = "default_password"  # Simulated password (replace with actual password handling)
    hashed_password = bcrypt.hash(password)
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Update in-memory cache
    connected_clients[client_id] = timestamp

    # Save to database
    save_client_to_database(client_id, hashed_password, timestamp)

# MQTT Callback: When the client disconnects
def on_disconnect(client, userdata, rc):
    if rc != 0:
        logging.warning("Unexpected disconnection. Attempting to reconnect...")
        while True:
            try:
                client.reconnect()
                logging.info("Reconnected to MQTT Broker.")
                break
            except Exception as e:
                logging.error(f"Reconnection failed: {e}. Retrying in 5 seconds...")
                time.sleep(5)

# Start MQTT Client
def start_mqtt_client():
    client = mqtt.Client(
        transport="websockets" if MQTT_PROTOCOL == "ws" else "tcp",
        protocol=get_mqtt_version(MQTT_VERSION)
    )
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    # Set username and password
    if MQTT_USERNAME and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    # SSL/TLS configuration
    if MQTT_USE_SSL:
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        if MQTT_CA_CERT:
            ssl_context.load_verify_locations(cafile=MQTT_CA_CERT)
        if MQTT_CLIENT_CERT and MQTT_CLIENT_KEY:
            ssl_context.load_cert_chain(certfile=MQTT_CLIENT_CERT, keyfile=MQTT_CLIENT_KEY)
        client.tls_set_context(ssl_context)

    # Connect to the MQTT broker
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever()

# REST API: Health check
@app.get("/health")
async def health_check():
    return JSONResponse(content={"status": "healthy"})

# REST API: Get status of all devices
@app.get("/devices")
async def get_devices_status():
    devices_status = {
        client_id: {"last_active": timestamp}
        for client_id, timestamp in connected_clients.items()
    }
    return JSONResponse(content=devices_status)

if __name__ == "__main__":
    # Initialize the database
    initialize_database()

    # Start MQTT client in a separate thread
    mqtt_thread = threading.Thread(target=start_mqtt_client)
    mqtt_thread.daemon = True
    mqtt_thread.start()

    # Start FastAPI server
    import uvicorn
    logging.info("Starting FastAPI server...")
    uvicorn.run(app, host="0.0.0.0", port=5000)