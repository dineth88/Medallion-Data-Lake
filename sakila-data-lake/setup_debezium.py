import requests
import json
import time

DEBEZIUM_URL = "http://localhost:8083"

def wait_for_debezium():
    print("Checking Debezium status...")
    for i in range(30):
        try:
            response = requests.get(f"{DEBEZIUM_URL}/")
            if response.status_code == 200:
                print("✓ Debezium is ready")
                return True
        except:
            pass
        time.sleep(1)
    return False

def create_mysql_connector():
    print("\nCreating MySQL CDC connector...")
    
    config = {
        "name": "sakila-mysql-connector",
        "config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "database.hostname": "host.docker.internal",
            "database.port": "3306",
            "database.user": "debezium",
            "database.password": "debezium123",
            "database.server.id": "184054",
            "topic.prefix": "sakila_cdc",
            "database.include.list": "sakila",
            "table.include.list": "sakila.customer,sakila.film,sakila.payment,sakila.rental",
            "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
            "schema.history.internal.kafka.topic": "schema-changes.sakila",
            "include.schema.changes": "true"
        }
    }
    
    try:
        response = requests.post(
            f"{DEBEZIUM_URL}/connectors",
            headers={"Content-Type": "application/json"},
            data=json.dumps(config)
        )
        
        if response.status_code in [200, 201]:
            print("✓ MySQL connector created!")
            print(json.dumps(response.json(), indent=2))
        else:
            print(f"✗ Failed: {response.text}")
            
    except Exception as e:
        print(f"✗ Error: {e}")

def check_connector_status():
    print("\nChecking connector status...")
    try:
        response = requests.get(f"{DEBEZIUM_URL}/connectors/sakila-mysql-connector/status")
        status = response.json()
        print(f"State: {status['connector']['state']}")
        if 'tasks' in status:
            for task in status['tasks']:
                print(f"Task: {task['state']}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if wait_for_debezium():
        create_mysql_connector()
        time.sleep(3)
        check_connector_status()
        
        print("\n" + "="*60)
        print("✓ CDC SETUP COMPLETE!")
        print("="*60)
        print("\nTest it:")
        print("1. Monitor CDC topic:")
        print("   docker exec -it kafka kafka-console-consumer \\")
        print("     --bootstrap-server localhost:9092 \\")
        print("     --topic sakila_cdc.sakila.customer \\")
        print("     --from-beginning")
        print("\n2. Make a change in MySQL:")
        print("   UPDATE customer SET first_name='TEST_CDC' WHERE customer_id=1;")
        print("="*60)