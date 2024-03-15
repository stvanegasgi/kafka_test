import requests
import os

# =============================================================================
# ============================== functions ====================================
# =============================================================================

def list_connectors(url_kafka_connect):
    url_list_connectors = url_kafka_connect + "connectors"
    response_create_connector = requests.get(url=url_list_connectors,
                                             headers={"Content-Type":"application/json"})

    if (response_create_connector.status_code in [200, 201]):
        return response_create_connector.json()
    else:
        return None

def create_config_struc_connector(name_connector,
                                  name_topic,
                                  input_path,
                                  output_path,
                                  error_path):
    return {
        "name": f"{name_connector}",
        "config": {
            "connector.class": "com.github.jcustenborder.kafka.connect.spooldir.SpoolDirCsvSourceConnector",
            "topic": f"{name_topic}",
            "input.path": f"{input_path}",
            "finished.path": f"{output_path}",
            "error.path": f"{error_path}",
            "input.file.pattern": ".*\\.csv",
            "schema.generation.enabled":"true",
            "csv.first.row.as.header":"true"
        }
    }

def create_connector(url_kafka_connect, data):
    url_create_connectors = url_kafka_connect + "connectors"
    response_create_connector = requests.post(url=url_create_connectors,
                                              headers={"Content-Type":"application/json"},
                                              data=data)

    print("response_create_connector ", url_create_connectors)
    print("response_create_connector ", url_create_connectors.json())

    if (response_create_connector.status_code in [200, 201]):
        return response_create_connector.json()
    else:
        return None
# =============================================================================
# ============================== variables ====================================
# =============================================================================

origin_files_path      = os.getcwd() + "/csv_files/"
data_volume_kafka_path = os.getcwd() + "/data/"

order_files = ['file_00.csv',
               'file_01.csv',
               'file_02.csv',
               'file_03.csv',
               'file_04.csv'
               'file_05.csv']

url_kafka_connect = "http://localhost:8083/"

connector_name = "CONNECTOR_00"
topic_name     = "TOPIC_00"
input_path     = data_volume_kafka_path + "unprocessed"
output_path    = data_volume_kafka_path + "processed"
error_path     = data_volume_kafka_path + "error"

# =============================================================================
# ============================== create the kafka folders =====================
# =============================================================================

names_directories = ["unprocessed", "processed", "error"]

for directory_i in names_directories:
    if not os.path.exists(data_volume_kafka_path + f"{directory_i}"):
        os.mkdir(data_volume_kafka_path + f"{directory_i}")

response = list_connectors(url_kafka_connect)
if (response != None):
    print("List connectors --> ", response)

struc_config = create_config_struc_connector(connector_name,
                                             topic_name,
                                             input_path,
                                             output_path,
                                             error_path)

print("input_path --> ", struc_config)