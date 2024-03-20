import requests
import os
import shutil
import json

# =============================================================================
# ============================== functions ====================================
# =============================================================================

def list_connectors(url_kafka_connect, expand_info=False):
    if (expand_info):
        url_list_connectors = url_kafka_connect + "connectors"
    else:
        url_list_connectors = url_kafka_connect + "connectors?expand=info"
    response_create_connector = requests.get(url=url_list_connectors,
                                             headers={"Content-Type":"application/json"})

    if (response_create_connector.status_code in [200, 201]):
        return response_create_connector.json()
    else:
        return None

def get_version_connector(url_kafka_connect):
    url_get_connect_version = url_kafka_connect
    response_get_connect_version = requests.get(url=url_get_connect_version,
                                                headers={"Content-Type":"application/json"})
    if (response_get_connect_version.status_code in [200, 201]):
        return response_get_connect_version.json()
    else:
        return None

def create_config_struct_connector(name_connector,
                                   name_topic,
                                   input_path,
                                   output_path,
                                   error_path):
    return json.dumps({
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
    })

def create_connector(url_kafka_connect, data):
    url_create_connectors = url_kafka_connect + "connectors"
    response_create_connector = requests.post(url=url_create_connectors,
                                              headers={"Content-Type":"application/json"},
                                              data=data)

    print("response_create_connector ", response_create_connector)
    print("response_create_connector ", response_create_connector.json())

    if (response_create_connector.status_code in [200, 201]):
        return response_create_connector.json()
    else:
        return None

def transfer_files_kafka_directories(list_path_files_origin, list_path_files_destination):
    if (len(list_path_files_origin) != len(list_path_files_destination)):
        return None
    try:
        for index_i, path_i in enumerate(list_path_files_origin):
            shutil.copy2(path_i, list_path_files_destination[index_i])
        return True
    except Exception as e:
        print(f"--> error in transfer files to kafka --> '{e}'")
        return None

def delete_connector(url_kafka_connect, name_connector):
    url_delete_connector_kafka = url_kafka_connect + f"connectors/{name_connector}/"

    response_delete_connector = requests.delete(url=url_delete_connector_kafka,
                                                headers={"Content-Type":"application/json"})

    print("delete_connector ", response_delete_connector)
    print("delete_connector ", response_delete_connector.json())

    if (response_delete_connector.status_code in [200, 201, 204]):
        return response_delete_connector.json()
    else:
        return None

# =============================================================================
# ============================== variables ====================================
# =============================================================================

origin_files_path      = os.getcwd() + "/csv_files/"
data_volume_kafka_path = "/data/"
data_volume_host       = os.getcwd() + "/data/"

order_files = ['file_00.csv',
               'file_01.csv',
               'file_02.csv',
               'file_03.csv',
               'file_04.csv',
               'file_05.csv']

url_kafka_connect = "http://localhost:8083/"

connector_name = "CONNECTOR_00"
topic_name     = "TOPIC_00"

input_path_kafka  = data_volume_kafka_path + "unprocessed"
output_path_kafka = data_volume_kafka_path + "processed"
error_path_kafka  = data_volume_kafka_path + "error"
input_path_host   = data_volume_host + "unprocessed"
output_path_host  = data_volume_host + "processed"
error_path_host   = data_volume_host + "error"

# =============================================================================
# ============================== create the kafka folders =====================
# =============================================================================

names_directories = ["unprocessed", "processed", "error"]

for directory_i in names_directories:
    if not os.path.exists(data_volume_host + f"{directory_i}"):
        os.mkdir(data_volume_host + f"{directory_i}")

response_list_connectors = list_connectors(url_kafka_connect, True)

print("\nList connectors --> ", response_list_connectors)

response_version_connect = get_version_connector(url_kafka_connect)

print("\nVersion connector --> ", response_version_connect)

struct_config_connector = create_config_struct_connector(connector_name,
                                                         topic_name,
                                                         input_path_kafka,
                                                         output_path_kafka,
                                                         error_path_kafka)

print("struct_config_connector --> ", struct_config_connector)


list_files_origin       = [origin_files_path + file_i for file_i in order_files]
list_files_destination  = [data_volume_host + "/unprocessed/" + file_i for file_i in order_files]
response_transfer_files = transfer_files_kafka_directories(list_files_origin, list_files_destination)

print("response_transfer_files ", response_transfer_files)

if (response_transfer_files):
    response_create_connector = create_connector(url_kafka_connect, struct_config_connector)
    print("response_create_connector ", response_create_connector)
