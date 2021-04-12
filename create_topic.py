from kafka import KafkaAdminClient
from kafka.admin import NewTopic
try:
    client = KafkaAdminClient(bootstrap_servers=['13.126.242.56:9092'])
    topics_list = list()
    topics_list.append(NewTopic(name="node-3", num_partitions=1, replication_factor=1))
    try:
        # client.create_topics(new_topics = topics_list)
        # client.create_topics(new_topics = topics_list)
    except Exception as e:
        print("Creation error ")
        print(e)
except Exception as e:
    print("Initial config ")
    print(e)

