from kafka import KafkaAdminClient
from kafka.admin import NewTopic
client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
topics_list = list()
topics_list.append(NewTopic(name="node-3", num_partitions=1, replication_factor=1))
client.create_topics(new_topics = topics_list)

