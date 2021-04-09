from kafka.cluster import ClusterMetadata

connection = ClusterMetadata(bootstrap_servers=['13.126.242.56:9092'])
print(connection.topics())
