from kafka.cluster import ClusterMetadata

connection = ClusterMetadata()
print(connection.topics())
