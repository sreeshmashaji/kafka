from confluent_kafka.admin import AdminClient, NewTopic,KafkaException


def create_topic_if_not_exist(topic_names):
    admin_client=AdminClient({"bootstrap.servers":"localhost:9092"})
    topic_metadata=admin_client.list_topics(timeout=5)
    topics_to_create=[
        NewTopic(topic,num_partitions=2, replication_factor=1,)
        for topic in topic_names
        if topic not in topic_metadata.topics ]
    if topics_to_create:
        try:
            admin_client.create_topics(topics_to_create)
            for topic in topics_to_create:
                print(f"{topic} created")
        except KafkaException as e:
            print(f"Failed to create topics: {e}")
    else:
        print("topics already exist")
            