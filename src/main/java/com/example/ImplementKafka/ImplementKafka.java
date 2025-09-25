package com.example.ImplementKafka;

import com.example.ImplementKafka.entities.Message;
import com.example.ImplementKafka.entities.Partition;
import com.example.ImplementKafka.entities.Topic;

/*
Design & implement in memory message bus system like apache kafka in java

requirements:

topic management -
- create & delete topic
- topic can have multiple partition

producer API
- publish message to specified topic
- partition selection based on key.hashcode % partition_count(random hash)

consumer API
- subscribe and unsubscribe  from topics
- poll for messages on subscribed topic
- maintain offset tracking per consumer

message ordering

- message within single partition must maintain their order
- no ordering gaurantees acriss different partitions

concurrency
- thread safe implementations for all components
support for concurrent producers and consumer

user multi class design, thread safety, design pattern & system level complexity
and use the right data structures

 */
public class ImplementKafka {

    public static void main(String[] args) {
        TopicManager topicManager = TopicManager.getInstance();

        //create topic
        topicManager.createTopic("testTopic", 3);

        Producer producer = new Producer();
        producer.publish("testTopic", "key1", "message1");
        producer.publish("testTopic", "key1", "message2");
        producer.publish("testTopic", "key2", "message3");

        //get topic
        Topic topic = topicManager.getTopic("testTopic");

        for (Partition partition : topic.getPartitions()) {
            System.out.println("Partition: " + partition.getIndex() + ", Messages: " + partition.getMessages());
        }


        Consumer consumer = new Consumer();
        //track subscription
        consumer.subscribe("testTopic");

        Message m1 = consumer.poll("testTopic");
        System.out.println("Message: " + m1.getPayload());

        Message m2 = consumer.poll("testTopic");
        System.out.println("Message: " + m2.getPayload());
    }
}
