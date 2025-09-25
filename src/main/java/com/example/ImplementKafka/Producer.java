package com.example.ImplementKafka;

import com.example.ImplementKafka.entities.Partition;
import com.example.ImplementKafka.entities.Topic;

public class Producer {

    public void publish(String topicName, String key, String payload) {

        //get the topic
        Topic topic = TopicManager.getInstance().getTopic(topicName);

        if (topic == null) {
            throw new IllegalArgumentException("Topic "+topic.getName()+" does not exist");
        }

        //get the partition count
        int partitionCount = topic.getPartitions().size();

        //get the partition index
        //Partition selection logic based on key hash
        int partitionIndex = 0;
        if (key != null) {
            partitionIndex = Math.abs(key.hashCode()) % partitionCount;
        }

        //get the partition
        Partition targetPartition = topic.getPartitionByIndex(partitionIndex);

        //add the message
        targetPartition.addMessage(key, payload);
    }
}

/*
1. get topic
2. get partition count from topic
3. pick partition from partition index formula
4. create message
5. add message in the target partition
 */