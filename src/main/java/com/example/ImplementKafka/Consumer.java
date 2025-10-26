package com.example.ImplementKafka;

import com.example.ImplementKafka.entities.Message;
import com.example.ImplementKafka.entities.Partition;
import com.example.ImplementKafka.entities.Topic;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/*
Each Consumer instance will:

Subscribe/unsubscribe to one or more topics
Internally track offset per partition
poll(topicName) returns next unread message from any partition

 */
public class Consumer {

    private final Set<String> subscribedTopics = ConcurrentHashMap.newKeySet();     //thread safe set of topic names consumer can subscribe to
    private final Map<String, Map<Integer, Long>> offsets = new ConcurrentHashMap<>();  // topic -> (partitionIndex -> offset)

    public void subscribe(String topicName) {
        Topic topic = TopicManager.getInstance().getTopic(topicName);
        if (topic == null) {
            throw new IllegalArgumentException("Topic not found: " + topicName);
        }
        subscribedTopics.add(topicName);

        // Initialize offset for all partitions to 0
        offsets.putIfAbsent(topicName, new ConcurrentHashMap<>());
        Map<Integer, Long> topicOffsets = offsets.get(topicName);   //partitionIndex -> offset
        for (Partition p : topic.getPartitions()) {
            topicOffsets.putIfAbsent(p.getIndex(), 0L);
        }
    }

    public void unsubscribe(String topicName) {
        subscribedTopics.remove(topicName);
        offsets.remove(topicName);
    }

    // Poll one message in round-robin from partitions of the topic
    public Message poll(String topicName) {
        //consumer cant accidentally read from a topic it hasn't subscribed to
        if (!subscribedTopics.contains(topicName)) {
            throw new IllegalStateException("Consumer not subscribed to topic: " + topicName);
        }

        Topic topic = TopicManager.getInstance().getTopic(topicName);
        Map<Integer, Long> topicOffsets = offsets.get(topicName);

        for (Partition partition : topic.getPartitions()) {
            int idx = partition.getIndex();
            long offset = topicOffsets.getOrDefault(idx, 0L);

            Message message = partition.getMessageAtOffset(offset);

            if (message != null) {
                topicOffsets.put(idx, offset + 1); // update offset
                return message;
            }
        }
        return null; // No new messages
    }

}

/*
An offset is the position of the next message the consumer should read in that partition.

🔁 Quick Example:

Let’s say a partition contains these messages:

Offset      Message
0           “Hello”
1           “World”
2           “How are you?”

	•	If a consumer’s offset is 0, the next message to read is "Hello".
	•	After reading it, the offset is incremented to 1.
	•	Now, the next message to read is "World" — and so on.

🔄 Offset is per consumer, per partition

Each consumer maintains:
	•	Its own offset
	•	Separately for each partition it consumes from

So even if 2 consumers read from the same partition, they may be at different offsets — reading at their own pace.


 */
