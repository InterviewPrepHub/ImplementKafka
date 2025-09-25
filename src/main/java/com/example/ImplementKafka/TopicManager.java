package com.example.ImplementKafka;

import com.example.ImplementKafka.entities.Topic;
import java.util.HashMap;
import java.util.Map;

public class TopicManager {

    // Registry of all topics by name
    private final Map<String, Topic> topicMap = new HashMap<>();
    // Singleton instance
    private static TopicManager instance;

    private TopicManager() {

    }

    public static TopicManager getInstance() {
        if (instance == null) {
            instance = new TopicManager();
        }
        return instance;
    }

    // Create topic
    public synchronized void createTopic(String name, int partitionCount) {
        if (topicMap.containsKey(name)) {
            throw new IllegalArgumentException("Topic '" + name + "' already exists.");
        }
        Topic topic = new Topic(name, partitionCount);
        topicMap.put(name, topic);
        System.out.println("Topic created: " + name + " with " + partitionCount + " partitions.");
    }

    // Delete topic
    public synchronized void deleteTopic(String name) {
        if (!topicMap.containsKey(name)) {
            throw new IllegalArgumentException("Topic '" + name + "' does not exist.");
        }
        topicMap.remove(name);
        System.out.println("Topic deleted: " + name);
    }

    // Fetch topic
    public Topic getTopic(String name) {
        return topicMap.get(name);
    }

    // List all topics (optional)
    public Map<String, Topic> getAllTopics() {
        return topicMap;
    }


}
