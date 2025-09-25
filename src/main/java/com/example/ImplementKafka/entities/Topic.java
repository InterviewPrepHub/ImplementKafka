package com.example.ImplementKafka.entities;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/*
Handles a collection of partitions and logic to route messages to the correct partition using hash(key).
 */

@Getter
@Setter
public class Topic {

    private final UUID topicId;
    private final String name;
    private final List<Partition> partitions;

    public String getName() {
        return name;
    }

    public List<Partition> getPartitions() {
        return partitions;
    }

    public Topic(String name, int partitionCount) {
        this.topicId = UUID.randomUUID();
        this.name = name;
        this.partitions = new ArrayList<>();

        for (int i = 0; i < partitionCount; i++) {
            Partition partition = new Partition(topicId, i);
            partitions.add(partition);
        }
    }

    public Partition getPartitionByIndex(int index) {
        return partitions.get(index);
    }
}
