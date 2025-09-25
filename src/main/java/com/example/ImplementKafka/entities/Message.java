package com.example.ImplementKafka.entities;

import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

/*
Message - Atomic unit stored in partition
Immutable unit of data published to a partition by a producer.
 */

@Getter
@Setter
public class Message {

    private final UUID messageId;     // Unique identifier
    private final UUID partitionId;   // Foreign key to Partition
    private final String key;         // Optional key for partition selection
    private final String payload;     // Message body
    private final long timestamp;     // Epoch millis of message creation
    private final long offset;        // Auto-incremented offset within partition

    public Message(UUID partitionId, String key, String payload, long offset) {
        this.messageId = UUID.randomUUID();
        this.partitionId = partitionId;
        this.key = key;
        this.payload = payload;
        this.timestamp = System.currentTimeMillis();
        this.offset = offset;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public UUID getPartitionId() {
        return partitionId;
    }

    public String getKey() {
        return key;
    }

    public String getPayload() {
        return payload;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getOffset() {
        return offset;
    }
}


/*
private final String key;

in the Message class is optional metadata provided by the producer.
Its primary use is for partition selection — helping ensure that messages with the same key go to the same partition,
which allows for preserving message ordering for that key.

✅ Partitioning strategy:

When a producer sends a message, the system decides which partition to put the message into.

A common strategy is:

partitionIndex = Math.abs(key.hashCode()) % partitionCount

This means:
	•	All messages with the same key (like "user123") will always go to the same partition.
	•	This ensures their relative order is preserved, because message ordering is only guaranteed within a partition.
 */