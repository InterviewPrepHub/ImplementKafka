package com.example.ImplementKafka.entities;

import lombok.Getter;
import lombok.Setter;

import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/*
maintains an ordered, thread-safe queue of messages and handle offset incrementing
 */

@Getter
@Setter
public class Partition {

    private final UUID partitionId;
    private final UUID topicId;
    private final int index;     // The index of this partition within a topic (e.g., partition 0, 1, 2, …).
    private final Queue<Message> messages;
    private final AtomicLong offsetCounter;  //A thread-safe counter that assigns sequential offsets to messages.
                                             //ensuring atomic increment across multiple producer threads.
                                             //No need to use explicit synchronization

    public Partition(UUID topicId, int index) {
        this.partitionId = UUID.randomUUID();
        this.topicId = topicId;
        this.index = index;
        this.messages = new ConcurrentLinkedQueue<>();
        this.offsetCounter = new AtomicLong(0);
    }

    public UUID getPartitionId() {
        return partitionId;
    }

    public UUID getTopicId() {
        return topicId;
    }

    public int getIndex() {
        return index;
    }

    public Queue<Message> getMessages() {
        return messages;
    }


    /*
          a thread-safe counter that assigns sequential offsets to messages.
          This value is atomically incremented by the addMessage method.
     */
    public AtomicLong getOffsetCounter() {
        return offsetCounter;
    }

    /*
    The addMessage() method creates a new message and puts it into a partition’s queue.
    That’s business logic because it:
        -   Decides the offset
        -   Inserts into the correct partition’s storage
        -   Updates state of that partition
     */
    public Message addMessage(String key, String payload) {
        long offset = offsetCounter.getAndIncrement();
        Message message = new Message(partitionId, key, payload, offset);
        messages.add(message);
        return message;
    }

    /*
    3️⃣ Why it’s wrong to put addMessage() in Message
       If you put addMessage() inside Message:

        -   You’re making the parcel decide how it gets delivered
        -   (which is not the parcel’s job — it’s the warehouse’s job).

        -   You’d be mixing data (message fields) with behavior (managing partitions).
        -   This violates the Single Responsibility Principle (SRP):

        -   SRP says: A class should have only one reason to change.
        -   Message should only change if message structure changes (e.g., adding a new field).
        -   Partition logic changes shouldn’t force changes to Message.
     */


    public Message getMessageAtOffset(long offset) {
        synchronized (messages) {
            for (Message message : messages) {
                if (message.getOffset() == offset) {
                    return message;
                }
            }
        }
        return null;
    }

}




/*
private final AtomicLong offsetCounter;

Imagine a partition like a mailbox, and each message is a letter dropped into it.
	•	Each letter (message) is given a serial number (offset) — 0, 1, 2, 3…

offsetCounter is like a counter that keeps track of the next serial number to assign.

When a producer adds a message:
	1.	offsetCounter.getAndIncrement() is called.
	2.	This returns the next available number (say 5) and increments the counter.
	3.	That value (5) becomes the message’s offset.

Because it’s an AtomicLong, it’s thread-safe — multiple producers can add messages without messing up the numbering.

 */