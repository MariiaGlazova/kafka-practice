import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class myKafkaApplication {
    public static void main(String[] args) {
        runProducer();
        runConsumer();
    }

    static void runConsumer() {
        Consumer<Long, String> consumer = ConsumerCreator.createConsumer();

        while (Constants.RUNNING) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

            if (consumerRecords.count() == 0) {
                    break;
            }
            consumerRecords.forEach(record -> {
                System.out.println("_________________________");
                System.out.println("Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());
                System.out.println("_________________________");
            });
            consumer.commitAsync();
        }
        consumer.close();
    }

    static void runProducer() {
        Producer<Long, String> producer = ProducerCreator.createProducer();
        for (int i = 0; i < Constants.MESSAGE_COUNT; i++) {
            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(Constants.TOPIC_NAME,
                    "\"Okay, try to send you a message, honey " + i  + "\"");
            try {
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Send a message with key " + i + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());
            } catch (ExecutionException e) {
                System.out.println("I really need to catch this exc" + e);
            } catch (InterruptedException e) {
                System.out.println("I really need to catch this exc" + e);
            }
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException ex) {
                System.out.println("some bad news for you, sweetie");
            }
        }
    }

    static void runConsoleProducer() {
        Producer<Long, String> producer = ProducerCreator.createProducer();
        for (int i = 0; i < Constants.MESSAGE_COUNT; i++) {
            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(Constants.TOPIC_NAME,
                    "\"Okay, try to send a message, honey " + i  + "\"");
            try {
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Send a message with key " + i + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());
            } catch (ExecutionException e) {
                System.out.println("I really need to catch this exc" + e);
            } catch (InterruptedException e) {
                System.out.println("I really need to catch this exc" + e);
            }
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException ex) {
                System.out.println("some bad news for you, sweetie");
            }
        }
    }
    static void runConsoleConsumer() {
        Consumer<Long, String> consumer = ConsumerCreator.createConsumer();

        while (Constants.RUNNING) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(100);

            if (consumerRecords.count() == 0) {
                break;
            }

            consumerRecords.forEach(record -> {
                System.out.println("_________________________");
                System.out.println("Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());
                System.out.println("_________________________");
            });
            consumer.commitAsync();
        }
        consumer.close();
    }
}