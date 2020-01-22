import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class myKafkaApplication {
    public static void main(String[] args) {
        runProducer();
        runConsoleProducer(runConsoleConsumer());
        runConsumer();
    }

    static ConsumerRecords<Long, String> runConsoleConsumer() {
        Consumer<Long, String> consumer = ConsumerCreator.createConsumer(Constants.FIRST_TOPIC);
        ConsumerRecords<Long, String> consumerRecordsToSend = null;
        while (Constants.RUNNING) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
            if (consumerRecords.count() == 0) {
                    break;
            }
            consumer.commitAsync();
            consumerRecordsToSend = consumerRecords;
        }
        consumer.close();
        return  consumerRecordsToSend;
    }

    static void runProducer() {
        Producer<Long, String> producer = ProducerCreator.createProducer();
        for (int i = 0; i < Constants.MESSAGE_COUNT; i++) {
            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(Constants.FIRST_TOPIC,
                    "\"nani :c  " + i);
            send(producer, record);
            System.out.println("Trying to send a message " + record.value() + "\" to topic " + record.topic() );
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException ex) {
                System.out.println("some bad news for you, sweetie");
            }
        }
    }

    static void runConsoleProducer(ConsumerRecords<Long, String> consumerRecords) {
        Producer<Long, String> producer = ProducerCreator.createProducer();
        for (ConsumerRecord<Long, String> consumerRecord : consumerRecords){
            ProducerRecord<Long, String>  record =  new ProducerRecord<Long, String>(Constants.SECOND_TOPIC,
                    consumerRecord.value().toUpperCase());
            send(producer, record);
        }
    }
    static void send(Producer<Long, String> producer, ProducerRecord<Long, String>  record ){
        try {
            RecordMetadata metadata = producer.send(record).get();
        } catch (ExecutionException e) {
            System.out.println("I really need to catch this exc" + e);
        } catch (InterruptedException e) {
            System.out.println("I really need to catch this exc" + e);
        }
    }

    static void runConsumer() {
        Consumer<Long, String> consumer = ConsumerCreator.createConsumer(Constants.SECOND_TOPIC);
        while (Constants.RUNNING) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000000000);
            if (consumerRecords.count() == 0) {
                break;
            }

            consumerRecords.forEach(record -> {
                System.out.println("_________________________");
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