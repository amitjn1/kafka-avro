package com.example;

//import com.example.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.commons.compress.utils.Charsets;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringSerializer;


import java.util.Properties;

public class KafkaAvroProducerV1 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        // normal producer
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        Producer<String, Customer> producer = new KafkaProducer<String, Customer>(properties);

        String topic = "customer-avro";

        // copied from avro examples
        Customer customer = Customer.newBuilder()
                .setAge(34)
                .setAutomatedEmail(false)
                .setFirstName("Jane3")
                .setLastName("Bo3")
                .setHeight(168f)
                .setWeight(65f)
                .build();

        ProducerRecord<String, Customer> producerRecord = new ProducerRecord<String, Customer>(
                topic, customer
        );

        System.out.println(customer);

        Headers headers = producerRecord.headers();
        headers.add("HdrKey1", "HdrVal1".getBytes(Charsets.UTF_8));
        headers.add("HdrKey2", "HdrVal2".getBytes(Charsets.UTF_8));
        headers.add("HdrKey3", "HdrVal3".getBytes(Charsets.UTF_8));

        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    System.out.println(metadata);
                } else {
                    exception.printStackTrace();
                }
            }
        });

        producer.flush();
        producer.close();


    }
}
