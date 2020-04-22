package com.example.kafka_for_beginners.kafka_p1;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {
	public static void main(String args[]) {
		// create producer properties
		Properties property = new Properties();
		String bootstrapServers = "127.0.0.1:9092";
		// property.setProperty("bootstrap.servers", bootstrapServers);
		// property.setProperty("key.serializer", StringSerializer.class.getName());
		// property.setProperty("value.serializer", StringSerializer.class.getName());

		property.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		property.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		property.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create producer

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(property);
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello Kafka!!");
		// send data
		producer.send(record);
		producer.flush();
		producer.close();
	}

}
