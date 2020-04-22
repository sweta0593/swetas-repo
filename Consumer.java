package com.example.kafka_for_beginners.kafka_p1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer {
		public static void main(String args[])
		{
			Logger logger=LoggerFactory.getLogger(Consumer.class);
			
			String bootstrapServer="127.0.0.1:9092";
			String groupId="my_third_application";
			String topic="first_topic";
			
			//create Consumer config
			Properties property=new Properties();
			property.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
			property.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			property.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
			property.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
			property.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

			//create consumer
			KafkaConsumer<String,String> consumer=new KafkaConsumer<String,String>(property);
			
			//subscribe consumer to our topics
			consumer.subscribe(Arrays.asList(topic));
			
			//poll for new data
			while(true)
			{
				ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(600));
				
				for(ConsumerRecord<String,String> record: records)
				{
					logger.info("Key: "+ record.key() + ",Value :"+ record.value());
					logger.info("Partition: "+ record.partition() + ",Offset :"+ record.offset());
				}
			}
			
			
		}
}
