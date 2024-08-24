package com.example.demo;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloConsumer {

	private static final Logger log  = LoggerFactory.getLogger(HelloConsumer.class);
	
	public static void main(String[] args) {
		
		

		Properties props = new Properties();
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, AppConfigs.application_id);
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstapServer);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getClass());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getClass());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, AppConfigs.application_id);

		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
		kafkaConsumer.subscribe(Arrays.asList(AppConfigs.topicName));

		  log.info("Consume message successfully consumerd");
		while (true) {

			ConsumerRecords<String, String> consumerRecord = kafkaConsumer.poll(Duration.ofMillis(100));
			consumerRecord.forEach(consumer -> {
				if (consumer.value().contains("hello")) {
					System.out.println(consumer.value());
				} else {
					System.out.println(consumer.value());
				}

			});

		}
	
		
		

	}

}
