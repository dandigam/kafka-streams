package com.example.demo;

import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloProducer {

	private static final Logger logger = LoggerFactory.getLogger(HelloProducer.class);

	public static void main(String[] args) {

		logger.info("Creating Kafak Producer ....");

		Properties pros = new Properties();
		pros.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.application_id);
		pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstapServer);
		pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
		pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getClass());

		KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(pros);

		logger.info("Start sending messages...");
		for (int i = 0; i < AppConfigs.numEvents; i++) {

			kafkaProducer.send(new ProducerRecord<Integer, String>(AppConfigs.topicName, i, "Simple Message- " + i));

		}

		logger.info("Finished Sending messages. Closing Producer. ");
		kafkaProducer.close();

	}

}
