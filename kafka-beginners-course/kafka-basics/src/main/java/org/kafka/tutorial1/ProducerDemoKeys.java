package org.kafka.tutorial1;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
		// create Producer properties
		Properties properties = new Properties();
		//properties.setProperty("boostrap.servers", "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		//create produce record
		String key = "id_#0";
		ProducerRecord<String, String> record =
				new ProducerRecord<String, String>("first_topic", key, "Hello world");

		logger.info("key: " + key); // with key, the data is send always to the same partition
		// send data - asynchronous
		producer.send(record, new Callback() {

			public void onCompletion(RecordMetadata metadata, Exception e) {
				if (e == null) {
					// the record was sent
					logger.info("metadata received: time " + metadata.timestamp());
				} else {
					logger.error("error produced", e);
				}

			}
		}).get(); // make it synchronous - don't do it in production

		// flush data
		producer.flush();

		// flush and close producer
		producer.close();
	}
}
