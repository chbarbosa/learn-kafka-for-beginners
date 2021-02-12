package com.github.simplech.kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThread {
	public static void main(String[] args) {
		new ConsumerDemoWithThread().run();
	}
	private ConsumerDemoWithThread() {
		super();
	}
	private void run() {
		final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "my-sixth-application";
		CountDownLatch latch = new CountDownLatch(1);

		Runnable myConsumerRunnable = new ConsumerThread(bootstrapServers, groupId, "first_topic", latch);

		Thread myThread = new Thread(myConsumerRunnable);
		myThread.start();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Caught shutdown hook");
			//TODO falta algo
		}));

		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("application got interrupted", e);
		} finally {
			logger.info("application is closing");
		}
	}
	public class ConsumerThread implements Runnable {

		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;
		Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());


		public ConsumerThread(
				String bootstrapServers,
				String groupId,
				String topic,
				CountDownLatch latch
				) {
			super();
			this.latch = latch;

			// create consumer configs
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

			consumer = new KafkaConsumer<String, String>(properties);

			// subscribe consumer to our topic(s)
			consumer.subscribe(Arrays.asList(topic));
		}
		public void run() {
			try {
				// poll for new data
				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
					for (ConsumerRecord<String, String> record : records) {
						logger.info("key: " + record.key() + "value: " + record.value());
						logger.info("partition: " + record.partition() + "offset: " + record.offset());
					}
				}
			} catch (WakeupException e) {
				logger.info("shutdown signal received!");
			} finally {
				consumer.close();
				latch.countDown();
			}

		}
		public void shutdown() {
			// It'll interrupt consumer.poll()
			this.consumer.wakeup();

		}

	}
}
