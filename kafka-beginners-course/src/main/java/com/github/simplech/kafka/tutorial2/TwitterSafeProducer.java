package com.github.simplech.kafka.tutorial2;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterSafeProducer {
	Logger logger = LoggerFactory.getLogger(TwitterSafeProducer.class.getName());
	// Optional: set up some followings and track terms
	List<String> terms = Lists.newArrayList("bitcoin", "politics", "soccer");
	// public TwitterProducer() {}
	public static void main(String[] args) {
		new TwitterSafeProducer().run();
	}
	private String consumerKey = System.getenv("TWT_CONS_KEY");
	private String consumerSecret = System.getenv("TWT_CONS_SECRET");
	private String token = System.getenv("TWT_TOKEN");
	private String secret = System.getenv("TWT_TOKEN_SECRET");
	public void run() {
		logger.info("Setup");
		BlockingQueue<String> msgQueue = new LinkedBlockingDeque<>(1000);

		// create a twitter client
		Client twitterClient = this.createTwitterClient(msgQueue);

		// Attempts to establish a connection.
		twitterClient.connect();

		// create a kafka producer
		KafkaProducer<String, String> producer = createKafkaProducer();

		// add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread (() -> {
			logger.info("stopping application");
			logger.info("shutting down client from twitter");
			twitterClient.stop();
			logger.info("closing producer");
			producer.close();
			logger.info("done! application stopped");
		}));

		// loop to send tweets to kafka
		// on a different thread, or multiple different threads....
		while (!twitterClient.isDone()) {
			try {
				String msg = msgQueue.poll(5, TimeUnit.SECONDS);
				logger.info(msg);
				producer.send(new ProducerRecord<>("twitter_tweets",  null, msg), new Callback() {

					@Override
					public void onCompletion(RecordMetadata metadata, Exception e) {
						if (e != null) {
							logger.error("something bad happened", e);
						}
					}
				});
			} catch (InterruptedException e) {
				e.printStackTrace();
				twitterClient.stop();
			}
		}
		logger.info("end of application");

	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder()
				  .name("Hosebird-Client-01")                              // optional: mainly for the logs
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));

		return builder.build();
	}

	private KafkaProducer<String, String> createKafkaProducer() {
		// create Producer properties
		Properties properties = new Properties();

		//properties.setProperty("boostrap.servers", "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		//create safe producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // if kafka 2, otherwise use 1.

		//high throughput producer cgf (at the expense of a bit of latency and cpu usage)
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32 KB

		//create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		return producer;
	}
}
