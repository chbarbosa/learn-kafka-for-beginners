package com.github.simplech.kafka.tutorial2;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

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

public class TwitterProducer {
	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
	// public TwitterProducer() {}
	public static void main(String[] args) {
		new TwitterProducer().run();
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

		// loop to send tweets to kafka
		// on a different thread, or multiple different threads....
		while (!twitterClient.isDone()) {
			try {
				String msg = msgQueue.poll(5, TimeUnit.SECONDS);
				logger.info(msg);
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
		// Optional: set up some followings and track terms
		List<String> terms = Lists.newArrayList("bitcoin");
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
}
