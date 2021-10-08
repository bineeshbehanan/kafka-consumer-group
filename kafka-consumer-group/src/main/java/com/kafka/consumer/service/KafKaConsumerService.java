package com.kafka.consumer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;


@Service
public class KafKaConsumerService 
{
	private final Logger logger 
		= LoggerFactory.getLogger(KafKaConsumerService.class);
	
	@KafkaListener(topics = "${topic.name}",
			groupId = "${consumer.group.id}")
	public void listenMessage1(
			@Payload String message,
			@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
		System.out.println(
				"CONSUMER1 :: Received Message: " + message
						+ " from partition: " + partition);
	}

	@KafkaListener(topics = "${topic.name}",
			groupId = "${consumer.group.id}")
	public void listenMessage2(
			@Payload String message,
			@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
		System.out.println(
				"CONSUMER2 :: Received Message: " + message
						+ " from partition: " + partition);
	}

	@KafkaListener(topics = "${topic.name}",
			groupId = "${consumer.group.id}")
	public void listenMessage3(
			@Payload String message,
			@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
		System.out.println(
				"CONSUMER3 :: Received Message: " + message
						+ " from partition: " + partition);
	}
}
