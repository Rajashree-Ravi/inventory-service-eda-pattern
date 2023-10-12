package com.ecommerce.inventoryservice.messaging;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.ecommerce.inventoryservice.model.InventoryDto;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class TopicProducer {

	@Value("${producer.config.topic.name}")
	private String topicName;

	private final KafkaTemplate<String, InventoryDto> kafkaTemplate;

	public void send(InventoryDto inventory) {
		log.info("Payload : {}", inventory.toString());
		kafkaTemplate.send(topicName, inventory);
	}

}