package com.ecommerce.inventoryservice.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.ecommerce.inventoryservice.exception.EcommerceException;
import com.ecommerce.inventoryservice.model.InventoryDto;
import com.ecommerce.inventoryservice.model.ItemDto;
import com.ecommerce.inventoryservice.model.OrderDto;
import com.ecommerce.inventoryservice.service.InventoryService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@Service
public class TopicListener {

	private final TopicProducer topicProducer;

	@Value("${consumer.config.topic.name")
	private String topicName;

	@Autowired
	InventoryService inventoryService;

	@KafkaListener(topics = "${consumer.config.topic.name}", groupId = "${consumer.config.group-id}")
	public void consume(ConsumerRecord<String, OrderDto> payload) {
		log.info("Topic : {}", topicName);
		log.info("Key : {}", payload.key());
		log.info("Headers : {}", payload.headers());
		log.info("Partion : {}", payload.partition());
		log.info("Order : {}", payload.value());

		OrderDto order = payload.value();
		for (ItemDto item : order.getItems()) {
			InventoryDto existingInventory = inventoryService.getInventoryById(item.getInventoryId());

			if (existingInventory.getVendorInventory() < item.getQuantity())
				throw new EcommerceException(
						"stock-unavailable", "Available stock in inventory id = " + existingInventory.getId()
								+ " is/are " + existingInventory.getVendorInventory(),
						HttpStatus.INTERNAL_SERVER_ERROR);

			int quantity = existingInventory.getVendorInventory() - item.getQuantity();
			existingInventory.setVendorInventory(quantity);

			InventoryDto updatedInventoryDto = inventoryService.updateInventory(existingInventory.getId(),
					existingInventory);

			if (updatedInventoryDto != null)
				topicProducer.send(updatedInventoryDto);
		}

	}

}