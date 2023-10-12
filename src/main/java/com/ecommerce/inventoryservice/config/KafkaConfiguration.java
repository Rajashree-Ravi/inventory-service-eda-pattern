package com.ecommerce.inventoryservice.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.ecommerce.inventoryservice.model.InventoryDto;
import com.ecommerce.inventoryservice.model.OrderDto;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {

	@Value("${producer.config.bootstrap-servers}")
	private String producerServers;

	@Value("${consumer.config.bootstrap-servers}")
	private String consumerServers;

	@Value("${consumer.config.group-id}")
	private String groupId;

	@Bean
	public ProducerFactory<String, InventoryDto> producerFactoryString() {
		Map<String, Object> configProps = new HashMap<>();

		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producerServers);
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

		return new DefaultKafkaProducerFactory<>(configProps);
	}

	@Bean
	public KafkaTemplate<String, InventoryDto> kafkaTemplateString() {
		return new KafkaTemplate<>(producerFactoryString());
	}

	@Bean
	public ConsumerFactory<String, OrderDto> consumerFactory() {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerServers);
		configProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
		configProps.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
		configProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.ecommerce.inventoryservice.model.OrderDto");

		return new DefaultKafkaConsumerFactory<>(configProps);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, OrderDto> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, OrderDto> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());

		return factory;
	}

}