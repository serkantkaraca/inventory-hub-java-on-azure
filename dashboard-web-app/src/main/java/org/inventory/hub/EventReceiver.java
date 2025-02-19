/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See LICENSE in the project root for
 * license information.
 */
package org.inventory.hub;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.codehaus.jackson.map.ObjectMapper;

import org.inventory.hub.controller.TransactionsController;
import org.inventory.hub.event.Transaction;

@Component
public class EventReceiver implements Runnable, ApplicationListener<ApplicationReadyEvent>
{   
    @Override
    public void onApplicationEvent(final ApplicationReadyEvent event)
    {
        Runnable notificationsReceiver = new EventReceiver();
        Thread receiverThread = new Thread(notificationsReceiver);
        receiverThread.start();
        System.out.println("====== Event Receiver Started =====");
    
        return;
    }

    public void run() 
    {
        String eventHubFqdn = System.getenv("NOTIFICATIONS_EVENT_HUB_FQDN");
        String eventHubName = System.getenv("NOTIFICATIONS_EVENT_HUB_NAME");
        String consumerGroupName = System.getenv("NOTIFICATIONS_EVENT_HUB_CONSUMER_GROUP_NAME");
        
        Properties properties = new Properties();
        
        // Consumer configuration
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, eventHubFqdn + ":9093");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaOAuthConsumer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupName);
        properties.put("auto.offset.reset", "latest");
        properties.put("session.timeout.ms", "30000");
        properties.put("request.timeout.ms", "60000");
        properties.put("security.protocol", "SASL_SSL");
 
        // OAuthBearer configuration
        properties.put("sasl.mechanism", "OAUTHBEARER");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
        properties.put("sasl.login.callback.handler.class", CustomAuthenticateCallbackHandler.class.getName());
        
        KafkaConsumer<String, byte[]> kConsumer = new KafkaConsumer<>(properties);
        kConsumer.subscribe(Arrays.asList(eventHubName));
        
        Transaction transaction;
        ObjectMapper objectMapper = new ObjectMapper();
        
        while (true)
        {
            ConsumerRecords<String, byte[]> receivedEvents = kConsumer.poll(Duration.ofSeconds(60));
            for (ConsumerRecord<String, byte[]> receivedEvent : receivedEvents)
            {
                try
                {
                    transaction = objectMapper.readValue(
                            new String(receivedEvent.value(), "UTF8"), Transaction.class);
                    TransactionsController.transactions.addFirst(transaction);
                    System.out.println("=== event data ===\n" + transaction.toString());
                }
                catch (Exception e)
                {
                    System.out.println("Processing failed for an event: " + e.toString());
                    e.printStackTrace();
                }
            }
        }
    }
}


