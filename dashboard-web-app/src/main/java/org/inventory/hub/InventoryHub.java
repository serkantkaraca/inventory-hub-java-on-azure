/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See LICENSE in the project root for
 * license information.
 */
package org.inventory.hub;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

//@EnableBinding(Sink.class)
@SpringBootApplication
public class InventoryHub extends SpringBootServletInitializer {

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(InventoryHub.class);
    }
    
    public static void main(String[] args) throws Exception {
        SpringApplication.run(InventoryHub.class, args);
    }

    /*@StreamListener(target=Sink.INPUT)
	public void receiveTransaction(String transactionMessage) {

        System.out.println("message is: " + transactionMessage);

        try
        {
            transaction = objectMapper.readValue(
                new String(transactionMessage.getBytes(), "UTF8"), Transaction.class);

            TransactionsController.transactions.addFirst(transaction);

            System.out.println("=== event data ===\n" + transaction.toString());

        } catch (Exception e)
        {
            System.out.println("Processing failed for an event: " + e.toString());
			e.printStackTrace();
        }

    }*/

    /*@StreamListener("errorChannel")
	public void errorGlobal(Message<?> message) {
		System.out.println("Handling ERROR: " + message);
	}*/
}
