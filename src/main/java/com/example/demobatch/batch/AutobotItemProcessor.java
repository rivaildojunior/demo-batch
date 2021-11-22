package com.example.demobatch.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import com.example.demobatch.model.Autobot;

public class AutobotItemProcessor implements ItemProcessor<Autobot, Autobot> {

    private static final Logger log = LoggerFactory.getLogger(AutobotItemProcessor.class);

    @Override
    public Autobot process(Autobot autobot) throws Exception {
        final String firstName = autobot.getName().toUpperCase();
        final String lastName = autobot.getCar().toUpperCase();

        final Autobot transformed = new Autobot(firstName, lastName);

        log.info("Converting (" + autobot + ") into (" + transformed + ")");

        return transformed;
    }
}