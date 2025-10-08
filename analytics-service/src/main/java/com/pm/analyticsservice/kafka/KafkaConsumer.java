package com.pm.analyticsservice.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import patient.events.PatientEvent;

@Service
public class KafkaConsumer {
    Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
    @KafkaListener(topics="patient",groupId = "analytics-service")
    public void consumeEvent(byte[] event){
        try {
            PatientEvent patientEvent = PatientEvent.parseFrom(event);
            logger.info("Raw PatientEvent: {}", patientEvent);
            logger.info("Received patient event: [PatientId = {}, PatientName = {}, PatientEmail = {}]",
                    patientEvent.getPatientId(),
                    patientEvent.getName(),
                    patientEvent.getEmail());
            logger.info(">>> Kafka listener triggered, raw bytes length={}", event.length);



            logger.info("Received patient event: [PatientId = {} , PatientName = {} , "
        +" PatientEmail={}]",patientEvent.getPatientId(),patientEvent.getName(),patientEvent.getEmail());

        } catch (InvalidProtocolBufferException e)
        {
            logger.error("Error deserializing event {}",e.getMessage());
        }
    }

}
