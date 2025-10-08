package com.pm.patientservice.Kafka;
import ch.qos.logback.classic.Logger;
import com.pm.patientservice.model.Patient;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import patient.events.PatientEvent;


@Service
public class kafkaProducer {

    private final KafkaTemplate<String,byte[]> kafkaTemplate;
    private static final Logger logger = (Logger) LoggerFactory.getLogger(kafkaProducer.class);

    public kafkaProducer(KafkaTemplate<String,byte[]> kafkaTemplate){
        this.kafkaTemplate=kafkaTemplate;
    }

    public void sendEvent(Patient patient){
        PatientEvent event = PatientEvent.newBuilder()
                .setPatientId(patient.getId().toString())
                .setName(patient.getName())
                .setEmail(patient.getEmail())
                .setEventType("PAITENT_CREATED")
                .build();

        try{
            kafkaTemplate.send("patient",event.toByteArray());
        }
        catch (Exception e){
            logger.error("Error Sending PatientCreated event: {}",e);
        }
    }


}
