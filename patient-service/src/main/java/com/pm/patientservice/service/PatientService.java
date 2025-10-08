package com.pm.patientservice.service;

import billing.BillingServiceGrpc;
import com.google.api.Billing;
import com.pm.patientservice.Kafka.kafkaProducer;
import com.pm.patientservice.dto.PatientRequestDTO;
import com.pm.patientservice.dto.PatientResponseDTO;
import com.pm.patientservice.exception.EmailAlreadyExistsException;
import com.pm.patientservice.exception.PatientNoFoundException;
import com.pm.patientservice.grpc.BillingServiceGrpcClient;
import com.pm.patientservice.mapper.PatientMapper;
import com.pm.patientservice.model.Patient;
import com.pm.patientservice.repository.PatientRepository;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;


@Service
public class PatientService {
    private final PatientRepository patientRepository;
    private final BillingServiceGrpcClient billingServiceGrpcClient;
    private final kafkaProducer kafkaProducer;


    public PatientService(PatientRepository patientRepository, BillingServiceGrpcClient billingServiceGrpcClient,
                          kafkaProducer kafkaProducer) {
        this.patientRepository = patientRepository;
        this.billingServiceGrpcClient = billingServiceGrpcClient;
        this.kafkaProducer = kafkaProducer;
    }

    public List<PatientResponseDTO> getPatients() {
        List<Patient> patients = patientRepository.findAll();
        List<PatientResponseDTO> patientResponseDTOS = patients.stream().map(PatientMapper::toDTO).toList();

        return patientResponseDTOS;
    }

    public PatientResponseDTO createPatient(PatientRequestDTO patientRequestDTO) {
        if (patientRepository.existsByEmail(patientRequestDTO.getEmail())) {
            throw new EmailAlreadyExistsException("A Patient with this Email"
                    + "already exists" + patientRequestDTO.getEmail());
        }

        Patient patient = PatientMapper.toModel(patientRequestDTO);

        Patient Patient = patientRepository.save(patient);
        billingServiceGrpcClient.createBillingAccount(Patient.getId().toString(),
                Patient.getName(),Patient.getEmail());

        kafkaProducer.sendEvent(Patient);
        return PatientMapper.toDTO(Patient);
    }

    public PatientResponseDTO updatePatient(PatientRequestDTO patientRequestDTO, UUID uuid){
        Patient patient = patientRepository.findById(uuid).orElseThrow(()-> new PatientNoFoundException("Patient doesnot Exist"));
        patient.setName(patientRequestDTO.getName());
        patient.setEmail(patientRequestDTO.getEmail());
        patient.setAddress(patientRequestDTO.getAddress());
        patient.setDateOfBirth(LocalDate.parse(patientRequestDTO.getDateOfBirth()));

        patientRepository.save(patient);
        return PatientMapper.toDTO(patient);

    }

    public void deletePatient(UUID uuid) {
        patientRepository.findById(uuid).orElseThrow(()-> new PatientNoFoundException("Patient doesnot Exist"));
        patientRepository.deleteById(uuid);
    }
}
