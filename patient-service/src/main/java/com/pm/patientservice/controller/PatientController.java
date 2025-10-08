package com.pm.patientservice.controller;

import com.pm.patientservice.dto.PatientRequestDTO;
import com.pm.patientservice.dto.PatientResponseDTO;
import com.pm.patientservice.dto.Validators.CreatePatientValidationGroup;
import com.pm.patientservice.service.PatientService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.groups.Default;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/patients")
@Tag(name = "Patients", description = "API managing the Patients")
public class PatientController {
    private final PatientService patientService;

    public PatientController(PatientService patientService) {
        this.patientService = patientService;
    }

    @GetMapping
    @Operation(summary = "Get all Patients")
    public ResponseEntity<List<PatientResponseDTO>> getAllPatients() {
        List<PatientResponseDTO> patients = patientService.getPatients();
        return ResponseEntity.ok().body(patients);
    }

    @PostMapping
    @Operation(summary = "Create new Patient")
    public ResponseEntity<PatientResponseDTO> CreatePatient(@Validated({CreatePatientValidationGroup.class}) @RequestBody PatientRequestDTO patientRequestDTO) {
        PatientResponseDTO patientResponseDTO = patientService.createPatient(patientRequestDTO);
        return ResponseEntity.ok().body(patientResponseDTO);
    }

    @PutMapping("/{id}")
    @Operation(summary ="Update the existing Patient")
    public  ResponseEntity<PatientResponseDTO> UpdatePatient(@Validated({Default.class}) @RequestBody PatientRequestDTO patientRequestDTO, @PathVariable("id") UUID id) {
        PatientResponseDTO patientResponseDTO = patientService.updatePatient(patientRequestDTO, id);
        return ResponseEntity.ok().body(patientResponseDTO);
    }

    @DeleteMapping("{id}")
    @Operation(summary = "Delete the existing Patient")
    public  ResponseEntity<Void> DeletePatient(@PathVariable("id") UUID id) {
        patientService.deletePatient(id);
        return ResponseEntity.noContent().build();
    }
}
