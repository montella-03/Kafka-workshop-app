package com.example.demo.controller;

import com.example.demo.dto.TransactionRequest;
import com.example.demo.dto.TransactionResponse;
import com.example.demo.service.TransactionEventProducer;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/transactions")
public class TransactionController {

    private final TransactionEventProducer producer;

    public TransactionController(TransactionEventProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    public ResponseEntity<TransactionResponse> publish(@Valid @RequestBody TransactionRequest request) {
        producer.send(request);
        return ResponseEntity.accepted()
                .body(new TransactionResponse(
                        request.transactionId(),
                        "ACCEPTED",
                        "TransactionEvent published to Kafka"
                ));
    }
}
