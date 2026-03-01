package com.example.demo.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Instant;

public record TransactionRequest(
        @NotBlank String transactionId,
        @NotBlank String userId,
        @NotNull @Positive Double amount,
        @NotBlank String currency,
        Instant timestamp
) {
}
