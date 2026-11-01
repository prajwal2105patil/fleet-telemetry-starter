package com.fleetbootcamp.telemetry.error;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Every error leaves the API as {"error": "...", "details" | "message": ...} with the right status code. */
@RestControllerAdvice
public class ApiErrors {

    /** Thrown by services for rules that annotations can't express (e.g. "ts is in the future"). */
    public static class ValidationFailedException extends RuntimeException {
        private final String field;

        public ValidationFailedException(String field, String message) {
            super(message);
            this.field = field;
        }

        public String field() {
            return field;
        }
    }

    public static class NotFoundException extends RuntimeException {
        public NotFoundException(String message) {
            super(message);
        }
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> invalidBody(MethodArgumentNotValidException ex) {
        List<Map<String, String>> details = ex.getBindingResult().getFieldErrors().stream()
                .map(fe -> Map.of("field", snake(fe.getField()), "message", String.valueOf(fe.getDefaultMessage())))
                .collect(Collectors.toList());
        return badRequest(details);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> unreadable(HttpMessageNotReadableException ex) {
        String field = "body";
        if (ex.getCause() instanceof MismatchedInputException mie && !mie.getPath().isEmpty()) {
            field = mie.getPath().stream().map(JsonMappingException.Reference::getFieldName)
                    .filter(f -> f != null).collect(Collectors.joining("."));
        }
        return badRequest(List.of(Map.of("field", field, "message", "invalid value or format")));
    }

    @ExceptionHandler(ValidationFailedException.class)
    public ResponseEntity<Map<String, Object>> rule(ValidationFailedException ex) {
        return badRequest(List.of(Map.of("field", ex.field(), "message", ex.getMessage())));
    }

    @ExceptionHandler({MissingServletRequestParameterException.class, MethodArgumentTypeMismatchException.class})
    public ResponseEntity<Map<String, Object>> badParam(Exception ex) {
        return ResponseEntity.badRequest().body(Map.of("error", "validation_failed", "message", ex.getMessage()));
    }

    @ExceptionHandler(NotFoundException.class)
    public ResponseEntity<Map<String, Object>> notFound(NotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "not_found", "message", ex.getMessage()));
    }

    private static ResponseEntity<Map<String, Object>> badRequest(List<Map<String, String>> details) {
        return ResponseEntity.badRequest().body(Map.of("error", "validation_failed", "details", details));
    }

    static String snake(String camel) {
        return camel.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase();
    }
}
