package com.ticketrush.gateway;

public class InvalidOrderRequestException extends RuntimeException {

    public InvalidOrderRequestException(String message) {
        super(message);
    }
}
