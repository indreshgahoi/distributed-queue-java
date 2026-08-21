package io.github.indreshgahoi.queue;

import java.io.IOException;

public class WalException extends RuntimeException {
    public WalException(String message) {
        super(message);
    }
    public WalException(String s, Throwable cause) {
        super(s, cause);
    }
}
