package ru.mail.polis.utils;

import java.io.IOException;

public class FastIOException extends IOException {
    public FastIOException(Exception e) {
        super("retrace", e);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}

