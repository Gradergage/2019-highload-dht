package ru.mail.polis.utils;

import java.io.IOException;

@SuppressWarnings("serial")
public class FastIOException extends IOException {
    public FastIOException(final Exception e) {
        super("retrace", e);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
            return this;
    }
}

