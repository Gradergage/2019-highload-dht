package ru.mail.polis.utils;

import java.io.IOException;

public class FastIOException extends IOException {
    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}

