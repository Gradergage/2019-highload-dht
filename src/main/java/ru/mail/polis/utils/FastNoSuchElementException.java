package ru.mail.polis.utils;

import java.util.NoSuchElementException;

public class FastNoSuchElementException extends NoSuchElementException {

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
