package ru.mail.polis.utils;

import java.util.NoSuchElementException;

@SuppressWarnings("serial")
public class FastNoSuchElementException extends NoSuchElementException {

    @Override
    public synchronized Throwable fillInStackTrace() {
            return this;
    }
}
