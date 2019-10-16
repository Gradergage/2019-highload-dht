package ru.mail.polis.utils;

import java.util.NoSuchElementException;

@SuppressWarnings("serial")
public class FastNoSuchElementException extends NoSuchElementException {

    @Override
    public Throwable fillInStackTrace() {
        synchronized(this) {
            return this;
        }
    }
}
