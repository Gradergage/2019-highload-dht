package ru.mail.polis.service;

public class Replicas {
    final int ack;
    final int from;
    Replicas(int ack, int from)
    {
        this.ack=ack;
        this.from=from;
    }

    public int getAck() {
        return ack;
    }

    public int getFrom() {
        return from;
    }
}
