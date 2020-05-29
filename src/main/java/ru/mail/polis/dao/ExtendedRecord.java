package ru.mail.polis.dao;

import java.nio.ByteBuffer;

public class ExtendedRecord {
    private final ByteBuffer value;
    private final long timestamp;
    private final boolean deleted;


    public ExtendedRecord(final ByteBuffer value, final long timestamp, final boolean deleted) {
        this.value = value;
        this.timestamp = timestamp;
        this.deleted = deleted;
    }


    public byte[] toBytes() {
        final char deletedChar = this.deleted ? 'd' : 'e';
        return ByteBuffer.allocate(Character.BYTES + Long.BYTES + value.remaining())
                .putChar(deletedChar).putLong(timestamp).put(value.duplicate()).array();
    }


    public static ExtendedRecord fromBytes(final byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return fromBytes(buffer);
    }


    public static ExtendedRecord fromBytes(final ByteBuffer buffer) {
        final char symbol = buffer.getChar();
        final long timestamp = buffer.getLong();
        return new ExtendedRecord(buffer, timestamp, symbol == 'd');
    }


    public boolean isDeleted() {
        return deleted;
    }


    public ByteBuffer getValue() {
        if (this.isDeleted()) {
            throw new IllegalStateException("Record is deleted");
        }
        return value;
    }


    public long getTimestamp() {
        return timestamp;
    }
}