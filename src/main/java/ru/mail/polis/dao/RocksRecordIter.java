package ru.mail.polis.dao;

import org.rocksdb.RocksIterator;
import ru.mail.polis.Record;
import ru.mail.polis.utils.RocksByteBufferUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Iterator.
 * Used as wrapping upon {@link RocksIterator}, which operating byte[] arrays,
 * to work with {@link Record}.
 *
 * @author Pavel Pokatilo
 **/
public final class RocksRecordIter implements Iterator<Record>, Closeable {
    private final RocksIterator rocksIterator;

    private RocksRecordIter(final RocksIterator rocksIterator, final ByteBuffer from) {
        this.rocksIterator = rocksIterator;
        if (from == null) {
            this.rocksIterator.seekToFirst();
        } else {
            this.rocksIterator.seek(RocksByteBufferUtils.copyByteBuffer(from));
        }
    }

    public static Iterator<Record> getIter(final RocksIterator rocksIterator, final ByteBuffer from) {
        return new RocksRecordIter(rocksIterator, from);
    }

    @Override
    public boolean hasNext() {
        skipDeleted();
        return rocksIterator.isValid();
    }

    private void skipDeleted() {
        while (rocksIterator.isValid()) {
            ExtendedRecord record = ExtendedRecord.fromBytes(rocksIterator.value());
            if (record.isDeleted())
                rocksIterator.next();
            else
                break;

        }
    }

    @Override
    public Record next() {
        skipDeleted();
        final ExtendedRecord record = ExtendedRecord.fromBytes(rocksIterator.value());
        final Record current = Record.of(
                RocksByteBufferUtils.fromUnsignedByteArray(rocksIterator.key()),
                record.getValue());
        rocksIterator.next();
        skipDeleted();
        /* RocksDB already has the selected element, so we need return it in next()
         at first time and in future */
        return current;
    }

    @Override
    public void close() throws IOException {
        rocksIterator.close();
    }
}
