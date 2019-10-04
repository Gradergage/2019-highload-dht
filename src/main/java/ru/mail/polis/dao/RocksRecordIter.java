package ru.mail.polis.dao;

import org.rocksdb.RocksIterator;
import ru.mail.polis.Record;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * {@link DAOImplementation} iterator,
 *
 * Used as wrapping upon {@link RocksIterator}, which operating byte[] arrays,
 * to work with {@link Record}
 *
 * @author Pavel Pokatilo
 */
public class RocksRecordIter implements Iterator<Record> {
    private RocksIterator rocksIterator;

    private RocksRecordIter(RocksIterator rocksIterator, ByteBuffer from) {
        this.rocksIterator = rocksIterator;
        if (from != null)
            this.rocksIterator.seek(from.array());
        else {
            this.rocksIterator.seekToFirst();
        }
    }

    public static Iterator<Record> getIter(RocksIterator rocksIterator, ByteBuffer from) {
        return new RocksRecordIter(rocksIterator, from);
    }

    @Override
    public boolean hasNext() {
        return rocksIterator.isValid();
    }

    @Override
    public Record next() {
        Record current = Record.of(
                ByteBuffer.wrap(rocksIterator.key()),
                ByteBuffer.wrap(rocksIterator.value()));
        if (rocksIterator.isValid())
            rocksIterator.next();
        /* RocksDB already has the selected element, so we need return it in next()
         at first time and in future */
        return current;
    }
}
