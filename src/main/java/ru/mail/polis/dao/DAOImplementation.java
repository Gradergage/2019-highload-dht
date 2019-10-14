package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import org.rocksdb.Comparator;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Slice;

import ru.mail.polis.Record;
import ru.mail.polis.utils.FastIOException;
import ru.mail.polis.utils.FastNoSuchElementException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Custom {@link DAO} storage implementation class.
 * Uses RocksDB as storage.
 * @author Pavel Pokatilo
 */
public class DAOImplementation implements DAO {
    private final File data;
    private RocksDB db;

    /**
     * @param data File for creating LSM storage
     */
    public DAOImplementation(@NotNull final File data) {
        this.data = data;

        try {
            initialize();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    private void initialize() throws RocksDBException {
        RocksDB.loadLibrary();
        final Options options = new Options().setCreateIfMissing(true);

        final ComparatorOptions compOptions = new ComparatorOptions();
        /* New comparator is necessary for correct evaluating of next element */
        final Comparator comp = new Comparator(compOptions) {
            @Override
            public String name() {
                return "CorrectSequenceComparator";
            }

            @Override
            public int compare(final Slice a, final Slice b) {
                return ByteBuffer.wrap(a.data()).compareTo(ByteBuffer.wrap(b.data()));
            }
        };

        options.setComparator(comp);
        db = RocksDB.open(options, data.getAbsolutePath());
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        return RocksRecordIter.getIter(db.newIterator(), from);
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) throws IOException, NoSuchElementException {
        byte[] res = null;
        try {
            res = db.get(copyByteBuffer(key));
        } catch (RocksDBException e) {
            throw new FastIOException();
        }
        if (res == null) {
            throw new FastNoSuchElementException();
        }
        return ByteBuffer.wrap(res);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        try {
            final byte[] res = db.get(copyByteBuffer(key));
            if (res != null) {
                db.delete(copyByteBuffer(key));
            }
            db.put(copyByteBuffer(key), copyByteBuffer(value));
        } catch (RocksDBException e) {
            throw new FastIOException();
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        try {
            db.delete(copyByteBuffer(key));
        } catch (RocksDBException e) {
            throw new FastIOException();
        }
    }

    @Override
    public void compact() throws IOException {
        try {
            db.compactRange();
        } catch (RocksDBException e) {
            throw new FastIOException();
        }
    }

    @Override
    public void close() throws IOException {
        db.close();
    }

    /**
     * Return array from ByteBuffer that might be read-only
     * @param src source ByteBuffer
     * @return byte[] array
     */
    @NotNull
    private byte[] copyByteBuffer(@NotNull final ByteBuffer src) {
        final byte[] array = new byte[src.remaining()];
        src.duplicate().get(array);
        return array;
    }
}
