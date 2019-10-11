package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.rocksdb.*;
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
 *
 * Uses RocksDB as storage.
 *
 * @author Pavel Pokatilo
 */
public class DAOImplementation implements DAO {
    private final File data;
    private RocksDB db;

    public DAOImplementation(@NotNull File data) {
        this.data = data;

        try {
            initialize();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    private void initialize() throws RocksDBException {
        System.out.println(data.getAbsolutePath());
        RocksDB.loadLibrary();
        Options options = new Options().setCreateIfMissing(true);

        ComparatorOptions compOptions = new ComparatorOptions();
        /* New comparator is necessary for correct evaluating of next element */
        Comparator comp = new Comparator(compOptions) {
            @Override
            public String name() {
                return "CorrectSequenceComparator";
            }

            @Override
            public int compare(Slice a, Slice b) {
                return (ByteBuffer.wrap(a.data())).compareTo(ByteBuffer.wrap(b.data()));
            }
        };

        options.setComparator(comp);
        db = RocksDB.open(options, data.getAbsolutePath());
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        return RocksRecordIter.getIter(db.newIterator(), from);
    }

    @NotNull
    @Override
    public Iterator<Record> range(@NotNull ByteBuffer from, @Nullable ByteBuffer to) throws IOException {
        if (to == null) {
            return iterator(from);
        }

        if (from.compareTo(to) > 0) {
            return Iters.empty();
        }

        final Record bound = Record.of(to, ByteBuffer.allocate(0));
        return Iters.until(iterator(from), bound);
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull ByteBuffer key) throws IOException, NoSuchElementException {
        byte[] res = null;
        try {
            res = db.get(key.array());
        } catch (RocksDBException e) {
            throw new FastIOException();
        }
        if (res == null)
            throw new FastNoSuchElementException();
        return ByteBuffer.wrap(res);
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        try {
            final byte[] res = db.get(key.array());
            if (res != null)
                db.delete(key.array());
            db.put(key.array(), value.array());
        } catch (RocksDBException e) {
            throw new FastIOException();
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        try {
            db.delete(key.array());
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
}
