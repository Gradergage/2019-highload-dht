package ru.mail.polis.dao;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.spi.LoggerFactory;
import org.jetbrains.annotations.NotNull;

import org.rocksdb.BuiltinComparator;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import ru.mail.polis.Record;
import ru.mail.polis.utils.FastIOException;
import ru.mail.polis.utils.FastNoSuchElementException;
import ru.mail.polis.utils.RocksByteBufferUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Custom DAO storage implementation class.
 * Uses RocksDB as storage.
 *
 * @author Pavel Pokatilo
 */
public class DAOImplementation implements DAO {
    private static final Log log = LogFactory.getLog(DAOImplementation.class);
    private final File data;
    private RocksDB db;

    /**
     * Constructor overwritten from interface.
     *
     * @param data File for creating LSM storage
     */
    public DAOImplementation(@NotNull final File data) {
        this.data = data;

        try {
            initialize();
        } catch (RocksDBException e) {
            log.error("Exception while initializing RocksDB", e);
        }
    }

    private void initialize() throws RocksDBException {
        RocksDB.loadLibrary();
        final Options options = new Options()
                .setCreateIfMissing(true)
                .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);
        db = RocksDB.open(options, data.getAbsolutePath());
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final ByteBuffer tempFrom = RocksByteBufferUtils.toUnsignedByteArray(from);
        return RocksRecordIter.getIter(db.newIterator(), tempFrom);
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) throws IOException, NoSuchElementException {
        final ByteBuffer tempKey = RocksByteBufferUtils.toUnsignedByteArray(key);
        byte[] res = null;
        try {
            res = db.get(RocksByteBufferUtils.copyByteBuffer(tempKey));
        } catch (RocksDBException e) {
            throw new FastIOException(e);
        }
        if (res == null) {
            throw new FastNoSuchElementException();
        }
        return ByteBuffer.wrap(res);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        final ByteBuffer tempKey = RocksByteBufferUtils.toUnsignedByteArray(key);
        try {
            final byte[] res = db.get(RocksByteBufferUtils.copyByteBuffer(tempKey));
            if (res != null) {
                db.delete(RocksByteBufferUtils.copyByteBuffer(tempKey));
            }
            db.put(RocksByteBufferUtils.copyByteBuffer(tempKey), RocksByteBufferUtils.copyByteBuffer(value));
        } catch (RocksDBException e) {
            throw new FastIOException(e);
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        final ByteBuffer tempKey = RocksByteBufferUtils.toUnsignedByteArray(key);
        try {
            db.delete(RocksByteBufferUtils.copyByteBuffer(tempKey));
        } catch (RocksDBException e) {
            throw new FastIOException(e);
        }
    }

    @Override
    public void compact() throws IOException {
        try {
            db.compactRange();
        } catch (RocksDBException e) {
            throw new FastIOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        db.close();
    }


}
