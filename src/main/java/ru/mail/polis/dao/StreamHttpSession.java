package ru.mail.polis.dao;

import com.google.common.base.Charsets;
import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.net.Socket;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import ru.mail.polis.Record;
import ru.mail.polis.utils.RocksByteBufferUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class StreamHttpSession extends HttpSession {
    private static final Log log = LogFactory.getLog(StreamHttpSession.class);

    private Iterator<Record> iterator;

    private static final byte[] CRLF = "\r\n".getBytes(Charsets.UTF_8);
    private static final byte[] DELIMITER = "\n".getBytes(Charsets.UTF_8);
    private static final byte[] EMPTY = "0\r\n\r\n".getBytes(Charsets.UTF_8);


    public StreamHttpSession(final Socket socket, final HttpServer server) {
        super(socket, server);
    }

    public void openStream(final Iterator<Record> iterator) throws IOException {
        this.iterator = iterator;
        if (handling == null) {
            throw new IOException("Out of order response");
        }
        final Response response = new Response(Response.OK);
        response.addHeader(keepAlive() ? "Connection: Keep-Alive" : "Connection: close");
        response.addHeader("Transfer-Encoding: chunked");
        writeResponse(response, false);
        writeNextRecord();
    }

    @Override
    protected void processWrite() throws Exception {
        super.processWrite();
        writeNextRecord();
    }

    private void writeNextRecord() throws IOException {
        if (iterator == null) {
            throw new IllegalStateException("Iterator isn't initialized");
        }
        while (iterator.hasNext() && queueHead == null) {
            final Record record = iterator.next();
            final byte[] chunk = buildChunkFromRecord(record);
            write(chunk, 0, chunk.length);
        }
        if (!iterator.hasNext()) {
            closeStream();
            closeIterator();
        }

    }

    private byte[] buildChunkFromRecord(final Record record) throws IOException {
        final byte[] key = RocksByteBufferUtils.copyByteBuffer(record.getKey());
        final byte[] value = RocksByteBufferUtils.copyByteBuffer(record.getValue());

        final int entityLength = key.length + value.length + DELIMITER.length;
        final String entityLengthHex = Integer.toHexString(entityLength);
        final int chunkLength = entityLengthHex.length() + entityLength + CRLF.length*2;

        final byte[] chunk = new byte[chunkLength];
        final ByteBuffer chunkWrapper = ByteBuffer.wrap(chunk);

        chunkWrapper.put(entityLengthHex.getBytes(Charsets.UTF_8))
                .put(CRLF)
                .put(key)
                .put(DELIMITER)
                .put(value)
                .put(CRLF);

        return chunk;
    }

    private void closeStream() throws IOException {
        write(EMPTY, 0, EMPTY.length);
        server.incRequestsProcessed();

        if (!keepAlive()) {
            scheduleClose();
        }

        if ((handling = pipeline.pollFirst()) != null) {
            if (handling == FIN) {
                scheduleClose();
            } else {
                server.handleRequest(handling, this);
            }
        }
    }

    private void closeIterator() {
        try {
            ((RocksRecordIter) iterator).close();
        } catch (IOException exception) {
            log.error("Exception while close iterator", exception);
        }
        iterator = null;
    }

    private boolean keepAlive() {
        final var connection = handling.getHeader("Connection: ");
        return handling.isHttp11()
                ? !"close".equalsIgnoreCase(connection)
                : "Keep-Alive".equalsIgnoreCase(connection);
    }
}
