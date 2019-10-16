package ru.mail.polis.service;

import com.google.common.base.Charsets;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import one.nio.server.RejectedSessionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.StreamHttpSession;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class CustomServer extends HttpServer implements Service {
    private static final Log log = LogFactory.getLog(CustomServer.class);
    private final DAO dao;

    public CustomServer(final int port, @NotNull final DAO dao) throws IOException {
        super(getConfig(port));
        this.dao = dao;
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        final Response response = new Response(Response.BAD_REQUEST, "Wrong query".getBytes(Charsets.UTF_8));
        session.sendResponse(response);
    }

    /**
     * Check status of this node.
     *
     * @return status
     */
    @Path("/v0/status")
    public Response status() {
        return Response.ok("OK");
    }

    /**
     * Provides an entry by the id (key). id is strongly required.
     *
     * @param id      id from /v0/entity&id request
     * @param request Http request
     * @param session Http session
     * @return response for
     */
    @Path("/v0/entity")
    public Response entity(@Param("id") final String id,
                           final Request request,
                           final HttpSession session) {
        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, "Query requires id".getBytes(Charsets.UTF_8));
        }
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        try {

            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    return handleGet(key);
                case Request.METHOD_PUT:
                    return handlePut(request, key);
                case Request.METHOD_DELETE:
                    return handleDelete(key);
                default:
                    return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
            }
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }

    }

    /**
     * Provides all entries from start key to end key, if end key.
     * Retrieves full range from start in case of nonexistent end key.
     *
     * @param start start key of range
     * @param end   end key of range
     */
    @Path("/v0/entities")
    public void entities(
            @Param("start") final String start,
            @Param("end") final String end,
            @NotNull final HttpSession session) throws IOException {
        if (start == null || start.isEmpty()) {
            session.sendResponse(
                    new Response(Response.BAD_REQUEST,
                            "Start parameter is required".getBytes(Charsets.UTF_8)));
        }
        final ByteBuffer startKey = ByteBuffer.wrap(start.getBytes(Charsets.UTF_8));
        ByteBuffer tempEndKey = null;

        if (end != null && !end.isEmpty()) {
            tempEndKey = ByteBuffer.wrap(end.getBytes(Charsets.UTF_8));
        }
        final ByteBuffer endKey = tempEndKey;
        final StreamHttpSession streamSession = (StreamHttpSession) session;
        asyncExecute(() -> {
            try {
                handleEntities(startKey, endKey, streamSession);
            } catch (IOException exception) {
                try {
                    session.sendResponse(new Response(Response.INTERNAL_ERROR));
                } catch (IOException e) {
                    log.error("Exception while initializing RocksDB", e);
                }
            }
        });
    }

    private void handleEntities(
            @NotNull final ByteBuffer start,
            @Nullable final ByteBuffer end,
            final StreamHttpSession session) throws IOException {
        final Iterator<Record> iterator = dao.range(start, end);
        session.openStream(iterator);
    }

    @Override
    public HttpSession createSession(final Socket socket) throws RejectedSessionException {
        return new StreamHttpSession(socket, this);
    }

    private Response handleGet(final ByteBuffer key) throws IOException {
        try {
            final ByteBuffer value = dao.get(key);
            final byte[] body = new byte[value.remaining()];
            value.get(body);
            return new Response(Response.OK, body);
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, "Key not found".getBytes(Charsets.UTF_8));
        }
    }

    private Response handlePut(final Request request, final ByteBuffer key) throws IOException {
        dao.upsert(key, ByteBuffer.wrap(request.getBody()));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response handleDelete(final ByteBuffer key) throws IOException {
        dao.remove(key);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private static HttpServerConfig getConfig(final int port) {
        if (port <= 1024 || port >= 65535) {
            throw new IllegalArgumentException("Invalid port");
        }
        final AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        final HttpServerConfig httpServerConfig = new HttpServerConfig();
        httpServerConfig.acceptors = new AcceptorConfig[]{acceptor};
        return httpServerConfig;
    }
}
