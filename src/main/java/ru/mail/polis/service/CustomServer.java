package ru.mail.polis.service;

import com.google.common.base.Charsets;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

public class CustomServer extends HttpServer implements Service {
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
     * Main point of Service, provides access to the DB
     * @param id id from /v0/entity&id request
     * @param request Http request
     * @param session Http session
     * @return response for
     */
    @Path("/v0/entity")
    public Response entity(@Param("id") final String id, final Request request, final HttpSession session) {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        try {
            if (id.isEmpty()) {
                return new Response(Response.BAD_REQUEST, "Query requires id".getBytes(Charsets.UTF_8));
            }
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
