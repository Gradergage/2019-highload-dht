package ru.mail.polis.service;

import com.google.common.base.Charsets;
import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

public class CustomServer extends HttpServer implements Service {
    private final DAO dao;

    public CustomServer(final int port, @NotNull DAO dao) throws IOException {
        super(getConfig(port));
        this.dao = dao;
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }

    @Path("/v0/entity")
    public Response entity(@Param("id") final String id, final Request request, final HttpSession session) {
        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    try {
                        final ByteBuffer value = dao.get(key);

                        byte[] body = new byte[value.remaining()];
                        value.get(body);
                        return new Response(Response.OK, body);
                    } catch (NoSuchElementException e) {
                        return new Response(Response.NOT_FOUND, "Key not found".getBytes(Charsets.UTF_8));
                    }

                case Request.METHOD_PUT:
                    dao.upsert(key, ByteBuffer.wrap(request.getBody()));
                    return new Response(Response.CREATED, Response.EMPTY);

                case Request.METHOD_DELETE:
                    dao.remove(key);
                    return new Response(Response.ACCEPTED, Response.EMPTY);

                default:
                    return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
            }
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }
    
    private static HttpServerConfig getConfig(final int port) {
        if (port <= 1024 || port >= 65535) {
            throw new IllegalArgumentException("Invalid port");
        }
        AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        HttpServerConfig httpServerConfig = new HttpServerConfig();
        httpServerConfig.acceptors = new AcceptorConfig[]{acceptor};
        return httpServerConfig;
    }
}
