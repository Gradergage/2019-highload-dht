package ru.mail.polis.service;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import one.nio.http.*;
import one.nio.net.ConnectionString;
import one.nio.net.Socket;
import one.nio.pool.PoolException;
import one.nio.server.AcceptorConfig;
import one.nio.server.RejectedSessionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.ExtendedRecord;
import ru.mail.polis.dao.StreamHttpSession;
import ru.mail.polis.utils.CompletableFutureExecutor;
import ru.mail.polis.utils.RocksByteBufferUtils;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class CustomServer extends HttpServer implements Service {

    private static final Log log = LogFactory.getLog(CustomServer.class);
    private final DAO dao;

    private static final String SERVICE_REQUEST_HEADER = "X-Service-Request:";
    private final Map<String, HttpClient> clusterPool;
    private static final int TIMEOUT = 100;
    private final Topology topology;
    private final java.net.http.HttpClient httpClient;

    public CustomServer(final int port, @NotNull final DAO dao, final Topology topology) throws IOException {
        super(getConfig(port));
        this.dao = dao;
        this.topology = topology;
        clusterPool = topology.getNodes().stream().filter(
                node -> !topology.isCurrentNode(node))
                .collect(Collectors.toMap(
                        node -> node,
                        node -> new HttpClient(new ConnectionString(node + "?timeout=" + TIMEOUT))));
        this.httpClient = java.net.http.HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build();
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
     */
    @Path("/v0/entity")
    public void entity(@Param("id") final String id,
                       @Param("replicas") final String replicas,
                       final Request request,
                       final HttpSession session) {

        if (id == null || id.isEmpty()) {
            sendResponse(new Response(Response.BAD_REQUEST, "Query requires id".getBytes(Charsets.UTF_8)), session);
            return;
        }
        System.out.println("on node [" + port + "] | is service:" + getServiceMarkerHeader(request));

        if (getServiceMarkerHeader(request)) {
            asyncExecute(() -> {
                try {
                    sendResponse(handleEntityRequest(id, request), session);
                } catch (IOException exception) {
                    sendResponse(new Response(Response.INTERNAL_ERROR, Response.EMPTY), session);
                }
            });
            return;
        }

        Integer[] ackFrom = generateAckFrom();
        if (replicas != null && !replicas.isEmpty()) {
            ackFrom = getAckFrom(replicas);
        }

        if (!checkAckFrom(ackFrom[0], ackFrom[1])) {
            sendResponse(new Response(Response.BAD_REQUEST, "Bad replicas parameter".getBytes(Charsets.UTF_8)), session);
            return;
        }
        final Replicas replicasObj = new Replicas(ackFrom[0], ackFrom[1]);
        final long timestamp = System.currentTimeMillis();

        asyncExecute(() -> {
            executeEntityRequest(id, replicasObj, request, session, timestamp);
        });

    }

    private void executeEntityRequest(final String id,
                                      final Replicas replicas,
                                      final Request request,
                                      final HttpSession session,
                                      final long timestamp) {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));

        final List<CompletableFuture<Response>> responses = new ArrayList<>();

        for (String n : topology.selectNodePool(id, replicas.getFrom())) {
            if (topology.isCurrentNode(n)) {
                responses.add(processLocally(id, request));
            } else {
                //  responses.add(processOnNode(n, request));
                responses.add(processNoNio(id, request, n, httpClient));
            }
        }

        CompletableFutureExecutor
                .onComplete(responses, replicas.getAck())
                .thenAccept(
                        res -> {
                            //TODO
                            try {
                                System.out.println("answering:" + port);
                                session.sendResponse(extractReplicasResponse(request, replicas.getAck(), res));
                            } catch (IOException e) {
                                sendError(session, e.getMessage());
                            }
                        }
                )
                .exceptionally(e -> {
                    try {

                        System.out.println("gateway timeout:" + port);
                        session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                    } catch (IOException ex) {

                        System.out.println("error:" + port);
                        sendError(session, ex.getMessage());
                    }
                    return null;
                });

    }

    private CompletableFuture<Response> processOnNode(@NotNull final String node, @NotNull final Request request) {
        final CompletableFuture<Response> response = new CompletableFuture<>();
        setServiceMarkerHeader(request);
        try {
            //?????
            final Response r = clusterPool.get(node).invoke(request, TIMEOUT);
            System.out.println("answering onNode:" + port);
            response.complete(r);
        } catch (IOException | InterruptedException | PoolException | HttpException e) {
            System.out.println("exceptionally onNode:" + port + " " + e.getMessage());
            response.completeExceptionally(e);
        }
        return response;
    }

    private CompletableFuture<Response> processLocally(@NotNull final String id, @NotNull final Request request) {
        final CompletableFuture<Response> response = new CompletableFuture<>();
        setServiceMarkerHeader(request);
        try {
            final Response r = handleEntityRequest(id, request);
            System.out.println("answeringLocally:" + port);
            response.complete(r);
        } catch (IOException e) {
            System.out.println("exceptionallyLocally:" + port + " " + e.getMessage());
            response.completeExceptionally(e);
        }
        return response;
    }

    public static CompletableFuture<Response> processNoNio(final String id,
                                                           @NotNull final Request request,
                                                           final String node,
                                                           final java.net.http.HttpClient client) {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(node + "/v0/entity?id=" + id))
                .timeout(Duration.ofMillis(100))
                .setHeader("X-Service-Request", "true");
        if (request.getMethod() == Request.METHOD_GET) {
            requestBuilder = requestBuilder.GET();
        } else if (request.getMethod() == Request.METHOD_PUT) {
            requestBuilder = requestBuilder.PUT(HttpRequest.BodyPublishers.ofByteArray(request.getBody()));
        } else if (request.getMethod() == Request.METHOD_DELETE) {
            requestBuilder = requestBuilder.DELETE();
        } else {
            throw new IllegalStateException();
        }
        final HttpRequest httpRequest = requestBuilder.build();
        return client.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(res -> new Response("" + res.statusCode(), res.body()));
    }

    private Response extractReplicasResponse(@NotNull final Request request,
                                             final int ack,
                                             final List<Response> responses) {
        final Set<Integer> codes = ImmutableSet.of(200, 201, 202, 404);
        final long successResponses = responses.stream()
                .map(Response::getStatus)
                .filter(codes::contains)
                .count();
        if (successResponses < ack) {
            return new Response("504 Not Enough Replicas", Response.EMPTY);
        }
        switch (request.getMethod()) {
            case Request.METHOD_GET: {
                ExtendedRecord newest = null;
                for (final Response response : responses) {
                    if (response.getStatus() != 200) {
                        continue;
                    }
                    final ExtendedRecord record = ExtendedRecord.fromBytes(response.getBody());
                    if (newest == null || record.getTimestamp() > newest.getTimestamp()) {
                        newest = record;
                    }
                }
                if (newest == null || newest.isDeleted()) {
                    return new Response(Response.NOT_FOUND, Response.EMPTY);
                } else {
                    final byte[] value = RocksByteBufferUtils.copyByteBuffer(newest.getValue());
                    return Response.ok(value);
                }
            }
            case Request.METHOD_PUT: {
                return new Response(Response.CREATED, Response.EMPTY);
            }
            case Request.METHOD_DELETE: {
                return new Response(Response.ACCEPTED, Response.EMPTY);
            }
            default:
                return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
        }
    }

    private void sendError(@NotNull final HttpSession session, @NotNull final String message) {
        try {
            session.sendError(Response.INTERNAL_ERROR, message);
        } catch (IOException e) {
            log.error("Error", e);
            System.out.println("error" + e.getMessage());
        }
    }

    private Integer[] generateAckFrom() {
        return new Integer[]{topology.getNodes().size() / 2 + 1, topology.getNodes().size()};
    }

    private boolean checkAckFrom(Integer ack, Integer from) {
        return ack <= from && ack != 0 && from != 0 && from <= topology.getNodes().size();
    }

    private Integer[] getAckFrom(final String replicas) {
        final List<String> array = Splitter.on("/").splitToList(replicas);
        if (array.size() < 2) {
            return new Integer[]{0, 0};
        } else {
            return new Integer[]{Integer.parseInt(array.get(0)), Integer.parseInt(array.get(1))};
        }
    }

    private void sendResponse(@NotNull final Response response, @NotNull final HttpSession session) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            log.error(e);
        }
    }

    private Response handleEntityRequest(@NotNull final String id, @NotNull final Request request) throws IOException {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        Response response;
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                response = handleGet(key);
                break;
            case Request.METHOD_PUT:
                response = handlePut(request, key);
                break;
            case Request.METHOD_DELETE:
                response = handleDelete(key);
                break;
            default:
                response = new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
                break;
        }
        return response;
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
            return;
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
            @NotNull final StreamHttpSession session) throws IOException {
        final Iterator<Record> iterator = dao.range(start, end);
        session.openStream(iterator);
    }

    @Override
    public HttpSession createSession(final Socket socket) throws RejectedSessionException {
        return new StreamHttpSession(socket, this);
    }

    private Response handleGet(final ByteBuffer key) throws IOException {
        try {

            ExtendedRecord record = dao.getRecord(key);
            return new Response(Response.OK, record.toBytes());
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

  /*  private void setTimestamp(final Request request, final long timestamp) {
        request.addHeader(TIMESTAMP_HEADER + timestamp);
    }

    private long getTimestamp(final Request request) {
        final String header = request.getHeader(TIMESTAMP_HEADER);
        if (header == null) {
            return System.currentTimeMillis();
        }
        return Long.parseLong(header);
        // request.addHeader(TIMESTAMP_HEADER + timestamp);
    }*/

    private boolean getServiceMarkerHeader(final Request request) {
        final String header = request.getHeader(SERVICE_REQUEST_HEADER);
        if (header == null) {
            return false;
        }
        // return Boolean.parseBoolean(header);
        return Boolean.parseBoolean(header.strip());
    }

    private void setServiceMarkerHeader(final Request request) {
        request.addHeader(SERVICE_REQUEST_HEADER + "true");
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
