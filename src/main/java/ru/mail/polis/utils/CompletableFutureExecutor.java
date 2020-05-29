package ru.mail.polis.utils;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class CompletableFutureExecutor {
    private CompletableFutureExecutor() {
    }

    /**
     * Make result when necessary number of futures complete.
     *
     * @param futures - list of futures
     * @param ack     - needed number of completed futures
     * @param <T>     - type of result
     * @return list of results
     */
    public static <T> CompletableFuture<List<T>> onComplete(@NotNull final List<CompletableFuture<T>> futures,
                                                            final int ack) {
        final CompletableFuture<List<T>> result = new CompletableFuture<>();
        final List<T> values = new CopyOnWriteArrayList<>();
        final AtomicInteger maxNumOfExceptions = new AtomicInteger(futures.size() - ack);
        for (final CompletableFuture<T> future : futures) {
            future
                    .thenAccept(value -> {
                        values.add(value);
                        if (values.size() >= ack) {
                            result.complete(values);
                        }
                    })
                    .exceptionally(exception -> {
                        if (maxNumOfExceptions.decrementAndGet() < 0) {
                            result.completeExceptionally(new IOException("Futures completed with exceptions"));
                        }
                        return null;
                    });
        }
        return result;
    }
}
