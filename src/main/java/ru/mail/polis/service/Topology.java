package ru.mail.polis.service;

import com.google.common.base.Charsets;
import com.google.common.collect.*;
import com.google.common.hash.Hashing;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class Topology {
    private final String currentNode;
    private final List<String> nodes;
    private final TreeRangeMap<Integer, String> nodesTable;

    private static final int PART_SIZE = 1 << 22;
    private static final int PARTS_NUMBER = 1 << (Integer.SIZE - 22);

    public Topology(String currentNode, final Set<String> topology) {
        if (!topology.contains(currentNode)) {
            throw new IllegalArgumentException("Current node isn't member of topology");
        }
        this.currentNode = currentNode;
        this.nodes = topology.stream().sorted().collect(Collectors.toList());
        this.nodesTable = createTable();
    }

    private TreeRangeMap<Integer, String> createTable() {
        final TreeRangeMap<Integer, String> table = TreeRangeMap.create();
        final Iterator<String> nodeIterator = Iterators.cycle(nodes);
        for (int i = 0; i < PARTS_NUMBER; i++) {
            final String node = nodeIterator.next();
            final int lowerBound = Integer.MIN_VALUE + i * PART_SIZE;
            final int upperBound = Integer.MIN_VALUE + (i + 1) * PART_SIZE - 1;
            final Range<Integer> key = Range.closed(lowerBound, upperBound);
            table.put(key, node);
        }
        return table;
    }

    public boolean isCurrentNode(@NotNull final String node) {
        return currentNode.equals(node);
    }

    private int hash(final ByteBuffer key) {
        final var keyCopy = key.duplicate();
        return Hashing.sha256()
                .newHasher(keyCopy.remaining())
                .putBytes(keyCopy)
                .hash()
                .asInt();
    }

    public String getNode(@NotNull final ByteBuffer key) {
        final int hash = hash(key);
        final String node = nodesTable.get(hash);
        if (node == null) {
            throw new IllegalStateException("Node doesn't exist for hash: " + hash);
        }
        return node;
    }
    public List<String> selectNodePool(@NotNull final String keyString, final int numOfNodes) {
        final ByteBuffer key = ByteBuffer.wrap(keyString.getBytes(Charsets.UTF_8));
        final int keyHash = Hashing.sha256().hashBytes(key.duplicate()).asInt();
        final String node = this.nodesTable.get(keyHash);
        final List<String> resultNodes = new ArrayList<>(numOfNodes);
        final PeekingIterator<String> iterator = Iterators.peekingIterator(Iterators.cycle(nodes));
        while (!iterator.peek().equals(node)) {
            iterator.next();
        }
        for (int i = 0; i < numOfNodes; i++) {
            resultNodes.add(iterator.next());
        }
        return resultNodes;
    }
    public Set<String> getNodes() {
        return new HashSet<>(nodes);
    }
}
