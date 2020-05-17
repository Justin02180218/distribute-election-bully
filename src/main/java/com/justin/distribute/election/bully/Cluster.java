package com.justin.distribute.election.bully;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.justin.distribute.election.bully.Node.Metadata;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class Cluster {
    private final static Logger logger = LogManager.getLogger(Cluster.class.getSimpleName());

    private final AtomicInteger epoch = new AtomicInteger(0);
    private final ConcurrentMap<String, Metadata> nodes = new ConcurrentHashMap<String, Metadata>();

    private AtomicReference<Metadata> leader = new AtomicReference<Metadata>();

    public void addNode(final UUID nodeId, final Node.Metadata nodeMetadata) {
        nodes.put(nodeId.toString(), nodeMetadata);
    }

    public List<Metadata> largerNodes(final Metadata metadata) {
        final List<Metadata> largerNodes = new ArrayList<Metadata>();
        for (Metadata node : nodes.values()) {
            if (metadata.compareTo(node) < 0) {
                largerNodes.add(node);
            }
        }
        return largerNodes;
    }

    public List<Metadata> otherNodes(final Metadata metadata) {
        final List<Metadata> otherNodes = new ArrayList<Metadata>();
        for (Metadata node : nodes.values()) {
            if (!node.getNodeId().toString().equals(metadata.getNodeId().toString())) {
                otherNodes.add(node);
            }
        }
        return otherNodes;
    }

    public String getNodeIdByaddr(final String addr) {
        for (Map.Entry<String, Metadata> entry : nodes.entrySet()) {
            String nodeId = entry.getKey();
            Metadata metadata = entry.getValue();
            if (metadata.getNodeAddress().equals(addr)) {
                return nodeId;
            }
        }
        return null;
    }

    public ConcurrentMap<String, Metadata> getNodes() {
        return nodes;
    }

    public int getEpoch() {
        return epoch.get();
    }

    public void setEpoch(final int epochInt) {
        epoch.compareAndSet(getEpoch(), epochInt);
    }

    public Metadata getLeader() {
        return leader.get();
    }

    public void setLeader(Metadata metadata) {
        leader.compareAndSet(getLeader(), metadata);
    }
}
