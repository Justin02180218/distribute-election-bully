package com.justin.distribute.election.bully;

import com.justin.distribute.election.bully.messages.ClusterSyncMessage;
import com.justin.distribute.election.bully.messages.ElectionMessage;
import com.justin.distribute.election.bully.messages.MessageType;
import com.justin.distribute.election.bully.messages.VictoryMessage;
import com.justin.distribute.election.bully.processor.ClusterSyncRequestProcessor;
import com.justin.distribute.election.bully.processor.ElectionRequestProcessor;
import com.justin.distribute.election.bully.processor.VictoryRequestProcessor;
import com.justin.distribute.election.bully.common.PropertiesUtil;
import com.justin.net.remoting.RemotingClient;
import com.justin.net.remoting.RemotingServer;
import com.justin.net.remoting.netty.NettyRemotingClient;
import com.justin.net.remoting.netty.NettyRemotingServer;
import com.justin.net.remoting.netty.conf.NettyClientConfig;
import com.justin.net.remoting.netty.conf.NettyServerConfig;
import com.justin.net.remoting.protocol.RemotingMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public final class Node {
    private static final Logger logger = LogManager.getLogger(Node.class.getSimpleName());

    private final UUID nodeId = UUID.randomUUID();
    private final AtomicReference<NodeStatus> status = new AtomicReference<NodeStatus>(NodeStatus.UNKNOWN);
    private final AtomicInteger epoch = new AtomicInteger(0);
    private final String[] nodesAddress = PropertiesUtil.getNodesAddress();
    private final ConcurrentMap<String, Integer> nodeDetectFailNums = new ConcurrentHashMap<String, Integer>();
    private final ExecutorService processorExecutor = Executors.newFixedThreadPool(PropertiesUtil.getProcessorThreads());
    private final Timer timer = new Timer("NodeMessage", true);
    private final Metadata metadata;

    private final Cluster cluster;
    private final String host;
    private final int port;

    private String localAddr = PropertiesUtil.getLocalAddress();
    private RemotingServer server;
    private RemotingClient client;

    public Node() {
        String[] localAddress = localAddr.split(":");
        String host = localAddress[0];
        int port = Integer.parseInt(localAddress[1]);

        this.host = host;
        this.port = port;
        this.cluster = new Cluster();

        this.metadata = new Metadata(nodeId, localAddr, status.get(), epoch.get());
    }

    public Node(final String host, final int port) {
        this.host = host;
        this.port = port;
        this.localAddr = host + ":" + port;
        this.cluster = new Cluster();
        this.metadata = new Metadata(nodeId, localAddr, status.get(), epoch.get());
    }

    public synchronized void start() {
        logger.info("Prepare start node[id: {}, address: {}:{}]", nodeId, host, port);
        if (status.get() != NodeStatus.ALIVE) {
            try {
                cluster.addNode(nodeId, metadata);

                server = new NettyRemotingServer(new NettyServerConfig(host, port));
                server.registerProcessor(MessageType.CLUSTER_SYNC, new ClusterSyncRequestProcessor(this), processorExecutor);
                server.registerProcessor(MessageType.ELECTION, new ElectionRequestProcessor(), processorExecutor);
                server.registerProcessor(MessageType.VICTORY, new VictoryRequestProcessor(this), processorExecutor);
                server.start();

                client = new NettyRemotingClient(new NettyClientConfig());
                client.start();

                setStatus(NodeStatus.ALIVE);
                metadata.setNodeStatus(NodeStatus.ALIVE);

                timer.scheduleAtFixedRate(new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            Node.this.clusterSync(metadata);
                        }catch (Throwable e) {
                            logger.error("Cluster sync request failure: " + e.getMessage());
                        }
                    }
                }, 2*1000, 10*1000);

                timer.scheduleAtFixedRate(new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            Node.this.electLeader();
                        }catch (Throwable e) {
                            logger.error("Elect leader failure: " + e.getMessage());
                        }
                    }
                }, 5*1000, 5*1000);
            }catch (Exception e) {
                e.printStackTrace();
            }
        }else {
            logger.info("Node is alive " + this.toString());
        }
    }

    public synchronized void shutdown() {
        try {
            if (server != null) {
                server.shutdown();
            }
            if (client != null) {
                client.shutdown();
            }
            if (timer != null) {
                timer.cancel();
            }

            setStatus(NodeStatus.DEAD);
        }catch (Exception e) {
            logger.error(e);
        }
    }

    private void clusterSync(Metadata metadata) throws Exception {
        for(String nodeAddress : nodesAddress) {
            if (!nodeAddress.equals(localAddr)) {
                RemotingMessage response = client.invokeSync(nodeAddress, ClusterSyncMessage.getInstance().request(metadata), 3 * 1000);
                String peerNodeId = cluster.getNodeIdByaddr(nodeAddress);
                if (peerNodeId != null) {
                    if (response == null) {
                        if (nodeDetectFailNums.containsKey(peerNodeId)) {
                            int failNums = nodeDetectFailNums.get(peerNodeId);
                            if (failNums >= 3) {
                                cluster.getNodes().get(peerNodeId).setNodeStatus(NodeStatus.DEAD);
                            } else {
                                nodeDetectFailNums.put(peerNodeId, ++failNums);
                            }
                        } else {
                            nodeDetectFailNums.put(peerNodeId, 1);
                        }
                    } else {
                        nodeDetectFailNums.put(peerNodeId, 0);
                    }
                }
            }
        }
    }

    private void electLeader() throws Exception {
        logger.info("[" + metadata + "] cluster leader: " + cluster.getLeader());
        if (cluster.getNodes().size() > 1) {
            Metadata leader = cluster.getLeader();
            if ((leader == null) || (leader.getNodeStatus() != NodeStatus.ALIVE)) {
                logger.info("Starting election ...");
                epoch.getAndIncrement();
                metadata.setEpoch(epoch.get());

                List<Metadata> largerNodes = cluster.largerNodes(metadata);
                if (largerNodes.isEmpty()) {
                    if (metadata.getNodeStatus() != NodeStatus.ALIVE) {
                        logger.warn("Node is not alive: " + metadata);
                    }else {
                        cluster.getNodes().get(metadata.getNodeId().toString()).setEpoch(epoch.get());
                        cluster.setLeader(metadata);

                        List<Metadata> otherNodes = cluster.otherNodes(metadata);
                        for (Metadata otherNode : otherNodes) {
                            client.invokeOneway(otherNode.getNodeAddress(), VictoryMessage.getInstance().request(metadata), 3*1000);
                        }
                    }
                }else {
                    for (Metadata largerNode : largerNodes) {
                        RemotingMessage response = client.invokeSync(largerNode.getNodeAddress(), ElectionMessage.getInstance().request(metadata), 3*1000);
                        String res = new String(response.getMessageBody(), Charset.forName("UTF-8"));
                        logger.info("Election response: " + res);
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Node [id=").append(nodeId).append(", host=").append(host).append(", port=")
                .append(port).append(", status=").append(status).append(", epoch=").append(epoch)
                .append("]");
        return builder.toString();
    }

    public UUID getNodeId() {
        return nodeId;
    }

    public RemotingServer getServer() {
        return server;
    }

    public RemotingClient getClient() {
        return client;
    }

    public NodeStatus getStatus() {
        return status.get();
    }

    public void setStatus(final NodeStatus nodeStatus) {
        status.compareAndSet(status.get(), nodeStatus);
    }

    public Integer getEpoch() {
        return epoch.get();
    }

    public void setEpoch(final Integer epochInt) {
        epoch.compareAndSet(epoch.get(), epochInt);
    }

    public Cluster getCluster() {
        return cluster;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public class Metadata implements Comparable<Metadata> {
        private final UUID nodeId;
        private final String nodeAddress;
        private NodeStatus nodeStatus;
        private int epoch;

        public Metadata(final UUID nodeId, final String nodeAddress, final NodeStatus nodeStatus, final int epoch) {
            this.nodeId = nodeId;
            this.nodeAddress = nodeAddress;
            this.nodeStatus = nodeStatus;
            this.epoch = epoch;
        }

        @Override
        public int compareTo(Metadata o) {
            return nodeId.compareTo(o.getNodeId());
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("Node metadata [id=").append(nodeId).append(", host=").append(nodeAddress)
                    .append(", status=").append(nodeStatus).append(", epoch=").append(epoch)
                    .append("]");
            return builder.toString();
        }

        public UUID getNodeId() {
            return nodeId;
        }

        public String getNodeAddress() {
            return nodeAddress;
        }

        public NodeStatus getNodeStatus() {
            return nodeStatus;
        }

        public void setNodeStatus(NodeStatus nodeStatus) {
            this.nodeStatus = nodeStatus;
        }

        public int getEpoch() {
            return epoch;
        }

        public void setEpoch(int epoch) {
            this.epoch = epoch;
        }
    }
}
