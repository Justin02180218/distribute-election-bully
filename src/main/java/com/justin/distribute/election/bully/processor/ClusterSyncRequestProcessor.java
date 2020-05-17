package com.justin.distribute.election.bully.processor;

import com.justin.distribute.election.bully.Node;
import com.justin.distribute.election.bully.Node.Metadata;
import com.justin.distribute.election.bully.messages.ClusterSyncMessage;
import com.justin.net.remoting.netty.NettyRequestProcessor;
import com.justin.net.remoting.protocol.RemotingMessage;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.Charset;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class ClusterSyncRequestProcessor implements NettyRequestProcessor {
    private static final Logger logger = LogManager.getLogger(ClusterSyncRequestProcessor.class.getSimpleName());
    private final Node node;

    public ClusterSyncRequestProcessor(final Node node) {
        this.node = node;
    }

    @Override
    public RemotingMessage processRequest(ChannelHandlerContext ctx, RemotingMessage request) throws Exception {
        Metadata peerNodeMetadata = ClusterSyncMessage.getInstance().getPeerNodeMetadata(request);
        logger.info("[" + node.getMetadata() + " ===> " + peerNodeMetadata + "] cluster sync!");
        node.getCluster().addNode(peerNodeMetadata.getNodeId(), peerNodeMetadata);

        byte[] res = "ALIVE".getBytes(Charset.forName("UTF-8"));
        request.setMessageBody(res);
        return request;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
