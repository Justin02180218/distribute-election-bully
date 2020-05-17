package com.justin.distribute.election.bully.processor;

import com.justin.distribute.election.bully.Node;
import com.justin.distribute.election.bully.Node.Metadata;
import com.justin.distribute.election.bully.messages.VictoryMessage;
import com.justin.net.remoting.netty.NettyRequestProcessor;
import com.justin.net.remoting.protocol.RemotingMessage;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class VictoryRequestProcessor implements NettyRequestProcessor {
    private static final Logger logger = LogManager.getLogger(VictoryRequestProcessor.class.getSimpleName());

    private final Node node;

    public VictoryRequestProcessor(final Node node) {
        this.node = node;
    }

    @Override
    public RemotingMessage processRequest(ChannelHandlerContext ctx, RemotingMessage request) throws Exception {
        Metadata peerNodeMetadata = VictoryMessage.getInstance().getPeerNodeMetadata(request);
        logger.info("[" + node.getMetadata() + " ===> " + peerNodeMetadata + "] victory!");
        node.setEpoch(peerNodeMetadata.getEpoch());
        node.getCluster().setEpoch(peerNodeMetadata.getEpoch());
        node.getCluster().setLeader(peerNodeMetadata);

        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
