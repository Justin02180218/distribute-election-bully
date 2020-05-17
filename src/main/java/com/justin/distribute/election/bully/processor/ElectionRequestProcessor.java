package com.justin.distribute.election.bully.processor;

import com.justin.distribute.election.bully.messages.ElectionMessage;
import com.justin.net.remoting.netty.NettyRequestProcessor;
import com.justin.net.remoting.protocol.RemotingMessage;
import com.justin.distribute.election.bully.Node.Metadata;

import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.Charset;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class ElectionRequestProcessor implements NettyRequestProcessor {
    private static final Logger logger = LogManager.getLogger(ElectionRequestProcessor.class.getSimpleName());

    @Override
    public RemotingMessage processRequest(ChannelHandlerContext ctx, RemotingMessage request) throws Exception {
        Metadata peerNodeMetadata = ElectionMessage.getInstance().getPeerNodeMetadata(request);
        logger.info("[ ===> " + peerNodeMetadata + "] election!");

        byte[] res = "ALIVE".getBytes(Charset.forName("UTF-8"));
        request.setMessageBody(res);
        return request;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
