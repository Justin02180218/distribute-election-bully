package com.justin.distribute.election.bully.messages;

import com.justin.net.remoting.protocol.JSONSerializable;
import com.justin.net.remoting.protocol.RemotingMessage;
import com.justin.net.remoting.protocol.RemotingMessageHeader;

import com.justin.distribute.election.bully.Node.Metadata;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public abstract class AbtractMessage {

    public Metadata getPeerNodeMetadata(final RemotingMessage request) {
        byte[] body = request.getMessageBody();
        Metadata peerNodeMetadata = JSONSerializable.decode(body, Metadata.class);
        return peerNodeMetadata;
    }

    public RemotingMessage request(final Metadata nodeMetadata) {
        RemotingMessageHeader header = new RemotingMessageHeader();
        header.setCode(getMessageType());

        byte[] body = JSONSerializable.encode(nodeMetadata);

        RemotingMessage remotingMessage = new RemotingMessage(header, body);
        return remotingMessage;
    }

    public abstract int getMessageType();
}
