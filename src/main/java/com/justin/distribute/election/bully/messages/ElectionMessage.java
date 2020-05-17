package com.justin.distribute.election.bully.messages;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class ElectionMessage extends AbtractMessage {

    private ElectionMessage(){}

    public static ElectionMessage getInstance() {
        return new ElectionMessage();
    }

    @Override
    public int getMessageType() {
        return MessageType.ELECTION;
    }
}
