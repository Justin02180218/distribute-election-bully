package com.justin.distribute.election.bully.messages;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class VictoryMessage extends AbtractMessage {

    private VictoryMessage() {}

    public static VictoryMessage getInstance() {
        return new VictoryMessage();
    }

    @Override
    public int getMessageType() {
        return MessageType.VICTORY;
    }
}
