package com.justin.distribute.election.bully.messages;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class ClusterSyncMessage extends AbtractMessage {
    private ClusterSyncMessage(){}

    public static ClusterSyncMessage getInstance() {
        return new ClusterSyncMessage();
    }

    @Override
    public int getMessageType() {
        return MessageType.CLUSTER_SYNC;
    }
}
