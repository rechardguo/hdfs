package rechard.learn.namenode.constant;

/**
 * 消息类消息
 *
 * @author Rechard
 **/
public enum MsgType {
    NAME_NODE_PEER_AWARE(1, "namenode report self to other namenode");

    private int code;
    private String desc;

    MsgType(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    /**
     * 通过code那到msgType
     *
     * @param code
     * @return
     */
    public static MsgType get(int code) {
        for (MsgType type : values()) {
            if (type.code == code)
                return type;
        }
        return null;
    }

    public int code() {
        return this.code;
    }
}
