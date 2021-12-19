package rechard.learn.dfs.common.constant;

/**
 * 消息类消息
 *
 * @author Rechard
 **/
public enum MsgType {
    NAME_NODE_PEER_AWARE(1, "namenode report self to other namenode"),
    BACK_NODE_AUTH_REQUEST(2, "backupnode auth request"),
    BACK_NODE_AUTH_RESPONSE(3, "backupnode auth response"),
    BACK_NODE_FETCH_FSIMAGE_REQUEST(4, "backupnode fetch fsimage from namenode request"),
    BACK_NODE_FETCH_FSIMAGE_RESPONSE(5, "backupnode fetch fsimage from namenode response"),
    BACK_NODE_FETCH_EDITLOG_REQUEST(6, "backupnode fetch editlog from namenode request"),
    BACK_NODE_FETCH_EDITLOG_RESPONSE(7, "backupnode fetch editlog from namenode response"),
    TRANSFER_FILE(8, "transfer FILE"),
    FS_OP_MKDIR_REQUEST(9, "file system send mkdir request to namenode server"),
    FS_OP_MKDIR_RESPONSE(10, "namenode server returen mkdir response");

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
