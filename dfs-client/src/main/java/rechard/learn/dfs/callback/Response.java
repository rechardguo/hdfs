package rechard.learn.dfs.callback;

import rechard.learn.dfs.common.constant.MsgType;

/**
 * @author Rechard
 **/
public class Response<T> {
    private int msgType;
    private T data;

    public Response(int msgType, T data) {
        this.msgType = msgType;
        this.data = data;
    }

    public MsgType msgType() {
        return MsgType.get(this.msgType);
    }

    public T data() {
        return this.data;
    }
}
