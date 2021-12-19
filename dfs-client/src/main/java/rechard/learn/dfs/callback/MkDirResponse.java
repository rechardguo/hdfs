package rechard.learn.dfs.callback;

import java.util.Map;

/**
 * @author Rechard
 **/
public class MkDirResponse extends Response<Map> {
    public MkDirResponse(int msgType, Map data) {
        super(msgType, data);
    }
}
