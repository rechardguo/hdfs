package rechard.learn.dfs.common.fs;

import lombok.Data;

import java.util.Map;

/**
 * @author Rechard
 **/
@Data
public class OperationLogItem {
    private long txId;
    private int opType;
    private Map attr;
    private String path;
}
