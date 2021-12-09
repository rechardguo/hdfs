package rechard.learn.namenode.fs;

import lombok.Data;

import java.util.Map;

/**
 * @author Rechard
 **/
@Data
public class EditLog {
    private long txId;
    private int opType;
    private Map attr;
    private String path;
}
