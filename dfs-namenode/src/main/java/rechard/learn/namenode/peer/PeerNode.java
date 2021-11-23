package rechard.learn.namenode.peer;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 结点
 *
 * @author Rechard
 **/
@Data
@AllArgsConstructor
public class PeerNode {
    private String host;
    private int port;
    private int id;
}
