package rechard.learn.namenode.config;

import lombok.Data;
import rechard.learn.namenode.exeception.NodeNodeFileParseException;
import rechard.learn.namenode.peer.PeerNode;

import java.util.Properties;

/**
 * @author Rechard
 **/
@Data
public class NameNodeConfig {
    private String nameNodeServers;
    private int port;
    private int nameNodeId;

    public PeerNode[] getPeerNodes() {
        String[] nameNodes = nameNodeServers.split(",");
        PeerNode[] peerNodes = new PeerNode[nameNodes.length];
        int i = 0;
        for (String nameNode : nameNodes) {
            String[] nameNodeInfo = nameNode.split(":");
            if (nameNodeInfo.length != 3)
                throw new NodeNodeFileParseException(nameNodeInfo + " is invalid config");
            peerNodes[i++] = new PeerNode(nameNodeInfo[0],
                    Integer.parseInt(nameNodeInfo[1]),
                    Integer.parseInt(nameNodeInfo[2]));
        }
        return peerNodes;
    }

    public void loadFromResource(Properties properties) {
        this.port = Integer.parseInt((String) properties.get("server.port"));
        this.nameNodeId = Integer.parseInt((String) properties.get("server.nameNodeId"));
        this.nameNodeServers = (String) properties.get("namenode.peer.servers");
    }
}
