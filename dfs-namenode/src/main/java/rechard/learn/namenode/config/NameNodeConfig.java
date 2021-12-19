package rechard.learn.namenode.config;

import io.netty.util.internal.StringUtil;
import lombok.Data;
import rechard.learn.dfs.common.exeception.NodeNodeFileParseException;
import rechard.learn.namenode.peer.PeerNode;

import java.io.File;
import java.util.Properties;

/**
 * @author Rechard
 **/
@Data
public class NameNodeConfig {
    private String nameNodeServers;
    private int port;
    private int nameNodeId;
    private String dfsHome;
    private String pass;

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
        this.pass = (String) properties.get("namenode.conn.requirepass");
    }

    public boolean authRequired() {
        return !StringUtil.isNullOrEmpty(this.pass);
    }


    public File getFsimgDir() {
        return new File(this.dfsHome + File.separatorChar + "fsimg");
    }

    public File getEditLogDir() {
        return new File(this.dfsHome + File.separatorChar + "log");
    }
}
