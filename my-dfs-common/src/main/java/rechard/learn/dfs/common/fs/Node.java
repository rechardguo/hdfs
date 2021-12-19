package rechard.learn.dfs.common.fs;

import com.ruyuan.dfs.model.backup.INode;
import lombok.Data;
import rechard.learn.dfs.common.constant.NodeType;

import java.util.*;

/**
 * 目录节点
 * 节点形成树
 *
 * @author Rechard
 **/
@Data
public class Node {

    private String path;
    private int type; //文件类型，目录还是文件
    private final TreeMap<String, Node> children;
    private Map<String, String> attr;
    private Node parent;

    public Node() {
        this.children = new TreeMap<>();
        this.attr = new HashMap<>();
    }

    public Node(String path, int type) {
        this();
        this.path = path;
        this.type = type;
    }

    public Node(String path, int type, Node parent) {
        this();
        this.path = path;
        this.type = type;
        this.parent = parent;
    }

    public boolean isDirectory() {
        return this.type == NodeType.DIRECTORY.getValue();
    }

    public boolean isFile() {
        return this.type == NodeType.FILE.getValue();
    }

    //将node转成protobuf里的node
    public static INode toINode(Node node) {
        INode.Builder builder = INode.newBuilder();
        String path = node.getPath();
        int type = node.getType();
        builder.setPath(path);
        builder.setType(type);
        builder.putAllAttr(node.getAttr());
        Collection<Node> children = node.getChildren().values();
        if (children.isEmpty()) {
            return builder.build();
        }
        List<INode> tmpNode = new ArrayList<>(children.size());
        for (Node child : children) {
            //递归
            INode iNode = toINode(child);
            tmpNode.add(iNode);
        }
        builder.addAllChildren(tmpNode);
        return builder.build();
    }


}
