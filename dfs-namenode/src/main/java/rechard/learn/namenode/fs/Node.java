package rechard.learn.namenode.fs;

import lombok.Data;
import rechard.learn.namenode.enums.NodeType;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

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

}
