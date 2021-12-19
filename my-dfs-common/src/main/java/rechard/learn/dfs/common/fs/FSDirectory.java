package rechard.learn.dfs.common.fs;

import com.ruyuan.dfs.model.backup.EditLog;
import com.ruyuan.dfs.model.backup.INode;
import rechard.learn.dfs.common.constant.NodeType;
import rechard.learn.dfs.common.exeception.NameNodeException;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static rechard.learn.dfs.common.constant.NameNodeConstant.DELETE_FILE;
import static rechard.learn.dfs.common.constant.NameNodeConstant.MKDIR;

/**
 * 目录结构
 * 对目录树的操作
 *
 * @author Rechard
 **/
public class FSDirectory {

    //根目录
    private Node root;
    private ReadWriteLock lock = new ReentrantReadWriteLock();

    public FSDirectory() {
        this.root = new Node("/", NodeType.DIRECTORY.getValue());
    }

    public void setRoot(Node node) {
        this.root = node;
    }


    /**
     * 根据内存目录树生成FsImage
     *
     * @return FsImage
     */
    public FsImage getFsImage() {
        try {
            lock.readLock().lock();
            INode iNode = Node.toINode(root);
            return new FsImage(0L, iNode);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @param path
     * @param attr
     * @throws NameNodeException
     */
    public void mkdir(String path, Map<String, String> attr) throws NameNodeException {
        String dirPath = cleanPath(path);
        lock.writeLock().lock();
        String[] pathInfo = dirPath.split("/");
        Node current = root;
        Node parent = null;
        for (String p : pathInfo) {
            parent = current;
            current = findNode(current, p);
            if (current == null) {
                current = new Node(p, NodeType.DIRECTORY.getValue(), parent);
                current.setAttr(attr);
                parent.getChildren().put(p, current);
            }
            if (!current.isDirectory()) {
                throw new NameNodeException(p + " in " + path + " is not directory");
            }
        }
        lock.writeLock().unlock();
    }

    /**
     * 找到节点
     *
     * @param node
     * @param path
     * @return
     */
    public Node findNode(Node node, String path) {
        Node current = node;
        if (current.getPath().equals(path)) {
            return current;
        } else {
            TreeMap<String, Node> childMap = current.getChildren();
            if ((current = childMap.get(path)) != null) {
                return current;
            }
        }
        return null;
    }

    public Node findNode(String path) throws NameNodeException {
        String nodePath = cleanPath(path);
        Node current = root;
        String[] pathInfo = nodePath.split("/");
        for (int i = 0; i < pathInfo.length; i++) {
            if (current == null)
                throw new NameNodeException(path + " is not exist");
            Node tmp = this.findNode(current, pathInfo[i]);

            if (i == pathInfo.length - 1) {
                //到最后一个节点是空的，就返回异常
                if (tmp == null)
                    throw new NameNodeException(path + " is not exist");
                return tmp;
            }
            current = tmp;
        }
        return null;
    }


    /**
     * 创建文件
     *
     * @param fileName
     * @param attr
     * @return
     */
    public boolean createFile(String fullFileName, Map<String, String> attr) throws NameNodeException {
        String cleanFileName = cleanPath(fullFileName);
        lock.writeLock().lock();
        //找到目录
        int lastSlashIndex = cleanFileName.lastIndexOf("/");
        String parentPath = cleanFileName.substring(0, lastSlashIndex);
        String fileName = cleanFileName.substring(lastSlashIndex + 1);
        Node node = findNode(parentPath);
        if (node == null) {
            throw new NameNodeException(parentPath + " is not exist");
        }
        node.getChildren().put(fileName, new Node(fileName, NodeType.FILE.getValue(), node));
        return true;
    }

    /**
     * 删除文件
     *
     * @param fileName
     * @return
     * @throws NameNodeException
     */
    public boolean deleteFile(String fileName) throws NameNodeException {
        String cleanFileName = cleanPath(fileName);
        Node node = this.findNode(fileName);
        if (node.isFile()) {
            node.getParent().getChildren().remove(node.getPath());
            return true;
        }
        throw new NameNodeException(fileName + " is not file");
    }

    /**
     * 如果path带有/开头，则去掉
     *
     * @param path
     * @return
     */
    public String cleanPath(String path) {
        if (path.startsWith("/")) {
            return path.substring(1).trim();
        }
        return path.trim();
    }

    /**
     * 通过editlog进行回放
     *
     * @param editLog
     */
    public void replay(EditLog editLog) throws NameNodeException {
        int opType = editLog.getOpType();
        switch (opType) {
            case MKDIR:
                mkdir(editLog.getPath(), editLog.getAttrMap());
            case DELETE_FILE:
                deleteFile(editLog.getPath());

        }
    }
}
