package rechard.learn.dfs.common.fs;

import rechard.learn.dfs.common.exeception.NameNodeException;

import java.util.Map;

/**
 * 文件系统
 * 为什么这里需要1个接口
 *
 * @author Rechard
 **/
public interface FSNameSystem {
    /**
     * 创建文件夹
     *
     * @param path 文件路径
     * @param attr 文件属性
     */
    void mkdir(String path, Map<String, String> attr) throws NameNodeException;

    /**
     * 创建文件
     *
     * @param filename 文件名称
     * @param attr     文件属性
     * @return 是否创建成功
     */
    boolean createFile(String filename, Map<String, String> attr) throws NameNodeException;


    /**
     * 删除文件
     *
     * @param filename 文件名
     * @return 是否删除成功
     */
    boolean deleteFile(String filename) throws NameNodeException;
}
