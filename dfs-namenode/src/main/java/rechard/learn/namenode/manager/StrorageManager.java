package rechard.learn.namenode.manager;

import rechard.learn.namenode.fs.FSDirectory;

/**
 * 负载命名目录存储结构的
 *
 * @author Rechard
 **/
public class StrorageManager {

    private FSDirectory fsDirectory;

    public StrorageManager(FSDirectory fsDirectory) {
        this.fsDirectory = fsDirectory;
    }

    //从磁盘文件里恢复数据
    public void recover() {

    }

    public void mkdir(String path) {
        //1.写入文件目录树
        //2.写入磁盘文件数据
    }

}
