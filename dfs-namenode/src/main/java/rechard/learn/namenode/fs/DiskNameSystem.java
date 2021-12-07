package rechard.learn.namenode.fs;


import rechard.learn.namenode.config.NameNodeConfig;

/**
 * 磁盘文件管理系统
 *
 * @author Rechard
 **/
public class DiskNameSystem extends AbstractFSNameSystem {

    private NameNodeConfig nameNodeConfig;

    public DiskNameSystem(NameNodeConfig nameNodeConfig) {
        this.nameNodeConfig = nameNodeConfig;
    }

    @Override
    public void recoveryNamespace() {
        //从nameNodeConfig里找到fsimage 和editlog 重新构建出1个
    }
}
