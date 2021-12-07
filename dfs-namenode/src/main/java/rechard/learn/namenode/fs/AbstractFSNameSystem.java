package rechard.learn.namenode.fs;


import rechard.learn.namenode.exeception.NameNodeException;

import java.util.Map;

/**
 * @author Rechard
 **/
public abstract class AbstractFSNameSystem implements FSNameSystem {

    private FSDirectory fsDirectory;

    public AbstractFSNameSystem() {
        this.fsDirectory = new FSDirectory();
    }

    @Override
    public void mkdir(String path, Map<String, String> attr) throws NameNodeException {
        this.fsDirectory.mkdir(path, attr);
    }

    @Override
    public boolean createFile(String filename, Map<String, String> attr) throws NameNodeException {
        return this.fsDirectory.createFile(filename, attr);
    }

    @Override
    public boolean deleteFile(String filename) throws NameNodeException {
        return this.fsDirectory.deleteFile(filename);
    }

    public abstract void recoveryNamespace();
}
