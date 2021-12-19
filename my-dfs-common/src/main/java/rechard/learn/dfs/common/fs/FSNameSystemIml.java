package rechard.learn.dfs.common.fs;

import rechard.learn.dfs.common.exeception.NameNodeException;

import java.io.IOException;
import java.util.Map;

/**
 * @author Rechard
 **/
public class FSNameSystemIml implements FSNameSystem {

    private FSDirectory fsDirectory;


    public FSNameSystemIml(FSDirectory fsDirectory) throws IOException {
        this.fsDirectory = fsDirectory;
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

    public FSDirectory getFsDirectory() {
        return fsDirectory;
    }

}
