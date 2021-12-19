package rechard.learn.dfs.namenode.fs;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import rechard.learn.dfs.common.exeception.NameNodeException;
import rechard.learn.dfs.common.fs.FSDirectory;
import rechard.learn.dfs.common.fs.Node;

import java.util.HashMap;
import java.util.Map;

/**
 * 测试文件目录的增删改查的单元测试类
 *
 * @author Rechard
 **/
public class FSDirectoryTest {

    FSDirectory directory = null;

    @Before
    public void init() {
        directory = new FSDirectory();
    }

    @Test
    public void testMkdir() throws NameNodeException {
        Map<String, String> attr = new HashMap<>();
        attr.put("t1", "v1");
        attr.put("t2", "v2");
        directory.mkdir("/a/b/c", attr);

        Node node = directory.findNode("/a/b/c");
        Assert.assertEquals("v1", node.getAttr().get("t1"));
        Assert.assertEquals("v2", node.getAttr().get("t2"));
    }

    @Test
    public void testFindNode() throws NameNodeException {
        Map<String, String> attr = new HashMap<>();
        attr.put("t1", "v1");
        attr.put("t2", "v2");
        directory.mkdir("a/b/c", attr);

        Node node = directory.findNode("a/b/c");
        Assert.assertTrue(node.isDirectory());
    }


    @Test
    public void testCreateFile() throws NameNodeException {
        directory.mkdir("a/c/c", null);
        Map<String, String> attr = new HashMap<>();
        attr.put("t1", "v1");
        attr.put("t2", "v2");
        directory.createFile("a/c/c/my.txt", attr);

        Node node = directory.findNode("a/c/c/my.txt");
        Assert.assertTrue("check a/c/c/my.txt is file?", node.isFile());
    }


    @Test(expected = NameNodeException.class)
    public void testDeleteFile() throws NameNodeException {
        directory.mkdir("a/c/c", null);
        Map<String, String> attr = new HashMap<>();
        attr.put("t1", "v1");
        attr.put("t2", "v2");
        directory.createFile("a/c/c/my.txt", attr);
        Assert.assertTrue("check file delete true:", directory.deleteFile("a/c/c/my.txt"));
        Node node = directory.findNode("a/c/c/my.txt");
    }

}
