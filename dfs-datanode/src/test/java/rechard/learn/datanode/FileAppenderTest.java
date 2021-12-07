package rechard.learn.datanode;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * @author Rechard
 **/
public class FileAppenderTest {
    private String filePath;
    private FileAppender fileAppender;

    @Before
    public void init() throws IOException {
        this.filePath = System.getProperty("user.dir") + File.separator + "tmp";
        File testFile = new File(filePath + File.separator + "test.txt");
        testFile.delete();
        testFile.createNewFile();
        this.fileAppender = new FileAppender(testFile, 10);
    }

    @Test
    public void testSuccessAdd() {
        this.fileAppender.append(new byte[]{'r', 'r', 'c', 'd', 'c', 'c', '7', '8', '9', '9'});
        this.fileAppender.flush();
    }
}