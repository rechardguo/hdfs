package rechard.learn.dfs.namenode.fs;

import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;

/**
 * @author Rechard
 **/
public class FileNameSortTest {

    @Test
    public void testSort() {
        File[] files = new File[5];

        for (int i = files.length - 1; i >= 0; i--) {
            files[i] = new File(i + "");
        }
        Arrays.sort(files, Comparator.reverseOrder());
        for (int i = 0; i < files.length; i++) {
            System.out.println(files[i]);
        }

        for (File f : files) {
            System.out.println(f);
        }
    }

}
