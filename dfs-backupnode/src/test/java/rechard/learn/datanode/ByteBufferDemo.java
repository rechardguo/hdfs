package rechard.learn.datanode;

import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * @author Rechard
 **/
public class ByteBufferDemo {

    @Test
    public void test() {
        ByteBuffer buffer = ByteBuffer.allocate(10);
        buffer.put((byte) 'a');
        buffer.put((byte) 'b');
        buffer.put((byte) 'c');
        buffer.put((byte) 'c');
        buffer.put((byte) 'c');
        buffer.put((byte) 'c');
        buffer.put((byte) 'c');
        buffer.put((byte) 'c');

        System.out.println(buffer.position());
        System.out.println(buffer.limit());
        System.out.println(buffer.capacity());

        //slice其实得到是空白
        ByteBuffer slice = buffer.slice();
        System.out.println(slice.position());
        System.out.println(slice.limit());
        System.out.println(slice.capacity());
        System.out.println(slice.put((byte) '1'));
        System.out.println(slice.put((byte) '2'));

        buffer.flip();
        buffer.limit(10);
        for (int i = 0; i < buffer.capacity(); i++) {
            System.out.println((byte) buffer.get());
        }
    }
}
