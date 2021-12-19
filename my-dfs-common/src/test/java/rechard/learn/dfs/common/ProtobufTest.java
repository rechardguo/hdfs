package rechard.learn.dfs.common;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ruyuan.dfs.model.common.NettyPacketHeader;
import org.junit.Assert;
import org.junit.Test;

/**
 * protobuf 生成的类的后的使用
 * 1.对象转成byte[]
 * 2.byte[]转回类对象
 *
 * @author Rechard
 **/
public class ProtobufTest {

    @Test
    public void testCreateMapUse() throws InvalidProtocolBufferException {
        NettyPacketHeader.Builder headerBulder = NettyPacketHeader.newBuilder();
        headerBulder.putHeaders("v1", "t1");
        headerBulder.putHeaders("v2", "t2");
        NettyPacketHeader header = headerBulder.build();
        byte[] bytes = header.toByteArray();
        NettyPacketHeader header2 = NettyPacketHeader.parseFrom(bytes);
        Assert.assertEquals(header.getHeadersMap().get("v2"), header2.getHeadersMap().get("v2"));

    }
}
