package rechard.learn.namenode.protocol;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

/**
 * 消息包
 *
 * @author Rechard
 **/
@Data
@Builder
public class Packet<T> {
    private int msgType;//消息的类型
    private Map header; //消息头
    private byte[] body;
}
