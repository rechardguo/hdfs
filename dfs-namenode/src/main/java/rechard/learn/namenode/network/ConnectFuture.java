package rechard.learn.namenode.network;

import lombok.Data;

/**
 * 连接其它服务器的结果
 *
 * @author Rechard
 **/
@Data
public class ConnectFuture {
  private boolean success;
  private boolean done;
  private Throwable throwable;

  //等待多长时间
  public void await() {

  }

}
