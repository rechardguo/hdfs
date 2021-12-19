package rechard.learn.dfs;

import rechard.learn.dfs.callback.MkDirResponse;
import rechard.learn.dfs.common.constant.MsgType;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * @author Rechard
 **/
public class ClientA {

    public static void main(String[] args) {
        FileSystemClient fsClient = new FileSystemClient("user02", "token02");
        //消息回调
        fsClient.setCallback(res -> {
            if (res.msgType() == MsgType.FS_OP_MKDIR_RESPONSE) {
                MkDirResponse mkDirResponse = (MkDirResponse) res;
                Map respMap = (Map) mkDirResponse.data();
                System.out.println(String.format("收到name node处理的结果:%s", respMap.get("result")));
            }
        });
        fsClient.connect("localhost", 10001);//namenode
        //imClient.auth();
        Scanner scanner = new Scanner(System.in);
        String msg = null;
        while (!(msg = scanner.nextLine()).equals("bye")) {
            String[] cmdInfo = msg.split(" ");
            String cmd = cmdInfo[0];
            if (cmd.equalsIgnoreCase("mkdir") ||
                    cmd.equalsIgnoreCase("ddir") ||
                    cmd.equalsIgnoreCase("cfile") ||
                    cmd.equalsIgnoreCase("dfile")) {
                Map map = new HashMap<>();
                fsClient.send(cmd, cmdInfo[1], map);
            } else {
                System.err.println("invalid command!");
            }

        }
    }
}
