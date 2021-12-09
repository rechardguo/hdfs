package rechard.learn.backnode.config;

import lombok.Builder;
import lombok.Data;

import java.util.Properties;

/**
 * @author Rechard
 **/
@Builder
@Data
public class BackupNodeConfig {
    private int port;
    private String dfsHome;
    private String masterAddr;//主机的ip:port
    private String masterPass;

    //解析properties文件
    public static BackupNodeConfig loadFromResource(Properties properties) {
        int port = Integer.parseInt(properties.getProperty("backup.port"));
        String masterAddr = properties.getProperty("master.addr");
        String masterPass = properties.getProperty("master.auth.pass");

        return BackupNodeConfig.builder()
                .port(port)
                .masterAddr(masterAddr)
                .build();
    }

    public String getMasterIp() {
        String[] addr = masterAddr.split(":");
        return addr[0];
    }

    public int getMasterPort() {
        String[] addr = masterAddr.split(":");
        return Integer.parseInt(addr[1]);
    }

}
