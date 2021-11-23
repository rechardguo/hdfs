package rechard.learn.namenode;

import cn.hutool.core.io.FileUtil;
import rechard.learn.namenode.config.NameNodeConfig;
import rechard.learn.namenode.constant.NameNodeConstant;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 要求配置的文件统一放到
 * MQ_HOME
 *
 * @author Rechard
 **/
public class NameNodeBootstrap {
    public static void main(String[] args) {
        String dfshome = System.getenv(NameNodeConstant.DFSHOME);
        if (dfshome == null) {
            System.err.println("-c configfig must be not empty");
            System.exit(-1);
        }
        //没指定配置文件则使用默认的文件
        if (args == null || args.length == 0) {
            args = new String[]{"-c", dfshome + File.separator + "conf" + File.separator + "namenode.properties"};
        }
        NameNodeConfig nameNodeConfig = new NameNodeConfig();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-c")) {
                if (i >= args.length - 1) {
                    System.err.println("-c configfig must be not empty");
                    System.exit(-1);
                }
                String file = args[++i];
                //配置文件必须在HDFS_HOME/conf/  下
                InputStream is = FileUtil.getInputStream(dfshome + File.separator + "conf" + File.separator + file);
                try {
                    Properties properties = new Properties();
                    properties.load(is);
                    nameNodeConfig.loadFromResource(properties);
                } catch (IOException e) {
                    System.err.println("load config file fail as" + e.getMessage());
                    System.exit(-1);
                }
            } else {
                System.err.println("-c configfig must be not empty");
                System.exit(-1);
            }
        }

        NameNode nameNode = new NameNode(nameNodeConfig);
        nameNode.start();
    }
}
