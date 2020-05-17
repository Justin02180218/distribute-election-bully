package com.justin.distribute.election.bully.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class PropertiesUtil {

    public static final PropertiesUtil getInstance() {
        return new PropertiesUtil();
    }

    public static Properties getProParms() {
        return PropertiesUtil.getInstance().getProParms("/bully/node.properties");
    }

    public static String[] getNodesAddress() {
        return getClusterNodesAddress().split(",");
    }

    public static String getClusterNodesAddress() {
        return getProParms().getProperty("cluster.nodes.address");
    }

    public static String getLocalAddress() { return getProParms().getProperty("node.local.address"); }

    public static int getProcessorThreads() {
        return Integer.parseInt(getProParms().getProperty("node.processor.threads"));
    }

    private Properties getProParms(String propertiesName) {
        InputStream is = getClass().getResourceAsStream(propertiesName);
        Properties prop = new Properties();
        try {
            prop.load(is);
        } catch (IOException e1) {
            e1.printStackTrace();
        }finally {
            if(is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return prop;
    }
}
