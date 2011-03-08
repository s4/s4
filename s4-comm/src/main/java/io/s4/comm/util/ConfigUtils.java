package io.s4.comm.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.s4.comm.util.ConfigParser.Cluster;
import io.s4.comm.util.ConfigParser.Cluster.ClusterType;
import io.s4.comm.util.ConfigParser.ClusterNode;

public class ConfigUtils {
    public static List<Map<String, String>> readConfig(String configFilename,
            String clusterName, ClusterType clusterType, boolean isStatic) {
        ConfigParser parser = new ConfigParser();
        ConfigParser.Config config = parser.parse(configFilename);

        // find the requested cluster
        Cluster cluster = null;
        for (Cluster checkCluster : config.getClusters()) {
            if (checkCluster.getName().equals(clusterName)
                    && checkCluster.getType().equals(clusterType)) {
                cluster = checkCluster;
                break;
            }
        }
        if (cluster == null) {
            throw new RuntimeException("Cluster " + clusterName + " of type "
                    + clusterType + " not configured");
        }
        return readConfig(cluster, clusterName, clusterType, isStatic);
    }

    public static List<Map<String, String>> readConfig(Cluster cluster,
            String clusterName, ClusterType clusterType, boolean isStatic) {

        List<Map<String, String>> processSet = new ArrayList<Map<String, String>>();
        for (ClusterNode node : cluster.getNodes()) {
            Map<String, String> nodeInfo = new HashMap<String, String>();
            if (node.getPartition() != -1) {
                nodeInfo.put("partition", String.valueOf(node.getPartition()));
            }
            if (node.getPort() != -1) {
                nodeInfo.put("port", String.valueOf(node.getPort()));
            }
            nodeInfo.put("cluster.type", String.valueOf(clusterType));
            nodeInfo.put("cluster.name", clusterName);
            if (isStatic) {
                nodeInfo.put("address", node.getMachineName());
                nodeInfo.put("process.host", node.getMachineName());
            }
            nodeInfo.put("mode", cluster.getMode());
            nodeInfo.put("ID", node.getTaskId());
            processSet.add(nodeInfo);
        }
        return processSet;
    }
}
