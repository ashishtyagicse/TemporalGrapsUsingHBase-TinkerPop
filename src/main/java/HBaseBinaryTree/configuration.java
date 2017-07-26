package HBaseBinaryTree;

/**
 * Created by ashishtyagi on 11/23/16.
 * Configurable parameters used for connecting, generating data and query process
 */
public class configuration {

    // HBASE Connection configuration
    public static final String ZOOKEPER_QORUM = "hbase.zookeeper.quorum";
    public static final String ZOOKEPER_QORUM_NODE = "ashish-4.vpc.cloudera.com";
    public static final String ZOOKEPER_PARENT = "zookeeper.znode.parent";
    public static final String ZOOKEPER_PARENT_NODE = "/hbase";
    public static final int REGION_COUNT = 5;
    public static final String HIERARCHY_PREFIX = "TempGRAPH_";

    // Datagen configuration
    public static final int NUMBER_OF_HIERARCHY = 1;
    public static final int NUMBER_OF_HIERARCHY_NODES = 120000;
    public static final String[] HIERARCHY_NODE_PREFIX = { "Temp" , "Temp_US" , "Temp_IB" , "Temp_RISK" , "Temp_INDIA" , "Temp_EMEA" };
    public static final long HIERARCHY_CREATE_TIME = 1479600000L;  // Start creating nodes from time Date 11-20-2016 0:0:0
    public static final long UPDATE_INTERVAL = 86400L ;   // Update interval days * milli
    public static final long HIERARCHY_END_TIME = Long.MAX_VALUE - UPDATE_INTERVAL;  // End date for Till property
    public static final int NO_OF_UPDATES_PER_INTERVAL = 2; // No of graph updates per update interval

    // Query configuration
    public static final int NUMBER_OF_THREADS = 5;
    public static final String GRAPH_PRINT_DATE = "2016-20-11";
    public static final String CHECK_NODE = "Temp_EMEA_10000";

}
