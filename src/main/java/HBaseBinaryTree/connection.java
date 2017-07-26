package HBaseBinaryTree;


import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphConfiguration;
import org.apache.commons.configuration.Configuration;

import static HBaseBinaryTree.configuration.*;

/**
 * Created by ashishtyagi on 11/23/16.
 * Connect to HBASE and return the graph instance to interact with
 * Connection can be made in write or read only mode
 */


public class connection {

    private Configuration cfg;
    private HBaseGraph graph;

    public Configuration getCfg() {
        return cfg;
    }
    public void setCfg(Configuration cfg) {
        this.cfg = cfg;
    }
    public void setGraph(HBaseGraph graph) {
        this.graph = graph;
    }
    public HBaseGraph getGraph() {
        return graph;
    }

    public connection(String HIERARCHY_NAME, Boolean Create){
        this.cfg = new HBaseGraphConfiguration()
                .setInstanceType(HBaseGraphConfiguration.InstanceType.DISTRIBUTED)
                .setGraphNamespace(HIERARCHY_PREFIX + HIERARCHY_NAME)
                .setCreateTables(Create)
                .setRegionCount(REGION_COUNT)
                .set(ZOOKEPER_QORUM, ZOOKEPER_QORUM_NODE)
                .set(ZOOKEPER_PARENT, ZOOKEPER_PARENT_NODE);
        this.graph = HBaseGraph.open(cfg);
    }

}
