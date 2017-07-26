package HBaseBinaryTree;

import io.hgraphdb.ElementType;
import io.hgraphdb.HBaseBulkLoader;
import io.hgraphdb.HBaseGraph;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.ArrayList;
import java.util.Random;
import static HBaseBinaryTree.configuration.*;

/**
 * Created by ashishtyagi on 11/23/16.
 * Generates data for creating a NUMBER_OF_HIERARCHY_NODES binary tree
 * Vertex are prefix by HIERARCHY_NODE_PREFIX and followed by number
 * Edges are created with prefix edge followed by number
 * Vertex has created_on property and edges has since and Till property
 * Updates are made one every UPDATE_INTERVAL (which is 1 day) from HIERARCHY_CREATE_TIME onwards
 * Making NO_OF_UPDATES_PER_INTERVAL updates till current day
 * Updates are random and can create loops in tree
 * One sample Manual update method included for testing
 */
public class datagen {
    public static void main(String[] args) {
        long StartTime = System.currentTimeMillis();
        System.out.println("> Creating data for " + NUMBER_OF_HIERARCHY + " hierarchy's");
        for (int i = 0; i < NUMBER_OF_HIERARCHY; i++) {
            HBaseGraph Graph = createGraph(i);
            Graph.createIndex(ElementType.EDGE, "Parent", "since");
            Graph.createIndex(ElementType.EDGE, "Parent", "Till");
            ArrayList<Vertex> GraphVertex = createVertex(Graph);
            ArrayList<Edge> GraphEdges = createEdges(Graph, GraphVertex);
//            updateEdges(Graph, GraphVertex, GraphEdges);
//            updateEdgesManual(Graph, GraphEdges, GraphVertex); // use 12 nodes / 6 prefix of 2 nodes each for this one
        }
        long EndTime   = System.currentTimeMillis();
        long TotalTime = EndTime - StartTime;
        System.out.println("Total run time: " + TotalTime + " MiliSeconds");
    }


    /**
     * Connect to HBASE and create the Graph and tables
     * @param HierarchyNumber Name for graph to be created
     * @return Hbase Graph object
     */
    private static HBaseGraph createGraph(int HierarchyNumber) {
        connection Con = new connection(String.valueOf(HierarchyNumber), true);
        HBaseGraph Graph = Con.getGraph();
        System.out.println(">> Created Graph/hierarchy: " + HIERARCHY_PREFIX + HierarchyNumber);
        return Graph;
    }

    /**
     * Generates Vertex for the graph in bulk mode
     * @param Graph Graph object
     * @return ArrayList of all generated vertex
     */
    private static ArrayList<Vertex> createVertex(HBaseGraph Graph) {
        int NodePerPrefix = NUMBER_OF_HIERARCHY_NODES / HIERARCHY_NODE_PREFIX.length;
        ArrayList<Vertex> GraphVertex = new ArrayList<>();
        System.out.println(">> Creating Vertex");
        HBaseBulkLoader VertexBulkLoad = new HBaseBulkLoader(Graph);
        for (int i = 0; i < HIERARCHY_NODE_PREFIX.length; i++) {
            String NodePrefix = HIERARCHY_NODE_PREFIX[i];
            for (int NodeNum = 1; NodeNum <= NodePerPrefix; NodeNum++) {
                Vertex v = VertexBulkLoad.addVertex(T.id, NodePrefix + "_" + NodeNum, "created_on", HIERARCHY_CREATE_TIME);
                GraphVertex.add(v);
//                Print statements commented
//                System.out.println(">>> Created Vertex: Key=" + NodePrefix + "_" + NodeNum + " created_on=" + HIERARCHY_CREATE_TIME);
//                if( (i+1) * NodeNum % 1000 == 0){
//                    System.out.println("Current number of vertex: " + (i+1) * NodeNum);
//                }
            }
        }
        VertexBulkLoad.close();
        return GraphVertex;
    }

    /**
     * Generated edges for all the vertex and create Binary tree
     * @param Graph Graph object
     * @param GraphVertex All the generated vertex for this graph
     * @return ArrayList of all the edges generated
     */
    private static ArrayList<Edge> createEdges(HBaseGraph Graph, ArrayList<Vertex> GraphVertex) {
        System.out.println(">> Creating Edges");
        ArrayList<Edge> GraphEdges = new ArrayList<>();
        HBaseBulkLoader EdgeBulkLoad = new HBaseBulkLoader(Graph);
        for (int i = 1; (2 * i) - 1 < GraphVertex.size(); i++) {
            Vertex V = GraphVertex.get(i-1);
            Vertex V1 = GraphVertex.get(( 2 * i ) - 1);

            Edge E1 = EdgeBulkLoad.addEdge(V, V1, "Parent"
                    ,T.id, "edge_" + (( 2 * i ) - 1)
                    , "since" , HIERARCHY_CREATE_TIME
                    , "Till", HIERARCHY_END_TIME);

//            Print statements commented
//            System.out.println(">>> Adding Edge: Key=edge_" + (( 2 * i ) - 1)
//                    + " From: " + V.id().toString() + " To: " + V1.id().toString()
//                    + " since:" + HIERARCHY_CREATE_TIME + " Till:" + HIERARCHY_END_TIME);
            GraphEdges.add(E1);

            if (2 * i < GraphVertex.size()) {
                Vertex V2 = GraphVertex.get((2 * i));
                Edge E2 = EdgeBulkLoad.addEdge(V, V2, "Parent"
                        , T.id, "edge_" + (2 * i)
                        , "since", HIERARCHY_CREATE_TIME
                        , "Till", HIERARCHY_END_TIME);

                GraphEdges.add(E2);
//                Print statements commented
//                System.out.println(">>> Adding Edge: Key=edge_" + (2 * i)
//                        + " From: " + V.id().toString() + " To: " + V2.id().toString()
//                        + " since:" + HIERARCHY_CREATE_TIME + " Till:" + HIERARCHY_END_TIME);
//                if(((i * 2 ) % 1000 ) == 0) {
//                    System.out.println("Current number of Edges: " + (i * 2));
//                }
            }
        }
        EdgeBulkLoad.close();
        return GraphEdges;
    }

    /**
     * Created updates picking up random edges and vertex and creating new edges in place of old one
     * @param Graph Graph object
     * @param GraphVertex List of all graph vertex
     * @param GraphEdges List of all graph edges
     */
    private static void updateEdges(HBaseGraph Graph, ArrayList<Vertex> GraphVertex, ArrayList<Edge> GraphEdges) {
        System.out.println(">> Creating Edge updates");
        Random rand = new Random();
        long UpdateStartTime = HIERARCHY_CREATE_TIME + UPDATE_INTERVAL;
        long UpdateEndTime = System.currentTimeMillis() / 1000L;
        for (long i = UpdateStartTime ; i < UpdateEndTime; i += UPDATE_INTERVAL) {
            for (int j = 0; j < NO_OF_UPDATES_PER_INTERVAL ; j++) {

                Edge UpdateEdge = GraphEdges.get(rand.nextInt(GraphEdges.size() - 1));
                while (!UpdateEdge.property("Till").value().toString().equals("" + HIERARCHY_END_TIME)){
                    UpdateEdge = GraphEdges.get(rand.nextInt(GraphEdges.size() - 1));
                }
                Vertex UpdateVertex = GraphVertex.get(rand.nextInt(GraphVertex.size() - 1 ));
                while (UpdateVertex.id() == UpdateEdge.inVertex().id()){
                    UpdateVertex = GraphVertex.get(rand.nextInt(GraphVertex.size() - 1));
                }

                UpdateEdge.property("Till").remove();
                ElementHelper.attachProperties(UpdateEdge, "Till", i);
                Edge newEdge = UpdateVertex.addEdge(
                        "Parent", UpdateEdge.inVertex(),
                        T.id, "edge_" + (GraphEdges.size() + 1),
                        "since", i,
                        "Till", HIERARCHY_END_TIME);
                GraphEdges.add(newEdge);
//                Print statements commented
//                System.out.println(">>> Updating " + (((i - UpdateStartTime) / UPDATE_INTERVAL) + 20) + '_' + j
//                        + ": Key=" + UpdateEdge.id().toString()
//                        + " From_old: " + UpdateEdge.outVertex().id().toString()
//                        + " To_old: " + UpdateEdge.inVertex().id().toString()
//                        + " <<>> Key=" + newEdge.id().toString()
//                        + " From: " + UpdateVertex.id().toString()
//                        + " To: " + UpdateEdge.inVertex().id().toString());
            }
        }
    }

    /**
     * Create manual test updated to check how other parts of project react to updates
     * @param Graph Graph object
     * @param GraphEdges List of all edges
     * @param GraphVertex List of all vertex
     */
    private static void updateEdgesManual(HBaseGraph Graph, ArrayList<Edge> GraphEdges, ArrayList<Vertex> GraphVertex) {
        System.out.println(">> Creating Edge updates");
        long UpdateStartTime = HIERARCHY_CREATE_TIME + UPDATE_INTERVAL;

        updateManual(Graph, 1, "edge_11", "Temp_EMEA_1", UpdateStartTime);
        updateManual(Graph, 2, "edge_10", "Temp_INDIA_2", UpdateStartTime += UPDATE_INTERVAL/2 );
        updateManual(Graph, 3, "edge__1", "Temp_US_1", UpdateStartTime += UPDATE_INTERVAL/2 );
        updateManual(Graph, 4, "edge__2", "Temp_IB_1", UpdateStartTime += UPDATE_INTERVAL/2 );
        updateManual(Graph, 5, "edge_2", "Temp_2", UpdateStartTime += UPDATE_INTERVAL/2 );
        updateManual(Graph, 6, "edge_5", "Temp_RISK_2", UpdateStartTime += UPDATE_INTERVAL/2 );
        updateManual(Graph, 7, "edge_3", "Temp_1", UpdateStartTime += UPDATE_INTERVAL/2 );
        updateManual(Graph, 8, "edge_4", "Temp_1", UpdateStartTime += UPDATE_INTERVAL/2 );
        updateManual(Graph, 9, "edge__5", "Temp_1", UpdateStartTime += UPDATE_INTERVAL/2 );
        Graph.edge("edge_1").property("Till").remove();
        System.out.println("Removed edge1 ");
        updateManual(Graph, 10, "edge__7", "Temp_2", UpdateStartTime += UPDATE_INTERVAL/2 );
        updateManual(Graph, 11, "edge__8", "Temp_2", UpdateStartTime += UPDATE_INTERVAL/2 );
        updateManual(Graph, 12, "edge__9", "Temp_2", UpdateStartTime += UPDATE_INTERVAL/2 );
    }

    /**
     * Helper function for updateEdgesManual
     * @param graph Graph object
     * @param i New edge suffix number
     * @param EdgeId Old edge ID which needs to be updated
     * @param VertexId New vertex to which new edge will connect
     * @param UpdateTime Time of update
     */
    private static void updateManual(HBaseGraph graph, int i, String EdgeId, String VertexId, long UpdateTime) {
        Edge UpdateEdge = graph.edge(EdgeId);
        Vertex UpdateVertex = graph.vertex(VertexId);
        UpdateEdge.property("Till").remove();
        ElementHelper.attachProperties(UpdateEdge, "Till", UpdateTime);
        Edge NewEdge = UpdateVertex.addEdge(
                "Parent", UpdateEdge.inVertex(),
                T.id, "edge__" + i,
                "since", UpdateTime,
                "Till", HIERARCHY_END_TIME);
        System.out.println(">>> Updating " + i
                + ": Key=" + UpdateEdge.id().toString()
                + " From_old: " + UpdateEdge.outVertex().id().toString()
                + " To_old: " + UpdateEdge.inVertex().id().toString()
                + " <<>> Key=" + NewEdge.id().toString()
                + " From: " + UpdateVertex.id().toString()
                + " To: " + UpdateEdge.inVertex().id().toString());
    }
}
