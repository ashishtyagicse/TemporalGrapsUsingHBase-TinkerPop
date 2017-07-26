package HBaseBinaryTree;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseVertex;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TrueTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


import static HBaseBinaryTree.configuration.*;
/**
 * Created by ashishtyagi on 11/28/16.
 * Query the generated graph
 * Find root node for a given node at given date
 * Find and print the entire graph on a given date
 */
public class queryHierarchy {

    public HBaseGraph Graph;
    public queryHierarchy(HBaseGraph graph) {
        Graph = graph;
    }

    public HBaseGraph getGraph() {
        return Graph;
    }
    public void setGraph(HBaseGraph graph) {
        Graph = graph;
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        long startTimeTask = System.currentTimeMillis();
        System.out.println("> Querying data for " + NUMBER_OF_HIERARCHY + " hierarchy's");
        for (int i = 0; i < NUMBER_OF_HIERARCHY; i++) {
            HBaseGraph Graph = createGraph(i);
            queryHierarchy Q = new queryHierarchy(Graph);

//          Check the root for given node and time
            startTimeTask = System.currentTimeMillis();
            System.out.println("Checking for node " + CHECK_NODE + ":" );
            Q.getRoot((HBaseVertex) Q.getNode(CHECK_NODE), true, getDate(GRAPH_PRINT_DATE));
            System.out.println("Total run time for getting Root: "
                    + (System.currentTimeMillis() - startTimeTask)
                    + " MiliSeconds");


//          Print entire graph on a specific date
            startTimeTask = System.currentTimeMillis();
            System.out.println(StringUtils.repeat("=", 200));
            Q.printGraphThreaded(GRAPH_PRINT_DATE);
            System.out.println("Total run time for Graph Print: "
                    + (System.currentTimeMillis() - startTimeTask)
                    + " MiliSeconds");

//            Q.printGraph((HBaseVertex) Graph.allVertices().next(), GRAPH_PRINT_DATE);

//            Check for nodes and get root in increasing order of dates
//            int NodePerPrefix = NUMBER_OF_HIERARCHY_NODES / HIERARCHY_NODE_PREFIX.length;
//            for (int j = 0; j < HIERARCHY_NODE_PREFIX.length; j++) {
//                String NodePrefix = HIERARCHY_NODE_PREFIX[j];
//                for (int NodeNum = 1; NodeNum <= NodePerPrefix; NodeNum++) {
//                    System.out.println("=========" + NodePrefix + "_" + NodeNum + "====" + "2016-"+ (22 + j) +"-11" + "=========");
//                    Q.getRoot((HBaseVertex) Q.getNode(NodePrefix + "_" + NodeNum), true, getDate("2016-"+ (22 + j) +"-11"));
//                }
//            }

        }
        long endTime   = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Total run time: " + totalTime + " MiliSeconds");
    }


    /**
     * Traverse the entire graph with multithreading approach printing only Level value
     * Faster but needs more resources
     * @param GraphDate Date for which graph needs to be printed
     */
    public void printGraphThreaded(String GraphDate){
        HBaseVertex V = (HBaseVertex) Graph.allVertices().next();
        final long TimeInterval = getDate(GraphDate);
        System.out.println("Printing entire graph as on " + GraphDate);
        ArrayList<Vertex> VList = getRoot(V,false, TimeInterval);
        HashMap ProcessedNodes = new HashMap();
//        ArrayList<String> ProcessedNodes = new ArrayList<>();
        Vertex Root = VList.get(VList.size() - 1 );
        final Queue<Vertex> graphQueue = new LinkedList<>();

        int Level = 0;
        graphQueue.add(null);
        graphQueue.add(Root);
        VList.clear();
        ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
        while (graphQueue.size() > 0){
            Vertex node = graphQueue.poll();
            if (node == null){
                if (graphQueue.size() > 0) {
                    System.out.println("Level: " + Level + " > " );
                    Level++;
                    ArrayList<Future<ArrayList<Vertex>>> ProcessNodes = new ArrayList<>();
                    final ArrayList<Vertex> VResFuture = new ArrayList<>();
                    while(graphQueue.peek() != null) {
                        final Vertex LevelNode = graphQueue.poll();
                        ProcessNodes.add(executorService.submit(new Callable<ArrayList<Vertex>>() {
                            @Override
                            public ArrayList<Vertex> call() throws Exception {
                                ArrayList<Vertex> VRes = new ArrayList<>();

                                for (Vertex Child : getChild((HBaseVertex) LevelNode, TimeInterval)) {
                                    VRes.add(Child);
                                    VResFuture.add(Child);
                                }
                                return VRes;
                            }
                        }));
                    }
                    try {
                        graphQueue.add(null);
                        for (Future<ArrayList<Vertex>> TaskFuture: ProcessNodes ) {
                            VList = TaskFuture.get();
                            for (Vertex Child : VList) {
                                if (ProcessedNodes.containsKey(Child.id().toString())) {
                                    continue;
                                }
                                ProcessedNodes.put(Child.id().toString(), true);
                                graphQueue.add(Child);
                            }
                        }

                    }catch(Exception E){
                        System.out.println("Thread pool exception terminating execution");
                        System.out.println (E.getStackTrace());
                        System.exit(0);
                    }
                }
            }
        }
        executorService.shutdownNow();
    }


    /**
     * Prints the entire graph on a given date with single thread approach and prints the entire graph
     * One level at a time. really slow on graph larger than 10 levels use printGraphThreaded instead
     * @param GraphDate Date for which graph needs to be printed
     */
    public void printGraph(String GraphDate){
        HBaseVertex V = (HBaseVertex) Graph.allVertices().next();
        long TimeInterval = getDate(GraphDate);
        System.out.println("Printing entire graph as on " + GraphDate);
        ArrayList<Vertex> VList = getRoot(V,false, TimeInterval);
        ArrayList<String> ProcessedNodes = new ArrayList<>();
        Vertex Root = VList.get(VList.size() - 1 );
        Queue<Vertex> graphQueue = new LinkedList<>();

        int Level = 0;
        String PrintString = "";
        graphQueue.add(Root);
        graphQueue.add(null);
        VList.clear();
        while (graphQueue.size() > 0){
            Vertex node = graphQueue.poll();
            if (node == null){
                System.out.println("Level: " + Level + " > " + StringUtils.center(PrintString,200));
                Level++;
                PrintString = "";
                if (graphQueue.size() > 0){
                    graphQueue.add(null);
                }
                continue;
            }
            if (ProcessedNodes.contains(node.id().toString())){
                continue;
            }
            PrintString += "|" + node.id() + "|" ;
            ProcessedNodes.add(node.id().toString());

            VList = getChild((HBaseVertex) node, TimeInterval);
            for (Vertex Child : VList){
                graphQueue.add(Child);
            }
        }
    }

    /**
     * Finds the root for given vertex on given time or time range
     * Time range is not tested and can be tricky
     * loops are avoided
     * @param V Vertex for which root is required
     * @param Print If true entire path till root will be printed
     * @param TimeInterval Date or date range (in Second from epoc) on which root is required
     * @return ArrayList of all the vertex from Given vertex till root
     */
    public ArrayList<Vertex> getRoot(HBaseVertex V, Boolean Print, Object... TimeInterval) {
        return getRoot(V,Print,null, TimeInterval);
    }

    public ArrayList<Vertex> getRoot(HBaseVertex V, Boolean Print, ArrayList<Vertex> Res, Object... TimeInterval) {
        if (Res == null) {
            Res = new ArrayList<>();
        }
        Res.add(V);
        if (Print) {
            System.out.println(" ↴ " + V.id());
        }
        Vertex Parent = getParent(V, TimeInterval);
        if (Parent == null) {
            return Res;
        }
        if (!Res.contains(Parent)) {
            Res.addAll(getRoot((HBaseVertex) Parent, Print, Res, TimeInterval));
        }
        return Res;
    }

    /**
     * Get the parent of given vertex on given time
     * @param V Vertex for which parent is required
     * @param TimeInterval Date or date range (in Second from epoc) on which parent is required
     * @return Parent vertex for given Vertex
     */
    public Vertex getParent(HBaseVertex V, Object... TimeInterval){
        ArrayList<Vertex> Res = getRelated(V, Direction.IN, TimeInterval);
        if (Res.size() == 0){
            return null;
        }
        return Res.get(0);
    }

    /**
     * Get the child vertex's for given vertex on given time
     * @param V Vertex for which child vertex are required
     * @param TimeInterval Date or date range (in Second from epoc) on which child are required
     * @return ArrayList of all the child node for given Vertex
     */
    public ArrayList<Vertex> getChild(HBaseVertex V, Object... TimeInterval){
        return getRelated(V, Direction.OUT, TimeInterval);
    }


    /**
     * Generic method for getting Parent or Child for given Vertex on given time
     * @param V Vertex for which Parent vertex or child vertex are required
     * @param D Direction to define if Child is required or Parent
     * @param TimeInterval Date or date range (in Second from epoc) on which Parent vertex or child vertex are required
     * @return ArrayList of Parent vertex or child vertex
     */
    public ArrayList<Vertex> getRelated(HBaseVertex V, Direction D, Object... TimeInterval){
        ArrayList<Vertex> ResVertex = new ArrayList<>();
        ArrayList<Edge> ResEdge = new ArrayList<>();
        Iterator<Edge> E1 = null;
        Iterator<Edge> E2 = null;

        if (TimeInterval.length == 0){
            E1 = V.edges(D, "Parent", "Till", Long.MAX_VALUE);
        }
        if (TimeInterval.length == 1){
            E1 = V.edges(D, "Parent", "since", HIERARCHY_CREATE_TIME,  TimeInterval[0]);
            E2 = V.edges(D, "Parent", "Till", TimeInterval[0],  HIERARCHY_END_TIME + UPDATE_INTERVAL);
        }
        if (TimeInterval.length == 2){
            E1 = V.edges(D, "Parent", "since", HIERARCHY_CREATE_TIME,  TimeInterval[1]);
            E2 = V.edges(D, "Parent", "Till", TimeInterval[0], HIERARCHY_END_TIME + UPDATE_INTERVAL);
        }
        while (E1.hasNext()){
            ResEdge.add(E1.next());
        }
        while (E2.hasNext()){
            Edge E = E2.next();
            if (ResEdge.contains(E)){
                if(D == Direction.IN) {
                    ResVertex.add(E.outVertex());
                }
                else if(D == Direction.OUT){
                    ResVertex.add(E.inVertex());
                }
            }
        }
        return ResVertex;
    }


    /**
     * Get the vertex from graph based on vertex ID name
     * @param NodeName String ID for the Vertex
     * @return Vertex from graph
     */
    public Vertex getNode(String NodeName){
        return Graph.vertex(NodeName);
    }


    /**
     * Get Seconds from Epoc for given date String
     * @param s Date string in format yyyy-dd-MM
     * @return Seconds from Epoc
     */
    private static long getDate(String s) {
        DateFormat df = new SimpleDateFormat("yyyy-dd-MM");
        try {
            Date startDate = df.parse(s);
            //noinspection Since15
            return startDate.toInstant().toEpochMilli() / 1000L;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * Connect to HBASE and get the graph object
     * @param hierarchyNumber name of Graph
     * @return Hbase graph
     */
    private static HBaseGraph createGraph(int hierarchyNumber) {
        connection con = new connection(String.valueOf(hierarchyNumber), false);
        HBaseGraph graph = con.getGraph();
        System.out.println(">> Accessing Graph/hierarchy: " + HIERARCHY_PREFIX + hierarchyNumber);
        return graph;
    }

}
