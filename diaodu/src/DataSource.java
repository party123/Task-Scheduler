import java.util.*;

/**
 * 利用给定的服务器数和任务书来构建二分图
 */
public class DataSource
{
    //服务器数量，其值应该大于3。
    private int serverCount;

    //任务数量
    private int taskCount;

    //副本数量，即每个任务task有三个数据源
    private static final int duplicationCount = 2;

    //Map的Key为路径，Value为对应的边，如"S -> V1"这条路径代表<S,V1>
    private Map<String,Edge> edges = new HashMap<>();

    //Key:服务器名  Value:当前任务中存放数据所对应的任务集合列表
    private Map<String,Set<String>> preferedTasks = new HashMap<>();

    //此次需要分配的任务集合
    private Set<String> taskSet = new HashSet<>();

    //服务器集合
    private List<Server> serverList = new ArrayList<>();

    //任务之间的紧密度（relationValue[1][2]表示任务1和任务2的紧密度），自身和自身的紧密度为0
    private int[][] relationValue = null;

    public Map<String, Edge> getEdges() {
        return new HashMap<>(edges);
    }

    public int getTaskCount() {
        return taskCount;
    }

    public Map<String, Set<String>> getPreferedTasks() {
        Map<String, Set<String>> newPreferedTasks = new HashMap<>();
        for(String server : preferedTasks.keySet())
            newPreferedTasks.put(server, new HashSet<>(preferedTasks.get(server)));
        return newPreferedTasks;
    }

    public List<Server> getServers() {
        return serverList;
    }

    public Set<String> getTaskSet() {
        return new HashSet<>(taskSet);
    }

    public DataSource(int serverCount, int taskCount) {
        this.serverCount = serverCount;
        this.taskCount = taskCount;
        initGraph();
    }

    public int[][] getRelationValue() {
        return relationValue;
    }

    //Map的Key为路径，Value为对应的边，如 "S->V1" 这条路径代表就代表 <S,V1> 这条边
    public void initGraph() {

        int maxTaskCount = 0;
        //初始化服务器集合
        for(int i=1; i<=serverCount; i++) {
            Server server = new Server("s" + i,0);
            maxTaskCount += server.getSlot();
            serverList.add(server);
        }

        List<Integer> serverNumList = new ArrayList<>();
        Random random = new Random();

        taskCount = maxTaskCount > taskCount ? taskCount : maxTaskCount;
        System.out.println("合理化的任务数（这里假设任务最多按照服务器集合的slot总数）: " + taskCount);

        //随机产生任务之间的紧密度,值越高紧密度越大（0-9）
        relationValue = new int[taskCount][taskCount+1];
        int value = 0;
       // System.out.println("任务之间的紧密值：");
        for(int i=1; i<taskCount; i++)
            for(int j=i+1; j<=taskCount; j++){
                value = new Random().nextInt(10);
                relationValue[i][j] = value;
                //System.out.print(relationValue[i][j] + "(" + i + "," + j +")" + "   ");
                System.out.print(relationValue[i][j] + "\t");
                if(j==taskCount)
                    System.out.println();
            }

        //对于每个任务，都会分配3个服务器给其作为数据放置点
        for(int i = 1; i <= taskCount; i++) {
            String task = "t" + i;
            taskSet.add(task);
            //在服务器中随机进行选取 duplicationCount 个服务器作为其数据放置点
            while(serverNumList.size() < duplicationCount) {
                int serverNum = random.nextInt(serverCount) + 1;
                if(!serverNumList.contains(serverNum))
                    serverNumList.add(serverNum);
            }

            //构建边的信息并将其放入Map中，使得路径可以对应与当前的边
            for(Integer num : serverNumList) {
                String server = "s" + num;
                //本地任务，因此容量(capacity)最大值为 1, 初始残留值为 capacity 值
                Edge edge = new Edge(server, task);
                //加入到路径信息中
                edges.put(edge.getPath(), edge);

                //初始化任务存放集合列表
                if(preferedTasks.containsKey(server))
                    preferedTasks.get(server).add(task);
                else{
                    Set<String> taskSet = new HashSet<>();
                    taskSet.add(task);
                    preferedTasks.put(server,taskSet);
                }
            }
            //增加一个终点T，即每个 task 与 T 都有一条边
            Edge edge =  new Edge(task, "T");
            edges.put(edge.getPath(), edge);
            serverNumList.clear();
        }
    }

    public void TestData2() {

        taskCount = 9;
        serverCount = 4;

        Edge edge1 = new Edge("s1","t1");
        Edge edge2 = new Edge("s1","t2");
        Edge edge3 = new Edge("s1","t3");
        Edge edge4 = new Edge("s1","t4");
        Edge edge5 = new Edge("s1","t5");
        Edge edge6 = new Edge("s1","t7");

        Edge edge7 = new Edge("s2","t1");
        Edge edge8 = new Edge("s2","t2");
        Edge edge9 = new Edge("s2","t3");
        Edge edge10 = new Edge("s2","t4");
        Edge edge11 = new Edge("s2","t6");
        Edge edge12 = new Edge("s2","t7");
        Edge edge13 = new Edge("s2","t8");

        Edge edge14 = new Edge("s3","t3");
        Edge edge15 = new Edge("s3","t4");
        Edge edge16 = new Edge("s3","t5");
        Edge edge17 = new Edge("s3","t6");
        Edge edge18 = new Edge("s3","t7");
        Edge edge19 = new Edge("s3","t8");

        Edge edge20 = new Edge("s4","t1");
        Edge edge21 = new Edge("s4","t2");
        Edge edge22 = new Edge("s4","t5");
        Edge edge23 = new Edge("s4","t6");
        Edge edge24 = new Edge("s4","t8");

        Edge edge25 = new Edge("t1","T");
        Edge edge26 = new Edge("t2","T");
        Edge edge27 = new Edge("t3","T");
        Edge edge28 = new Edge("t4","T");
        Edge edge29 = new Edge("t5","T");
        Edge edge30 = new Edge("t6","T");
        Edge edge31 = new Edge("t7","T");
        Edge edge32 = new Edge("t8","T");

        Edge edge33 = new Edge("s1","t9");
        Edge edge34 = new Edge("s3","t9");
        Edge edge35 = new Edge("s4","t9");
        Edge edge36 = new Edge("t9","T");

        Edge edge37 = new Edge("s1","t10");
        Edge edge38 = new Edge("s2","t10");
        Edge edge39 = new Edge("s4","t10");
        Edge edge40 = new Edge("t10","T");

        edges.put(edge1.getPath(),edge1);
        edges.put(edge2.getPath(),edge2);
        edges.put(edge3.getPath(),edge3);
        edges.put(edge4.getPath(),edge4);
        edges.put(edge5.getPath(),edge5);
        edges.put(edge6.getPath(),edge6);

        edges.put(edge7.getPath(),edge7);
        edges.put(edge8.getPath(),edge8);
        edges.put(edge9.getPath(),edge9);
        edges.put(edge10.getPath(),edge10);
        edges.put(edge11.getPath(),edge11);
        edges.put(edge12.getPath(),edge12);
        edges.put(edge13.getPath(),edge13);

        edges.put(edge14.getPath(),edge14);
        edges.put(edge15.getPath(),edge15);
        edges.put(edge16.getPath(),edge16);
        edges.put(edge17.getPath(),edge17);
        edges.put(edge18.getPath(),edge18);
        edges.put(edge19.getPath(),edge19);

        edges.put(edge20.getPath(),edge20);
        edges.put(edge21.getPath(),edge21);
        edges.put(edge22.getPath(),edge22);
        edges.put(edge23.getPath(),edge23);
        edges.put(edge24.getPath(),edge24);

        edges.put(edge25.getPath(),edge25);
        edges.put(edge26.getPath(),edge26);
        edges.put(edge27.getPath(),edge27);
        edges.put(edge28.getPath(),edge28);
        edges.put(edge29.getPath(),edge29);
        edges.put(edge30.getPath(),edge30);
        edges.put(edge31.getPath(),edge31);
        edges.put(edge32.getPath(),edge32);

        edges.put(edge33.getPath(),edge33);
        edges.put(edge34.getPath(),edge34);
        edges.put(edge35.getPath(),edge35);
        edges.put(edge36.getPath(),edge36);

        edges.put(edge37.getPath(),edge37);
        edges.put(edge38.getPath(),edge38);
        edges.put(edge39.getPath(),edge39);
        edges.put(edge40.getPath(),edge40);

        serverList.add(new Server("s1", 0));
        serverList.add(new Server("s2", 0));
        serverList.add(new Server("s3", 0));
        serverList.add(new Server("s4", 0));
    }

}
