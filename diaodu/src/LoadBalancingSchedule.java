import java.util.*;

/**
 *  负载均衡调度策略：每次从所有服务器中选取负载值最小的服务器作为分配任务的起点，来达到负载均衡
 *  并且保证分配到的任务均满足本地性(利用增广路径来保证分配给所有服务器的任务均是本地任务)
 */
public class LoadBalancingSchedule
{
    //保存当前经过的路径，即流
    private List<Edge> path = new ArrayList<>();

    //key:服务器名称  value:该服务器当前的负载量
    private Map<String, Integer> serverLoadMap = new HashMap<>();

    //key:任务  value:为该任务分配的服务器
    private Map<String, String> allocatedTask = new HashMap<>();

    /**
     * @param serverCount : 服务器数量
     * @param taskCount ： 任务数量
     * @return 最终的分配结果
     */
    public List<Server> getTaskAllocation(int serverCount, int taskCount) {

        DataSource ds = new DataSource(serverCount, taskCount);

        List<Server> servers = new ArrayList<>(ds.getServers());

        Map<String,Set<String>> preferedTasks = ds.getPreferedTasks();

        //key:路径  value:该路径对应的边对象
        Map<String,Edge> edges = ds.getEdges();

        //key:服务器名  value:该服务器的 slot 数目
        Map<String,Integer> serverSlotMap = new HashMap<>();

        //当前已经负载达到饱和的服务器集合
        Set<String> fullServerSet = new HashSet<>();

        //初始化 serverLoadMap 和 allocatedMap

        for(Server server : servers) {
            serverLoadMap.put(server.getServerName(), server.getLoad());
            serverSlotMap.put(server.getServerName(), server.getSlot());
        }

        //得到负载量最小的服务器名，作为搜索路径的起点
        String minLoadServer = getMinLoadServer(serverLoadMap, fullServerSet);

        //找到一条从最小负载服务器出发的增广路径，将其放入到 path 中。根据path更新服务器的负载load与edges，
        //由于权值为1,因此在走过一次后将路径反向。如走过一条路径 s1->t1->T 此时路径反向变为 T->t1->s1 并且需要更新 s1 的负载
        while(AugmentPath.hasAugmentPath(minLoadServer,"T", path, edges)) {

            for(Edge edge : path) {

                //筛选出以服务器开头的 edge，用以更新服务器的负载
                if(edge.getStart().startsWith("s")) {

                    String server = edge.getStart();
                    String task = edge.getEnd();

                    //进行回流操作
                    withdrawFlow(task, allocatedTask, serverSlotMap, fullServerSet, edges);

                    //将该任务加入已分配服务器集合中
                    allocatedTask.put(task, server);

                    //该服务器的负载加 1
                    serverLoadMap.put(server, serverLoadMap.get(server) + 1);

                    //当前服务器分配到的任务数量达到了该服务器的阈值，此时将该服务器加入负载饱和的服务器集合中
                    if(serverLoadMap.get(server) == serverSlotMap.get(server))
                        fullServerSet.add(server);
                }

                //从原有边信息中移除path中当前处理的边信息
                String key = edge.getPath();
                    if(edges.containsKey(key))
                        edges.remove(key);

                //加入当前处理边的的反向边，权值与当前处理边的权值一致
                Edge reverseEdge = new Edge(edge.getEnd(), edge.getStart());
                edges.put(reverseEdge.getPath(), reverseEdge);
            }

            //所有任务都已经被分配，跳出搜索增广路径
            if(allocatedTask.size() == ds.getTaskCount())
                break;

            path.clear();

            //重新获取最小负载对应的服务器
            minLoadServer = getMinLoadServer(serverLoadMap, fullServerSet);
        }

        //将最终分配结果写回服务器集合
        updateServers(servers, serverLoadMap, allocatedTask);

        return servers;
    }

    /** 如果当前的任务 t 已经分配给了其他服务器 s，此时需要将任务 t 从服务器 s 中移除，也即需要回流操作
     *  并且需要更新服务器的负载，若服务器 s 原先已经满负载，移除当前任务后，应该从满负载服务器集合中移除服务器 s
     * @param task : 当前待分配的任务
     * @param allocatedTask : key:任务  value:分配给该任务的服务器
     * @param serverSlotMap : key:服务器  value:服务器对应的 slot 数目
     * @param fullServerSet : 所有负载达已满的服务器集合
     * @param edges : 二分图边集合
     */
    private void withdrawFlow(String task, Map<String, String> allocatedTask,  Map<String, Integer> serverSlotMap,
                              Set<String> fullServerSet, Map<String,Edge> edges){

        if(allocatedTask.containsKey(task)){

            String preServer = allocatedTask.get(task); //获得分配到该任务对应的服务器
            serverLoadMap.put(preServer, serverLoadMap.get(preServer) - 1); //更新负载

            //当前服务器的负载小于其slot数目，若该服务在满负载集合中，则把它移除
            if(serverLoadMap.get(preServer) < serverSlotMap.get(preServer))
                if(fullServerSet.contains(preServer))
                    fullServerSet.remove(preServer);

            //将该任务对应的先前服务器的流反向
            if(edges.containsKey(task + "->" + preServer))
                edges.remove(task + "->" + preServer);
            Edge e = new Edge(preServer, task);
            edges.put(e.getPath(), e);
        }
    }

    /**
     * @param serverList : 服务器集合
     * @param serverLoadMap ：key:服务器  value:该服务器当前的负载量
     * @param allocatedTask ： key:任务  value:分配给该任务的服务器
     */
    private void updateServers(List<Server> serverList, Map<String, Integer> serverLoadMap,
                              Map<String, String> allocatedTask) {

        Map<String, Set<String>> allocated = new HashMap<>();
        for(String task : allocatedTask.keySet()){
            String serverName = allocatedTask.get(task);
            if(allocated.containsKey(serverName))
                allocated.get(serverName).add(task);
            else{
                Set<String> set = new HashSet<>();
                set.add(task);
                allocated.put(serverName, set);
            }
        }

        for(Server server : serverList){
            String serverName = server.getServerName();
            if(allocated.containsKey(serverName))
                server.setAllocatedTask(allocated.get(serverName));
            if(serverLoadMap.containsKey(serverName))
                server.setLoad(serverLoadMap.get(serverName));
        }
    }

    /**
     * 获取服务器中负载最小的服务器
     * @param serverLoadMap
     * @return
     */
    public String getMinLoadServer(Map<String,Integer> serverLoadMap, Set<String> fullServerSet) {

        int min = Integer.MAX_VALUE;
        String serverName = null;
        for(String server : serverLoadMap.keySet()) {
            //如果当前服务器的负载达到阈值，不再参与最小负载服务器的竞争
            if(fullServerSet.contains(server))
                continue;
            if(serverLoadMap.get(server) < min) {
                serverName = server;
                min = serverLoadMap.get(server);
            }
        }
        return serverName;
    }

    public static void main(String[] args){

        LoadBalancingSchedule loadBalancingSchedule = new LoadBalancingSchedule();

        List<Server> results = loadBalancingSchedule.getTaskAllocation(100,300);

    }
}
