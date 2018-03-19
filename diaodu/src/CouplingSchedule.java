import java.util.*;

/**
 *  紧耦合调度策略：对于服务器来说，每个服务器有执行任务的上限。为了保证有关系的任务可以分配到同一个
 *  服务器上，因此采用每次选取可以执行最大任务量的服务器来进行分配任务，直到该服务器达到最大负载时再去
 *  选择另一个服务器。每个服务器的最大负载的判定规则是 min{server.slot, 与该服务器相关联的任务数目}
 */
public class CouplingSchedule
{
    //保存当前经过的路径，即流
    private List<Edge> path = new ArrayList<>();

    //key:服务器 value:服务器最多可以执行的任务数
    private Map<String,Integer> serverAbility = new HashMap<>();

    //key:服务器名  value:该服务器的 slot 数目
    private Map<String,Integer> serverSlot = new HashMap<>();

    //key:任务  value:为该任务分配的服务器
    private Map<String, String> allocatedTask = new HashMap<>();

    /**
     * 对任务进行初始分配，保证本地性
     * @return
     */
    public List<Server> getTaskAllocation(DataSource ds)
    {
        List<Server> servers = ds.getServers();

        //key:服务器  value:当前服务器所引用到的任务数目
        Map<String, Set<String>> preferedTaskMap = ds.getPreferedTasks();

        //key:路径  value:该路径对应的边对象
        Map<String, Edge> edges = ds.getEdges();

        //此次需要分配的任务集合
        Set<String> taskSet = ds.getTaskSet();

        int totalSlot = 0;
        for(Server s : servers){
            //取服务器可用的 slot 数目与其引用的任务数目的最小值作为当前服务器的最大执行能力
            int ability = Math.min(preferedTaskMap.get(s.getServerName()).size(), s.getSlot());
            serverAbility.put(s.getServerName(), ability);
            serverSlot.put(s.getServerName(), s.getSlot());
            totalSlot += s.getSlot();
        }
        System.out.println("totalSlot : " + totalSlot);

        //获取最大执行能力的服务器
        String maxAbilityServer = getMaxAbilityServer(serverAbility);
        while(AugmentPath.hasAugmentPath(maxAbilityServer,"T", path, edges)){

            //每次先把最大负载服务器分配完成
            if(serverAbility.get(maxAbilityServer) > 0){

                for(Edge edge : path){

                    if(edge.getStart().startsWith("s")){
                        String server = edge.getStart();
                        String task = edge.getEnd();

                        //当前任务已经被分给其他服务器
                        if(allocatedTask.containsKey(task)){

                            //获取先前分配给当前待处理任务的服务器
                            String preServer = allocatedTask.get(task);

                            //将该任务对应的先前服务器的流反向
                            if(edges.containsKey(task + "->" + preServer))
                                edges.remove(task + "->" + preServer);
                            Edge e = new Edge(preServer, task);
                            edges.put(e.getPath(), e);
                        }

                        //将其他涉及到此任务的服务器进行更新，更新与此任务相关的服务器的引用任务集合。如将t1分配给了s1后：
                        //若服务器s2和s3均引用了t1，此时将t1从s2和s3的引用服务器集合中移除并更新s2和s3的最大执行能力。
                        updateGraph(task, server, serverAbility, serverSlot, preferedTaskMap);

                        //任务分配
                        allocatedTask.put(task,server);

                        //将此任务从待分配任务集合中移除
                        if(taskSet.contains(task))
                            taskSet.remove(task);

                        //更新当前服务器的最大执行能力
                        if(serverAbility.get(server) > 0)
                            serverAbility.put(server,serverAbility.get(server) - 1);
                    }
                    //从原有边信息中移除 path 中当前处理的边信息
                    String key = edge.getPath();
                    if(edges.containsKey(key))
                        edges.remove(key);

                    //加入当前处理边的的反向边，权值与当前处理边的权值一致
                    Edge reverseEdge = new Edge(edge.getEnd(), edge.getStart());
                    edges.put(reverseEdge.getPath(), reverseEdge);

                    //重新获取最大执行能力的服务器
                    if(serverAbility.get(maxAbilityServer) == 0){
                        maxAbilityServer = getMaxAbilityServer(serverAbility);
                    }
                }
            }
            if(serverAbility.get(maxAbilityServer) == 0)
                break;
            path.clear();
        }
        //将最终结果写会服务器
        updateServers(servers, allocatedTask);

        //如果任务没有全部被分配，此时需要进行二次分配
        if(taskSet.size() > 0)
            reAllocate(servers, taskSet);

        return servers;
    }

    /**
     * 用于获取最大执行能力的服务器
     * @param abilityMap：key->服务器名   value->服务器最多可以执行的任务数
     * @return
     */
    public String getMaxAbilityServer(Map<String,Integer> abilityMap)
    {
        int maxNum = Integer.MIN_VALUE;
        String maxServer = null;
        for(String server : abilityMap.keySet()){
            if(abilityMap.get(server) > maxNum){
                maxNum = abilityMap.get(server);
                maxServer = server;
            }
        }
        return maxServer;
    }

    /**
     * @param serverList : 服务器集合
     * @param allocatedTask ： key:任务  value:分配给该任务的服务器
     */
    private void updateServers(List<Server> serverList, Map<String, String> allocatedTask) {

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
        }
    }

    /**
     * 当前任务分配后，需要更新服务器的最大执行任务量
     * @param task ： 当前待处理的任务
     * @param server ：当前预分配给当前task的服务器
     * @param serverAbilityMap ：key:服务器  value:服务器对应的最大执行能力
     * @param serverSlotMap ：key:服务器  value:服务器对应的slot数目
     * @param preferedTaskMap ：key:服务器  value:服务器引用的本地任务列表
     */
    private void updateGraph(String task,  String server, Map<String, Integer> serverAbilityMap,
                             Map<String,Integer> serverSlotMap, Map<String, Set<String>> preferedTaskMap){

        for(String s : preferedTaskMap.keySet()){
            if(s.equals(server))
                continue;

            //如果当前服务器的本地任务列表包含需要处理的任务，则从服务器的本地任务列表中移除该任务
            if(preferedTaskMap.get(s).contains(task)){

                preferedTaskMap.get(s).remove(task);
                //将任务从服务器的本地任务列表删除后，可能会导致当前服务器的最大执行能力发生变化，因此需要进行判断并更新
                int slot = serverSlotMap.get(s);
                int preferedTaskCount = preferedTaskMap.get(s).size();
                int minNum = slot > preferedTaskCount ? preferedTaskCount : slot;
                int ability = serverAbilityMap.get(s) > minNum ? minNum : serverAbilityMap.get(s);
                serverAbilityMap.put(s, ability);
            }
        }
    }

    /**
     * 按照上述规则进行分配后，对于有些任务可能会造成无法被分配，此时需要对这些任务进行重新的分配，分配的原则为每次选择剩余 slot
     * 数目最多的服务器进行分配
     * @param servers : 服务器集合
     * @param unAllocateTask : 未分配的任务
     */
    private void reAllocate(List<Server> servers, Set<String> unAllocateTask){

        for(String task : unAllocateTask){
            Server server = getMaxEmptyExcutorServer(servers);
            server.addTask(task);
        }
    }

    public Server getMaxEmptyExcutorServer(List<Server> servers){

        int maxExecutor = 0;
        Server s = null;
        for(Server server : servers){
            int slot = server.getSlot();
            int allocatedTaskCount = server.getAllocatedTask().size();
            if(maxExecutor < slot - allocatedTaskCount){
                maxExecutor = slot - allocatedTaskCount;
                s = server;
            }
        }
        return s;
    }

    public static void main(String[] args){

        CouplingSchedule couplingSchedule = new CouplingSchedule();
        DataSource dataSource = new DataSource(300,1000);
        List<Server> servers = couplingSchedule.getTaskAllocation(dataSource);
        int experimentData = 0;
        for(Server server : servers){
            experimentData += Math.pow(server.getAllocatedTask().size() - 3, 2);
        }
        System.out.println(experimentData);
    }
}
