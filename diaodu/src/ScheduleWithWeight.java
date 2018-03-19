import java.util.*;

/**
 * 此分配方案考虑分配一个任务给服务器时考虑任务之间的联系，也即关联程度。首先进行一个初始分配，也即采用
 * CouplingSchedule分配方案来得到初始时的每个服务器中的分配任务详情，然后从初始分配后的服务器中随机选择
 * 一个任务固定在该服务器上（当服务器的分配的任务数大于1时，等于1就去那唯一的一个）作为核（Kernal）。
 * 然后再次采用CouplingSchedule分配准则来进行分配任务，只是这次分配每个任务时需要考虑当前分配的任务与固定
 * 在该服务器上核任务的紧密程度。每次选取与当前核任务关联度最大的任务
 */
public class ScheduleWithWeight {

    private CouplingSchedule couplingSchedule = new CouplingSchedule();

    //图信息
    private Map<String, Edge> edges = null;

    //保存当前经过的路径，即流
    private List<Edge> path = new ArrayList<>();

    //Key:服务器名  Value:当前任务中存放数据所对应的任务集合列表
    private Map<String,Set<String>> preferedTasks = new HashMap<>();

    //key:服务器 value:服务器最多可以执行的任务数
    private Map<String,Integer> serverAbility = new HashMap<>();

    //key:服务器名  value:该服务器的 slot 数目
    private Map<String,Integer> serverSlot = new HashMap<>();

    //key:服务器名  value:该服务器对应的核任务
    private Map<String, String> kernelTasks = new HashMap<>();

    //key:任务  value:为该任务分配的服务器
    private Map<String, String> allocatedTask = new HashMap<>();

    //保存走过的流
    private List<List<Edge>> flows = new ArrayList<>();

    public static void main(String[] args){

        ScheduleWithWeight sww = new ScheduleWithWeight();
        System.out.println(sww.getTaskAllocation(6,12));
    }

    public List<Server> getTaskAllocation(int serverCount, int taskCount){

        DataSource dataSource = new DataSource(serverCount,taskCount);
        //1、核任务的初始分配
        //2、待分配的任务集合
        //3、任务之间的关联度
        List<Server> servers = getInitialAllocation(dataSource);
        Set<String> taskSet = dataSource.getTaskSet();
        int[][] relationValue = dataSource.getRelationValue();

        //1、初始化serverAblity, serverSlot, kernalTasks。更新taskSet(移除核任务)
        //2、取服务器可用的 slot 数目与其引用的任务数目的最小值作为当前服务器的最大执行能力
        //3、从待分配的任务集合中移除已分配的核任务
        for(Server s : servers){
            int ability = Math.min(preferedTasks.get(s.getServerName()).size(), s.getSlot());
            serverAbility.put(s.getServerName(), ability);
            serverSlot.put(s.getServerName(), s.getSlot());
            if(s.getAllocatedTask().size() > 0){
                String kernalTask = s.getAllocatedTask().get(0);
                kernelTasks.put(s.getServerName(), kernalTask);
                if(taskSet.contains(kernalTask))
                    taskSet.remove(kernalTask);
            }
        }

        //获取最大执行能力服务器和从此服务器出发的所有增广路径集合flows
        String maxAbilityServer = getMaxAbilityServer(serverAbility);
        AugmentPath.getAllAugmentPath(maxAbilityServer,"T", path, edges);

        while(AugmentPath.flows.size() > 0){

            //对所有流按照节省的费用值进行排序,每次选择节省费用最大的流
            sortFlowByFee(maxAbilityServer, relationValue, AugmentPath.flows);
            while(AugmentPath.flows.size() > 0 && serverAbility.get(maxAbilityServer) > 0){
                //1、取出当前 maxAbilityServer 对应的节省最大费用的流
                //2、利用标记 isDuplicated 判断是否是重复处理的任务
                //3、利用 recordTasks 来记录当前流中分配给其他服务器的任务
                int index = AugmentPath.flows.size() - 1;
                List<Edge> maxValueFlow = AugmentPath.flows.get(index);
                boolean isExist = true;
                List<String> recordTasks = new ArrayList<>();

                //判断当前选择的流在更新的图信息中确实存在，若当前流中的某条边不存在，此时移除当前流
                if(maxValueFlow.size() > 0){
                    for(Edge edge : maxValueFlow){
                        if(!edges.containsKey(edge.getPath())){
                            isExist = false;
                            break;
                        }
                        //判断当前流中的任务是否已经被分配，若被分配给其他的服务器，将这类任务加入到 recordTasks 集合中。
                        if (edge.getStart().startsWith("s")) {
                            String task = edge.getEnd();
                            if (allocatedTask.containsKey(task))
                                recordTasks.add(task);
                        }
                    }
                }
                if(!isExist){
                    AugmentPath.flows.remove(index);
                    continue;
                }

                //存在任务在先前已经被分配，此时需要考虑是否选择这条流
                //1、计算当前流中已经被分配的任务所节省下来的费用总和 preValue 和当前流所节省下来的费用总和 curValue
                //2、若curValue <= preValue 则说明当前流并不会相比于先前流能够节省更多的费用，抛弃当前流
                //3、若curValue > preValue 此时按照当前流来进行分配任务，并且对那些已经分配的任务流进行回撤
                //   分配完任务需要更新相关变量（taskSet,allocatedTasks,serverAbility,edges）以及从待处理流中移除当前流
                if(recordTasks.size() > 0) {
                    //计算出如果按照当前流来进行分配任务的话，在先前已经被分配的任务进行回撤时损失的开销
                    int preValue = 0;
                    for(String recordTask : recordTasks){
                        //1、该任务分配给的服务器具有核任务时，节省的开销为核任务与该任务之间的通信开销。
                        //2、该任务分配给的服务器不具有核任务时，节省的通信开销记为 0
                        String server = allocatedTask.get(recordTask);
                        if(kernelTasks.containsKey(server)){
                            String kernalTask = kernelTasks.get(server);
                            int i = Integer.parseInt(kernalTask.substring(1,kernalTask.length()));
                            int j = Integer.parseInt(recordTask.substring(1,recordTask.length()));
                            if(i > j)
                                preValue += relationValue[j][i];
                            else
                                preValue += relationValue[i][j];
                        }
                        else
                            continue;
                    }
                    int curValue = getSingleFlowFValue(maxAbilityServer, relationValue, maxValueFlow);
                    if(curValue > preValue){
                        for(Edge edge : maxValueFlow){
                            if(edge.getStart().startsWith("s")){
                                String server = edge.getStart();
                                String task = edge.getEnd();
                                if(allocatedTask.containsKey(task)){
                                    String preServer = allocatedTask.get(task);
                                    if(edges.containsKey(task + "->" + preServer))
                                        edges.remove(task + "->" + preServer);
                                    Edge e = new Edge(preServer, task);
                                    edges.put(e.getPath(), e);
                                    serverAbility.put(preServer, serverAbility.get(preServer)+1);
                                }
                                updateGraph(task, server, serverAbility, serverSlot, preferedTasks);
                                allocatedTask.put(task,server);
                                if(taskSet.contains(task))
                                    taskSet.remove(task);
                                if(serverAbility.get(server) > 0)
                                    serverAbility.put(server,serverAbility.get(server) - 1);
                            }
                            String key = edge.getPath();
                            if(edges.containsKey(key))
                                edges.remove(key);
                            Edge reverseEdge = new Edge(edge.getEnd(), edge.getStart());
                            edges.put(reverseEdge.getPath(), reverseEdge);
                        }
                        flows.add(new ArrayList<>(maxValueFlow));
                        AugmentPath.flows.remove(index);
                    }
                    else
                        AugmentPath.flows.remove(index);
                }

                //当前流中的所有任务均在之前没有被分配并且任务不重复的情况下（对于maxAbilityServer而言），此时直接按照流进行分配操作
                else if(recordTasks.size() == 0) {
                    for(Edge edge : maxValueFlow) {
                        if(edge.getStart().startsWith("s")) {
                            String server = edge.getStart();
                            String task = edge.getEnd();
                            updateGraph(task, server, serverAbility, serverSlot, preferedTasks);
                            allocatedTask.put(task, server);
                            if(taskSet.contains(task))
                                taskSet.remove(task);
                            if(serverAbility.get(server) > 0)
                                serverAbility.put(server,serverAbility.get(server) - 1);
                        }
                        String key = edge.getPath();
                        if(edges.containsKey(key))
                            edges.remove(key);
                        Edge reverseEdge = new Edge(edge.getEnd(), edge.getStart());
                        edges.put(reverseEdge.getPath(), reverseEdge);
                    }
                    flows.add(new ArrayList<>(maxValueFlow));
                    AugmentPath.flows.remove(index);
                }
            }
            path.clear();
            AugmentPath.flows.clear();

            //为了防止由于增加了限制条件而使得某些服务器始终为最大执行能力的服务器，因此若第二次选取的最大能力服务器
            //和第一次是同一个服务器，此时需要暂时把这个服务器的执行能力置0，再重新选取，选取后再进行恢复。
            String preMaxAbilityServer = maxAbilityServer;
            maxAbilityServer = getMaxAbilityServer(serverAbility);
            if(maxAbilityServer.equals(preMaxAbilityServer)){
                int abilty = serverAbility.get(maxAbilityServer);
                serverAbility.put(maxAbilityServer, 0);
                maxAbilityServer = getMaxAbilityServer(serverAbility);
                serverAbility.put(preMaxAbilityServer, abilty);
            }

            if(serverAbility.get(maxAbilityServer) == 0 || taskSet.size() == 0){
                updateServers(servers, allocatedTask);
                //任务没有被分配完，此时采用遍历法，寻找与服务器上面的kernalTask关联度最大的作为分配的服务器
                if(taskSet.size() != 0){
                    reAllocate(servers, taskSet, kernelTasks, relationValue);
                }
                return servers;
            }
            //重新获取新的最大执行能力服务器对应的增广路径
            AugmentPath.getAllAugmentPath(maxAbilityServer,"T", path, edges);
        }
        return servers;
    }

    /**
     * 对剩下未分配的任务再重新进行分配,分配规则为选取当前
     * @param taskSet ：未分配的任务集合
     * @param kernalTasks ：核任务集合
     */
    public void reAllocate(List<Server> servers, Set<String> taskSet,
                           Map<String, String> kernalTasks, int[][] relationValue ){

        Iterator<String> iterator = taskSet.iterator();
        while (iterator.hasNext()){
            String unAllocatedTask = iterator.next();
            int maxValue = -1;
            String selectedServer = null;
            List<String> allocatedTask = null;

            for(Server server : servers) {
                allocatedTask = server.getAllocatedTask();
                if(allocatedTask != null) {
                    int slot = server.getSlot();
                    int taskCount = server.getAllocatedTask().size();

                    //由于在加上核任务时，已经将服务器的slot数目减1，因此这里核任务不考虑在所有已分配的任务之内
                    if(kernalTasks.containsKey(server.getServerName()))
                        taskCount -= 1;

                    //当前服务器还有空闲slot可用
                    if(slot > taskCount){
                        //若当前服务器不存在核任务，则从其分配的任务中随机选取一个任务暂时作为其核任务用来参与后续计算
                        String kernalTask = null;
                        if(kernalTasks.containsKey(server.getServerName()))
                            kernalTask = kernalTasks.get(server.getServerName());
                        else if(server.getAllocatedTask().size() > 0) {
                            int randomIndex = new Random().nextInt(taskCount);
                            kernalTask = server.getAllocatedTask().get(randomIndex);
                        }

                        if(kernalTask != null) {
                            //计算待分配任务放在所有不同服务器上所带来的收益，取最大者。
                            int i = Integer.parseInt(kernalTask.substring(1, kernalTask.length()));
                            int j = Integer.parseInt(unAllocatedTask.substring(1, unAllocatedTask.length()));
                            int tempValue = 0;
                            if(i > j)
                                tempValue = relationValue[j][i];
                            else
                                tempValue = relationValue[i][j];
                            if(tempValue > maxValue){
                                maxValue = tempValue;
                                selectedServer = server.getServerName();
                            }
                        }
                    }
                }
            }
            if(selectedServer != null && allocatedTask != null){
                allocatedTask.add(unAllocatedTask);
                iterator.remove();
            }
        }
        //说明服务器集合中存在着若干服务器不具有核任务并且也没有被分配到任务,此时随机选择空闲服务器进行分配
        if(taskSet.size() > 0) {
            iterator = taskSet.iterator();
            while(iterator.hasNext()) {
                String task = iterator.next();
                for(Server server : servers) {
                    if(!kernalTasks.containsKey(server.getServerName()) && server.getAllocatedTask().size() < server.getSlot()) {
                        server.getAllocatedTask().add(task);
                        iterator.remove();
                    }
                }
            }
        }
    }

    /**
     * 得到CouplingSchedule分配的初始分配情形，并随机选取一个任务作为该服务器的核（Kernal）
     * 固定一个核任务后，需要更新Server的slot数目
     * @return
     */
    public List<Server> getInitialAllocation(DataSource dataSource){

        //初始的任务分配结果
        List<Server> servers = couplingSchedule.getTaskAllocation(dataSource);

        System.out.println("未选定核任务对应初始分配情况：" + servers);
        System.out.println();

        //对于每个服务器，若当前服务器分配的任务数大于0时，从中随机选取一个任务作为其核任务
        for(Server server : servers){
            if(server.getAllocatedTask().size() >= 1){
                int index = new Random().nextInt(server.getAllocatedTask().size());
                String kernalTask = server.getAllocatedTask().get(index);
                server.getAllocatedTask().clear();
                server.getAllocatedTask().add(kernalTask);
                //由于固定一个kernalTask在服务器上，因此该服务器的slot数目应该减1
                server.setSlot(server.getSlot() - 1);
            }
        }
        updateGraph(dataSource, servers);
        System.out.println("选定核任务对应的分配情况: " + servers);
        System.out.println();
        System.out.println();
        return servers;
    }

    /**
     * 由于为每个任务随机分配一个核任务，并且核任务固定在该服务器上，此时需要从图中移除有关核任务
     * 的边信息，并且更新每个服务器的本地任务集合以及该服务器对应的Slot数目。
     * @param dataSource
     * @param servers
     * @return
     */
    public void updateGraph(DataSource dataSource, List<Server> servers){

        edges = dataSource.getEdges();
        preferedTasks = dataSource.getPreferedTasks();

        System.out.println("未分配核任务对应的服务器本地任务集合：" + preferedTasks);
        System.out.println();

        //从图中移除所有有关kernalTask的边
        //从服务器的本地任务列表中移除kernalTask
        for(Server server : servers){
            if(server.getAllocatedTask().size() > 0){
                String kernalTask = server.getAllocatedTask().get(0);
                Iterator<String> keyIterator = edges.keySet().iterator();
                while(keyIterator.hasNext()){
                    String path = keyIterator.next();
                    if(path.contains(kernalTask))
                        keyIterator.remove();
                }
                for(String s : preferedTasks.keySet()){
                    if(preferedTasks.get(s).contains(kernalTask))
                        preferedTasks.get(s).remove(kernalTask);
                }
            }
        }
        System.out.println("分配核任务后对应的服务器本地任务集合：" + preferedTasks);
        System.out.println();
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
     * 更新服务器中的信息
     * @param servers
     * @param allocatedTask
     */
    public void updateServers(List<Server> servers, Map<String, String> allocatedTask) {
        for(String task : allocatedTask.keySet()) {
            String server = allocatedTask.get(task);
            for(Server server1 : servers){
                if(server1.getServerName().equals(server)) {
                    server1.addTask(task);
                    break;
                }
            }
        }
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
     * 对当前server经过的所有流按照费用进行一个排序
     * @param server
     * @param relationValue
     * @param flows
     */
    public void sortFlowByFee(String server, int[][] relationValue, List<List<Edge>> flows){

        Collections.sort(flows, new Comparator<List<Edge>>() {
            @Override
            public int compare(List<Edge> flow1, List<Edge> flow2) {
                int flow1_Value = getSingleFlowFValue(server, relationValue, flow1);
                int flow2_value = getSingleFlowFValue(server, relationValue, flow2);
                return flow1_Value - flow2_value;
            }
        });
    }

    /**
     * 获取server到终点的一条流所对应的费用
     * @param server
     * @param relationValue
     * @param flow
     * @return
     */
    public int getSingleFlowFValue(String server, int[][] relationValue, List<Edge> flow){

        int totalValue = 0;
        String kernalTask = null;
        for(Edge edge : flow){
            if(edge.getStart().startsWith("s")){
                String s = edge.getStart();
                String t = edge.getEnd();

                //当前流只包含两条边，并且当前流中的服务器没有核任务，直接返回本地任务的代价====1
                if(flow.size() == 2){
                    if(!kernelTasks.containsKey(s))
                        return 1;
                }

                //如果当前服务器没有核任务，将第一个分配给该服务器的任务当做核任务来处理
                if(!kernelTasks.containsKey(s)){
                    kernalTask = t;
                }
                else
                    kernalTask = kernelTasks.get(s);
                int i = Integer.parseInt(kernalTask.substring(1,kernalTask.length()));
                int j = Integer.parseInt(t.substring(1,t.length()));
                if(i > j)
                    totalValue += relationValue[j][i];
                else
                    totalValue += relationValue[i][j];
            }
        }
        return totalValue;
    }
}
