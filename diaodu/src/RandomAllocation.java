import java.util.*;

/**
 * 随机分配
 */
public class RandomAllocation {

    //记录有多少任务分配后对于分配的服务器来说属于本地任务
    private int localTaskCount = 0;

    public int getLocalTaskCount(){
        return localTaskCount;
    }

    public static void main(String[] args){
        RandomAllocation rm = new RandomAllocation();
        List<Server> servers = rm.getAllocation(300,1000);
        //System.out.println(servers);
        System.out.println(rm.getLocalTaskCount());
    }

    public List<Server> getAllocation(int serverCount, int taskCount){

        DataSource ds = new DataSource(serverCount, taskCount);

        System.out.println(ds.getServers().size());
        List<Server> servers = new ArrayList<>(ds.getServers());

        //key:服务器名  value:该服务器的本地任务集合
        Map<String,Set<String>> preferedTasks = ds.getPreferedTasks();
        //System.out.println(preferedTasks);

        //待分配任务集合
        List<String> taskList = new ArrayList<>(ds.getTaskSet());

        int index = 0;
        int totalServerNum = servers.size();

        Random random = new Random();
        while(index < taskList.size()){
            String task = taskList.get(index);

            //从服务器集合中随机选取一个服务器
            int randomServerIndex = random.nextInt(totalServerNum);
            Server server = servers.get(randomServerIndex);

            //服务器具有空闲的slot数目
            if(server.getSlot() > 0){
                server.addTask(task);
                server.setSlot(server.getSlot() - 1);
                //当前分配的任务在服务器的本地任务集合中，更新totalLocalTask
                if(preferedTasks.containsKey(server.getServerName())){
                    if(preferedTasks.get(server.getServerName()).contains(task))
                        localTaskCount++;
                }
                index++;
            }
        }
        return servers;
    }
}
