import java.util.*;

public class Server {

    //服务器名称
    private String serverName;
    //服务器负载量
    private int load = 0;
    //分配给该服务器的任务
    private List<String> allocatedTask = new ArrayList<>();
    //每个server最多可以同时运行的任务数
    private int slot = 0;

    public Server(String serverName, int load) {
        this.serverName = serverName;
        this.load = load;
        this.slot = new Random().nextInt(8) + 1;//假设最多运行 6 个任务
    }

    public String getServerName() {
        return serverName;
    }

    public int getLoad() {
        return load;
    }

    public void setLoad(int load) {
        this.load = load;
    }

    public List<String> getAllocatedTask() {
        return allocatedTask;
    }

    public void setAllocatedTask(Set<String> allocatedTask) {
        this.allocatedTask = new ArrayList<>(allocatedTask);
    }

    public int getSlot() {
        return slot;
    }

    public void setSlot(int slot) {
        this.slot = slot;
    }

    public void addTask(String task){
        allocatedTask.add(task);
    }

    @Override
    public String toString() {
        return "Server{" +
                "serverName='" + serverName + '\'' +
                ", slot='" + slot + '\'' +
                ", allocatedTask="+ allocatedTask +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Server server = (Server) o;

        if (Float.compare(server.load, load) != 0) return false;
        return serverName != null ? serverName.equals(server.serverName) : server.serverName == null;
    }

    @Override
    public int hashCode()
    {
        int result = serverName != null ? serverName.hashCode() : 0;
        result = 31 * result + (load != +0.0f ? Float.floatToIntBits(load) : 0);
        return result;
    }
}
