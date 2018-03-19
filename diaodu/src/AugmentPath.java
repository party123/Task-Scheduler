import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 增广路径操作类
 */
public class AugmentPath
{
    public static List<List<Edge>> flows = new ArrayList<>();
    /**
     * 判断从结点 start 到结点 end 是否具有一条增广路径,并将增广路径涉及到的路径放入到 list 集合中
     * @param start：起点
     * @param end：终点
     * @return
     */
    public static boolean hasAugmentPath(String start, String end, List<Edge> path, Map<String, Edge> edges)
    {
        //获取所有以结点 start 为起点的边的集合
        List<Edge> nextEdgeList = nextEdge(start, edges);
        for(Edge edge : nextEdgeList) {

            if(path.contains(edge)) //出现回路
                continue;

            path.add(edge);
            //当前 edge 的终点和给定的终点相同，表明找到了一条增广路径
            if(edge.getEnd().equals(end)){
                return true;
            }
            else {
                //深度递归
                if(hasAugmentPath(edge.getEnd(), end, path, edges)) //利用当前 edge 的终点作为起点递归查找增广路径
                    return true;
                else {
                    path.remove(edge);
                    continue;
                }
            }
        }
        return false;
    }

    public static boolean getAllAugmentPath(String start, String end, List<Edge> path, Map<String, Edge> edges){

        //获取所有以结点 start 为起点的边的集合
        List<Edge> nextEdgeList = nextEdge(start, edges);
        for(Edge edge : nextEdgeList) {
            if(path.contains(edge))
                continue;
            path.add(edge);

            //当前 edge 的终点和给定的终点相同，表明找到了一条增广路径
            if(edge.getEnd().equals(end)){
                //将当前所有可能的增广路径全加入到flows中
                flows.add(new ArrayList<>(path));
                path.remove(edge);
                return false;
            }
            else {
                //深度递归
                if(getAllAugmentPath(edge.getEnd(), end, path, edges)) //利用当前 edge 的终点作为起点递归查找增广路径
                    return true;
                else {
                    path.remove(edge);
                    continue;
                }
            }
        }
        return false;
    }

    /**
     * 获取以 start 为起点的边的集合 如以"S"为起点的 nextEdge 为 {"S -> V2", "S -> V1"}
     * @param start
     * @return
     */
    public static List<Edge> nextEdge(String start, Map<String, Edge> edges)
    {
        List<Edge> nextEdgeList = new ArrayList<>();
        for(String path : edges.keySet()) {
            Edge edge = edges.get(path);
            if(edge.getStart().equals(start))
                nextEdgeList.add(edges.get(path));
        }
        return nextEdgeList;
    }
}
