import java.util.ArrayList;
import java.util.List;

/**
 * 覆盖了equals和hashCode方法
 * Edge("start","end",0,0,0)和Edge("end","start",0,0,0)两个对象是相等的
 */
public class Edge
{
    private String start;//起始点
    private String end;//终结点

    public Edge(String start,String end)
    {
        this.start=start;
        this.end=end;
    }

    public String getStart()
    {
        return start;
    }

    public void setStart(String start)
    {
        this.start = start;
    }

    public String getEnd()
    {
        return end;
    }

    //获取路径
    public String getPath()
    {
        return  start + "->"  + end;
    }

    public String getReversePath()
    {
        return end + "->" + start;
    }

    @Override
    public String toString()
    {
        return start + "->" + end;
    }

    @Override
    public int hashCode()
    {
        int result = 1;
        result += ((end == null) ? 0 : end.hashCode());
        result += ((start == null) ? 0 : start.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Edge other = (Edge) obj;

        if(getStart().endsWith(other.getStart()) && getEnd().equals(other.getEnd()))
        {
            return true;
        }

        return false;
    }

    public static void main(String[] args)
    {
        List<Edge> list = new ArrayList<>();
        list.add(new Edge("t2","T"));
        Edge edge = new Edge("t2","T");
        if(list.contains(edge))
            System.out.println(true);
    }
}
