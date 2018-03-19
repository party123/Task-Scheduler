import java.io.*;

/**
 * Created by 3 on 2017/5/4.
 */
public class Test {

    public static void main(String[] args){

        int[][] relation = new int[20][21];
        try{
            BufferedReader br = new BufferedReader(
                    new FileReader("G:\\Intellij\\MyJavaProgram\\PaperProject_1\\src\\relation_matrix.txt"));
            String str = null;
            int taskNum = 1;
            int otherTaskNum = taskNum + 1;
            while((str = br.readLine()) != null){
                String[] values = str.split("\t");
                for(String s : values){
                    relation[taskNum][otherTaskNum] = Integer.parseInt(s);
                    System.out.print(relation[taskNum][otherTaskNum] + "(" + taskNum + "," + otherTaskNum + ")" + "\t\t");
                    otherTaskNum++;
                }
                System.out.println();
                taskNum++;
                otherTaskNum = taskNum + 1;
            }
            br.close();
        }
        catch(IOException e){
            e.printStackTrace();
        }
    }
}
