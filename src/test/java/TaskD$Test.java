import org.junit.Test;

public class TaskD$Test {

    @Test
    public void debug() throws Exception {
        String[] input = new String[3];

        input[0] = "file:///C:/Users/Chris/IdeaProjects/HadoopMapReduce_Project1/Friends.csv";
        input[1] = "file:///C:/Users/Chris/IdeaProjects/HadoopMapReduce_Project1/MyPage.csv";
        input[2] = "file:///C:/Users/Chris/IdeaProjects/HadoopMapReduce_Project1/outputTaskD.txt";

        TaskD d = new TaskD();
        d.debug(input);
    }
}