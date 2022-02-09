import org.junit.Test;

public class TaskB$Test {

    @Test
    public void debug() throws Exception {
        String[] input = new String[4];

        input[0] = "file:///C:/Users/Chris/IdeaProjects/HadoopMapReduce_Project1/AccessLogs.csv";
        input[1] = "file:///C:/Users/Chris/IdeaProjects/HadoopMapReduce_Project1/MyPage.csv";
        input[2] = "file:///C:/Users/Chris/IdeaProjects/HadoopMapReduce_Project1/outputTaskB.txt";

        TaskB b = new TaskB();
        b.debug(input);
    }
}