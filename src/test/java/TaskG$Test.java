import org.junit.Test;

public class TaskG$Test {

    @Test
    public void debug() throws Exception {
        String[] input = new String[3];

        input[0] = "file:///C:/Users/Chris/IdeaProjects/HadoopMapReduce_Project1/AccessLogs.csv";
        input[1] = "file:///C:/Users/Chris/IdeaProjects/HadoopMapReduce_Project1/outputTaskG.txt";

        TaskG g = new TaskG();
        g.debug(input);
    }
}