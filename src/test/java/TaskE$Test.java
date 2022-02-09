import org.junit.Test;

public class TaskE$Test {

    @Test
    public void debug() throws Exception {
        String[] input = new String[3];

        input[0] = "file:///C:/Users/Chris/IdeaProjects/HadoopMapReduce_Project1/AccessLogs.csv";
        input[1] = "file:///C:/Users/Chris/IdeaProjects/HadoopMapReduce_Project1/outputTaskE.txt";

        TaskE e = new TaskE();
        e.debug(input);
    }
}