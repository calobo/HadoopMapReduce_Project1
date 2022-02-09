import org.junit.Test;

public class TaskH$Test {
    @Test
    public void debug() throws Exception {
        String[] input = new String[2];

        input[0] = "file:///C:/Users/Chris/IdeaProjects/HadoopMapReduce_Project1/Friends.csv";
        input[1] = "file:///C:/Users/Chris/IdeaProjects/HadoopMapReduce_Project1/outputTaskH.txt";

        TaskH h = new TaskH();
        h.debug(input);
    }
}
