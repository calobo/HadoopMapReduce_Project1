import org.junit.Test;

public class TaskC$Test {

    @Test
    public void debug() throws Exception {
        String[] input = new String[2];
        input[0] = "file:///C:/Users/Chris/IdeaProjects/HadoopMapReduce_Project1/MyPage.csv";
        input[1] = "file:///C:/Users/Chris/IdeaProjects/HadoopMapReduce_Project1/outputTaskC.txt";

        TaskC c = new TaskC();
        c.debug(input);
    }
}