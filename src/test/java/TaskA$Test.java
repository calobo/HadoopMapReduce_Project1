import org.junit.Test;

import static org.junit.Assert.*;

public class TaskA$Test {

    @Test
    public void debug() throws Exception {
        String[] input = new String[2];

        input[0] = "file:///C:/Users/Chris/IdeaProjects/HadoopMapReduce_Project1/MyPage.csv";
        input[1] = "file:///C:/Users/Chris/IdeaProjects/HadoopMapReduce_Project1/outputTaskA.txt";

        TaskA a = new TaskA();
        a.debug(input);
    }
}