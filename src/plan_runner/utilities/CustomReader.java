package plan_runner.utilities;

import java.io.IOException;


public interface CustomReader {

    public String readLine()throws IOException;
    public void close();
}
