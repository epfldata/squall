package plan_runner.utilities;

import java.io.IOException;

public interface CustomReader {

	public void close();

	public String readLine() throws IOException;
}
