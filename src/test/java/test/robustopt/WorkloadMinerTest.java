package test.robustopt;

import org.apache.commons.io.FileUtils;
import edu.umich.robustopt.experiments.WorkloadMiner;
import java.io.File;

/**
 * Created by sorrow17 on 2016/3/19.
 */
public class WorkloadMinerTest {

    public void testWorkloadMiner() throws Exception {
        String[] args = {
                "src/test/resources/ParserTest/basic0_schema.txt",
                "src/test/resources/MinerTest/sample_query.txt",
                "target/MinerTest"
        };
        try {
            WorkloadMiner.main(args);
        }
        catch (Exception e) {
            System.out.println(e);
            System.out.print("WorkloadMiner test failed.");
            throw e;
        }
        finally {
            FileUtils.forceDeleteOnExit(new File(args[2]));
        }
    }
}
