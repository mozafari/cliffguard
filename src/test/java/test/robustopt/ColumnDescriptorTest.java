package test.robustopt;

/**
 * Created by zhxchen on 3/15/16.
 */

import org.junit.Assert;
import org.junit.Test;
import edu.umich.robustopt.staticanalysis.ColumnDescriptor;

import java.util.HashSet;
import java.util.Set;

public class ColumnDescriptorTest {
    @Test
    public void testColumnDescriptor() {
        ColumnDescriptor tmp = new ColumnDescriptor("a", "b", new String("c"));
        Set<ColumnDescriptor> set0 = new HashSet<ColumnDescriptor>();
        Set<ColumnDescriptor> set1 = new HashSet<ColumnDescriptor>();
        set0.add(tmp);
        set1.add(new ColumnDescriptor("c", "b", "a"));
        set1.add(new ColumnDescriptor("c", "b", "a"));
        Assert.assertTrue(set0.contains(new ColumnDescriptor(new String("a"), "b", "c")));
        Assert.assertFalse(set1.contains(tmp));
    }
}
