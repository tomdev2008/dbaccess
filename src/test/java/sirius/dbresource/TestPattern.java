package sirius.dbresource;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * @author liyong19861014@gmail.com
 * 
 */
public class TestPattern extends TestCase {

    private static final Pattern pattern1 = Pattern.compile("[a-zA-Z]+_([0-9]|1[0-9]|2[0-4])");

    private static final Pattern pattern2 = Pattern.compile("\\w*_[5-9][0-9]$");

    @Test
    public void testPatternString() {
        Assert.assertNotNull(pattern1);
        Assert.assertNotNull(pattern2);
        for (int i = 0; i < 100; ++i) {
            String tableName = "PERSON_" + i;
            Matcher m = pattern1.matcher(tableName);
            //			System.out.println(tableName + "\t" + m.matches() + "\t" + pattern1.pattern());
        }

    }

    @Test
    public void testString() {
        Pattern p = Pattern.compile("[a-zA-Z_-]+_[0-9]");
        Matcher m = p.matcher("aaa-bbb_9");
        System.out.println(m.matches());
    }
}
