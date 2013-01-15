package com.dajie.core.dbresource;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * @author yong.li@dajie-inc.com
 *
 */
public class TestPattern extends TestCase {

	
	private static final Pattern pattern1 = Pattern.compile("[a-zA-Z]+_[0-4][0-9]");
	private static final Pattern pattern2 = Pattern.compile("\\w*_[5-9][0-9]$");
	
	@Test
	public void testPatternString() {
		Assert.assertNotNull(pattern1);
		Assert.assertNotNull(pattern2);
		Matcher m = pattern1.matcher("fff_ab");
		System.out.println(m.matches());
		m = pattern1.matcher("user_79");
		System.out.println(m.matches());
		m = pattern1.matcher("user_29");
		System.out.println(m.matches());
		
	}

}
