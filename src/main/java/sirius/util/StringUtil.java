package sirius.util;


/**
 * 一些小工具而已
 * @author michael
 * @email liyong19861014@gmail.com
 */
public class StringUtil {

    /**
     * MD5 32 bit hex string
     * 
     * @param md5
     * @return
     */
    public static String MD5(String md5) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
            byte[] array = md.digest(md5.getBytes());
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < array.length; ++i) {
                sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100).substring(1, 3));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            // do nothing
        }
        return null;
    }

    private static final String PWD_SUFFIX = "6379";

    public static String getRedisPassword(String input) {
        String key = input + PWD_SUFFIX;
        String md5 = MD5(key);
        if (md5.length() != 32) {
            return null;
        }
        StringBuffer sb = new StringBuffer();
        sb.append(md5.charAt(5));
        sb.append(md5.charAt(2));
        sb.append(md5.charAt(6));
        sb.append(md5.charAt(8));
        sb.append(md5.charAt(15));
        sb.append(md5.charAt(12));
        sb.append(md5.charAt(16));
        sb.append(md5.charAt(18));
        return sb.toString();
    }
    
    public static void main(String[] args) {
        System.out.println(getRedisPassword("dba_test"));
    }
}
