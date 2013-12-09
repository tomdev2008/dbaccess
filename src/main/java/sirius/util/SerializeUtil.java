package sirius.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.log4j.Logger;

import sirius.cache.Constant;

/**
 * 
 * @author liyong19861014@gmail.com
 * 
 */
public class SerializeUtil {

    private static int BUFFER_SIZE = 1024 * 10 * 4;

    private static Logger logger = Constant.logger;

    /**
     * 序列化
     * 
     * @param obj
     * @return
     * @throws NullPointerException
     * @throws IOException
     */
    public static byte[] serialize(Object obj) throws NullPointerException, IOException {
        if (obj == null) {
            throw new NullPointerException("obj == null");
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(BUFFER_SIZE);
        ObjectOutputStream objStream = new ObjectOutputStream(out);
        objStream.writeObject(obj);
        byte[] bytes = out.toByteArray();
        objStream.close();
        return bytes;
    }

    /**
     * 反序列化
     * 
     * @param byteArray
     * @return
     * @throws NullPointerException
     * @throws IOException
     */
    public static Object deserialize(byte[] byteArray) throws NullPointerException, IOException {
        if (byteArray == null || byteArray.length == 0) {
            throw new NullPointerException("byteArray == null or length is 0");
        }

        ByteArrayInputStream in = new ByteArrayInputStream(byteArray);
        ObjectInputStream objStream = new ObjectInputStream(in);
        Object obj = null;
        try {
            obj = objStream.readObject();
        } catch (ClassNotFoundException e) {
            logger.error(e);
        }
        objStream.close();
        return obj;
    }
}
