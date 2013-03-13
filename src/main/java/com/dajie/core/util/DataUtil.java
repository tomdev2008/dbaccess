package com.dajie.core.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.ResultSet;

import org.apache.log4j.Logger;

import com.dajie.core.dbaccess.annotation.TableColumn;
import com.dajie.core.dbresource.Constants;

/**
 * convert mysql data to java Object
 * 
 * Class<?> cla must be a regular JavaBean Class
 * 
 * @author yong.li@dajie-inc.com
 * 
 */
public class DataUtil {

    private static Logger logger = Constants.logger;

    @SuppressWarnings("unchecked")
    public static <T> T convert(ResultSet rs, Class<T> cla) throws Exception {
        Object obj = null;
        try {
            obj = cla.newInstance();
        } catch (Exception e) {
            throw e;
        }
        Field[] fields = cla.getDeclaredFields();
        for (Field field : fields) {
            String fName = field.getName();
            String setMethodName = "set" + fName.substring(0, 1).toUpperCase() + fName.substring(1);
            Method setMethod = null;
            try {
                setMethod = cla.getDeclaredMethod(setMethodName, field.getType());
            } catch (Exception e) {
                logger.error(e);
            }
            if (setMethod == null) {
                continue;
            }
            TableColumn anno = field.getAnnotation(TableColumn.class);
            String tableColumnName;
            if (anno != null) {
                tableColumnName = anno.name();
            } else {
                tableColumnName = field.getName();
            }
            try {
                Object data = rs.getObject(tableColumnName);
                setMethod.invoke(obj, data);
            } catch (Exception e) {
                logger.error(e);
            }
        }
        return (T) obj;
    }
}
