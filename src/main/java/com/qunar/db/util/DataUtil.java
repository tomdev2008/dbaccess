package com.qunar.db.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.ResultSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qunar.db.access.annotation.TableColumn;

/**
 * convert mysql data to java Object
 * 
 * Class<?> cla must be a regular JavaBean Class
 * 
 * @author liyong19861014@gmail.com
 * 
 */
public class DataUtil {

    private static final Logger logger = LoggerFactory.getLogger(DbLogger.class);

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
                logger.error(e.getMessage(), e);
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
                logger.error(e.getMessage(), e);
            }
        }
        return (T) obj;
    }
}
