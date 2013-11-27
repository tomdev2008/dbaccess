package sirius.zk;

import java.util.List;

/**
 * 
 * @author liyong19861014@gmail.com
 * 
 */
public abstract class ZNodeListener {

    private String znode = "";

    public ZNodeListener(String znode) {
        if (znode != null) {
            this.znode = znode;
        }
    }

    public String getZNode() {
        return znode;
    }

    public abstract boolean update(List<String> childrenNameList);
}
