package sirius.cache;

/**
 * namespace和bussiness容器
 * 
 * @author michael
 * @email liyong19861014@gmail.com
 */
public class Entry {

    private String namespace;

    private String business;

    public Entry(String namespace, String business) {
        this.namespace = namespace;
        this.business = business;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getBusiness() {
        return business;
    }

    public void setBusiness(String business) {
        this.business = business;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Entry)) {
            return false;
        }
        Entry e = (Entry) obj;
        return (namespace.equals(e.namespace) && business.equals(e.business));
    }

    @Override
    public int hashCode() {
        int result = 17;
        result += namespace.hashCode() * 37;
        result += business.hashCode() * 37;
        return result;
    }
}
