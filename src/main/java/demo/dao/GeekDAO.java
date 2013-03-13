package demo.dao;

import demo.Geek;

public interface GeekDAO {

    public Geek getById(int id) throws Exception;

    public int insert(final Geek g) throws Exception;
}
