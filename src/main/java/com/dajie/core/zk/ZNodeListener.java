package com.dajie.core.zk;

import java.util.List;

/**
 * 
 * @author yong.li@dajie-inc.com
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
