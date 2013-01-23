package com.dajie.core.admin;

import java.io.IOException;

import org.apache.log4j.BasicConfigurator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.dajie.core.util.TextFileUtil;

public class ShitAdmin {

	public static void main(String[] args) {

		BasicConfigurator.configure();
		String fileName = "database_desc.json";
		String jsonContent = TextFileUtil.read(fileName);
		try {
			JSONArray root = new JSONArray(jsonContent);
			ZooKeeper zk = new ZooKeeper("localhost:2181", 5000, new Watcher() {

				public void process(WatchedEvent event) {
					System.out.println(event.toString());
				}
			});
			for (int i = 0; i < root.length(); ++i) {
				JSONObject obj = root.getJSONObject(i);
				String bizName = obj.getString("name");
				if ("".equals(bizName)) {
					continue;
				}
				String prefixPath = "/database_desc/" + bizName;
				Stat stat = zk.exists(prefixPath, false);
				if (stat == null) { // no path
					zk.create(prefixPath,
							String.valueOf(System.currentTimeMillis())
									.getBytes(), Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT);
				}
				// create every node
				JSONArray nodes = obj.getJSONArray("nodes");
				if (nodes != null && nodes.length() != 0) {
					for (int j = 0; j < nodes.length(); ++j) {
						JSONObject node = nodes.getJSONObject(j);
						String nodeString = node.toString();
						String nodePath = prefixPath + "/" + nodeString;
						zk.create(nodePath,
								String.valueOf(System.currentTimeMillis())
										.getBytes(), Ids.OPEN_ACL_UNSAFE,
								CreateMode.PERSISTENT);
					}
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(0);
	}

}
