/*
 * Copyright 1999-2011 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.registry.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.registry.NotifyListener;
import com.alibaba.dubbo.registry.Registry;
import com.alibaba.dubbo.rpc.RpcException;

/**
 * ZookeeperRegistry
 * 
 * @author william.liangf
 */
public class ZookeeperRegistry implements Registry {

    private final static Logger logger = LoggerFactory.getLogger(ZookeeperRegistry.class);
    
    private final static String SEPARATOR = "/";

    private final URL           url;
    
    private final boolean       auth;

    private final ZooKeeper     zookeeper;
    
    private final ConcurrentMap<String, Map<NotifyListener, NotifyWatcher>> wathers = new ConcurrentHashMap<String, Map<NotifyListener, NotifyWatcher>>();

    public ZookeeperRegistry(URL url) {
        this.url = url;
        try {
            String address = url.getAddress();
            String backup = url.getParameter(Constants.BACKUP_KEY);
            if (backup != null && backup.length() > 0) {
                address = address + "," + backup;
            }
            this.zookeeper = new ZooKeeper(address, url.getPositiveIntParameter(
                    Constants.TIMEOUT_KEY, 5000), new Watcher() {
                public void process(WatchedEvent event) {
                }
            });
            this.auth = url.getUsername() != null && url.getUsername().length() > 0 
                    && url.getPassword() != null && url.getPassword().length() > 0;
            if (auth) {
                zookeeper.addAuthInfo(url.getUsername(), url.getPassword().getBytes());
            }
        } catch (IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public URL getUrl() {
        return url;
    }

    public boolean isAvailable() {
        return zookeeper.getState().isAlive();
    }

    public void destroy() {
        try {
            zookeeper.close();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }

    public void register(URL url) {
        try {
            String service = toServicePath(url);
            String provider = service + toProviderPath(url);
            if (zookeeper.exists(service, false) == null) {
                zookeeper.create(service, new byte[0], auth ? Ids.CREATOR_ALL_ACL : Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            if (zookeeper.exists(provider, false) == null) {
                zookeeper.create(provider, url.toParameterString().getBytes(), auth ? Ids.CREATOR_ALL_ACL : Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            }
        } catch (Throwable e) {
            throw new RpcException("Failed to register " + url + ", cause: " + e.getMessage(), e);
        }
    }

    public void unregister(URL url) {
        try {
            String service = toServicePath(url);
            String provider = service + toProviderPath(url);
            zookeeper.delete(provider, -1);
        } catch (Throwable e) {
            throw new RpcException("Failed to unregister " + url + ", cause: " + e.getMessage(), e);
        }
    }

    public void subscribe(URL url, NotifyListener listener) {
        try {
            String service = toServicePath(url);
            NotifyWatcher wather = new NotifyWatcher(this, url, listener);
            Map<NotifyListener, NotifyWatcher> serviceWathers = wathers.get(service);
            if (serviceWathers == null) {
                wathers.put(service, new ConcurrentHashMap<NotifyListener, NotifyWatcher>());
                serviceWathers = wathers.get(service);
            }
            serviceWathers.put(listener, wather);
            List<String> providers = zookeeper.getChildren(service, wather);
            List<URL> urls = toUrls(service, providers);
            if (urls != null && urls.size() > 0) {
                listener.notify(urls);
            }
        } catch (Throwable e) {
            throw new RpcException("Failed to subscribe " + url + ", cause: " + e.getMessage(), e);
        }
    }

    public void unsubscribe(URL url, NotifyListener listener) {
        String service = toServicePath(url);
        Map<NotifyListener, NotifyWatcher> serviceWathers = wathers.remove(service);
        if (serviceWathers != null && serviceWathers.size() > 0) {
            NotifyWatcher wather = serviceWathers.remove(listener);
            if (wather != null) {
                wather.setListener(null);
            }
        }
    }

    public List<URL> lookup(URL url) {
        try {
            String service = toServicePath(url);
            List<String> providers = zookeeper.getChildren(service, false);
            return toUrls(service, providers);
        } catch (Throwable e) {
            throw new RpcException("Failed to lookup " + url + ", cause: " + e.getMessage(), e);
        }
    }
    
    private String toServicePath(URL url) {
        return SEPARATOR + URL.encode(url.getServiceKey());
    }
    
    private String toProviderPath(URL url) {
        return SEPARATOR + URL.encode(url.toIdentityString());
    }
    
    private List<URL> toUrls(String service, List<String> providers) throws KeeperException, InterruptedException {
        List<URL> urls = new ArrayList<URL>();
        for (String provider : providers) {
            String path = service + provider;
            String query = "";
            Stat stat = zookeeper.exists(path, false);
            if (stat != null) {
                byte[] data = zookeeper.getData(path, false, stat);
                if (data != null && data.length > 0) {
                    query = "?" + new String(data);
                }
            }
            urls.add(URL.valueOf(URL.decode(provider + query)));
        }
        return urls;
    }
    
    private static class NotifyWatcher implements Watcher {
        
        private final Registry registry;
        
        private final URL url;

        private transient NotifyListener listener;
        
        public NotifyWatcher(Registry registry, URL url, NotifyListener listener) {
            this.registry = registry;
            this.url = url;
            this.listener = listener;
        }

        public void setListener(NotifyListener listener) {
            this.listener = listener;
        }

        public void process(WatchedEvent event) {
            NotifyListener listener = this.listener;
            if (listener != null && (event.getType() == EventType.NodeChildrenChanged
                    || event.getType() == EventType.NodeDataChanged)) {
                List<URL> urls = registry.lookup(url);
                if (urls != null && urls.size() > 0) {
                    listener.notify(urls);
                }
            }
        }

    }

}