package com.wlzx;

import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.jedis.exceptions.JedisRedirectionException;
import redis.clients.util.JedisClusterCRC16;
import redis.clients.util.SafeEncoder;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class RedisClusterUtil2 extends PipelineBase implements Closeable {
    private JedisSlotBasedConnectionHandler connectionHandler;
    private JedisClusterInfoCache clusterInfoCache;
    private Queue<Client> clients = new LinkedList();
    private Map<JedisPool, Jedis> jedisMap = new HashMap<>();
    private boolean hasDataInBuf = false;


    private void initJedisCluster(JedisCluster jedisCluster) {
        try {
            Field conn = BinaryJedisCluster.class.getDeclaredField("connectionHandler");
            conn.setAccessible(true);
            connectionHandler = (JedisSlotBasedConnectionHandler) conn.get(jedisCluster);
            Field clusterInfo = JedisClusterConnectionHandler.class.getDeclaredField("cache");
            clusterInfo.setAccessible(true);
            clusterInfoCache = (JedisClusterInfoCache) clusterInfo.get(connectionHandler);
        } catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException e) {
            e.printStackTrace();
        }

    }


    public static RedisClusterUtil2 pipelined(JedisCluster jedisCluster) {
        RedisClusterUtil2 rcu = new RedisClusterUtil2();
        rcu.initJedisCluster(jedisCluster);
        return rcu;
    }

    public RedisClusterUtil2() {
    }

    private void refreshCluster() {
        connectionHandler.renewSlotCache();
    }

    private void flushCachedData(Jedis jedis) {
        try {
            jedis.getClient().getAll();
        } catch (RuntimeException ex) {
        }
    }

    public void sync() {
        try {
            for (Client client : clients) {
                generateResponse(client.getOne()).get();
            }
        } catch (JedisRedirectionException jre) {
            if (jre instanceof JedisMovedDataException) {
                refreshCluster();
            }
            throw jre;
        } finally {
            for (Jedis jedis : jedisMap.values()) {
                flushCachedData(jedis);
            }
            hasDataInBuf = false;
            try {
                close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws IOException {
        clean();
        clients.clear();
        for (Jedis jedis : jedisMap.values()) {
            if (hasDataInBuf) {
                flushCachedData(jedis);
            }
            jedis.close();
        }

        jedisMap.clear();

        hasDataInBuf = false;

    }

    @Override
    protected Client getClient(String key) {
        byte[] bKey = SafeEncoder.encode(key);
        return getClient(bKey);
    }

    @Override
    protected Client getClient(byte[] key) {
        Jedis jedis = getJedis(JedisClusterCRC16.getSlot(key));
        Client client = jedis.getClient();
        clients.add(client);

        return client;
    }

    private Jedis getJedis(int slot) {
        JedisPool pool = clusterInfoCache.getSlotPool(slot);
        Jedis jedis = jedisMap.get(pool);
        if (null == jedis) {
            jedis = pool.getResource();
            jedisMap.put(pool, jedis);
        }
        hasDataInBuf = true;
        return jedis;
    }

    // 集群所有节点
    public static Set<HostAndPort> getRedisClusterNodes() {
        Set<HostAndPort> jedisClusterNodes = new HashSet<>();
        jedisClusterNodes.add(new HostAndPort("10.243.170.196", 7002));
        jedisClusterNodes.add(new HostAndPort("10.243.170.197", 7002));
        jedisClusterNodes.add(new HostAndPort("10.243.170.198", 7002));
        jedisClusterNodes.add(new HostAndPort("10.243.170.199", 7002));
        jedisClusterNodes.add(new HostAndPort("10.243.170.200", 7002));
        jedisClusterNodes.add(new HostAndPort("10.243.170.201", 7002));
        jedisClusterNodes.add(new HostAndPort("10.243.170.202", 7002));
        jedisClusterNodes.add(new HostAndPort("10.243.170.203", 7002));
        jedisClusterNodes.add(new HostAndPort("10.243.170.204", 7002));
        jedisClusterNodes.add(new HostAndPort("10.243.170.205", 7002));
        jedisClusterNodes.add(new HostAndPort("10.243.170.206", 7002));
        jedisClusterNodes.add(new HostAndPort("10.243.170.207", 7002));
        jedisClusterNodes.add(new HostAndPort("10.243.170.208", 7002));
        jedisClusterNodes.add(new HostAndPort("10.243.170.209", 7002));
        jedisClusterNodes.add(new HostAndPort("10.243.170.210", 7002));
        jedisClusterNodes.add(new HostAndPort("10.243.170.166", 7002));
        jedisClusterNodes.add(new HostAndPort("10.243.170.167", 7002));
        jedisClusterNodes.add(new HostAndPort("10.243.170.168", 7002));
        jedisClusterNodes.add(new HostAndPort("10.243.170.169", 7002));
        jedisClusterNodes.add(new HostAndPort("10.243.170.172", 7002));
        jedisClusterNodes.add(new HostAndPort("10.243.170.173", 7002));
        jedisClusterNodes.add(new HostAndPort("10.243.170.174", 7002));
        jedisClusterNodes.add(new HostAndPort("10.243.170.175", 7002));
        jedisClusterNodes.add(new HostAndPort("10.243.170.196", 7001));
        jedisClusterNodes.add(new HostAndPort("10.243.170.197", 7001));
        jedisClusterNodes.add(new HostAndPort("10.243.170.198", 7001));
        jedisClusterNodes.add(new HostAndPort("10.243.170.199", 7001));
        jedisClusterNodes.add(new HostAndPort("10.243.170.200", 7001));
        jedisClusterNodes.add(new HostAndPort("10.243.170.201", 7001));
        jedisClusterNodes.add(new HostAndPort("10.243.170.202", 7001));
        jedisClusterNodes.add(new HostAndPort("10.243.170.203", 7001));
        jedisClusterNodes.add(new HostAndPort("10.243.170.204", 7001));
        jedisClusterNodes.add(new HostAndPort("10.243.170.205", 7001));
        jedisClusterNodes.add(new HostAndPort("10.243.170.206", 7001));
        jedisClusterNodes.add(new HostAndPort("10.243.170.207", 7001));
        jedisClusterNodes.add(new HostAndPort("10.243.170.208", 7001));
        jedisClusterNodes.add(new HostAndPort("10.243.170.209", 7001));
        jedisClusterNodes.add(new HostAndPort("10.243.170.210", 7001));
        jedisClusterNodes.add(new HostAndPort("10.243.170.166", 7001));
        jedisClusterNodes.add(new HostAndPort("10.243.170.167", 7001));
        jedisClusterNodes.add(new HostAndPort("10.243.170.168", 7001));
        jedisClusterNodes.add(new HostAndPort("10.243.170.169", 7001));
        jedisClusterNodes.add(new HostAndPort("10.243.170.172", 7001));
        jedisClusterNodes.add(new HostAndPort("10.243.170.173", 7001));
        jedisClusterNodes.add(new HostAndPort("10.243.170.174", 7001));
        jedisClusterNodes.add(new HostAndPort("10.243.170.175", 7001));
        jedisClusterNodes.add(new HostAndPort("10.243.170.196", 7003));
        jedisClusterNodes.add(new HostAndPort("10.243.170.197", 7003));
        jedisClusterNodes.add(new HostAndPort("10.243.170.198", 7003));
        jedisClusterNodes.add(new HostAndPort("10.243.170.199", 7003));
        jedisClusterNodes.add(new HostAndPort("10.243.170.200", 7003));
        jedisClusterNodes.add(new HostAndPort("10.243.170.201", 7003));
        jedisClusterNodes.add(new HostAndPort("10.243.170.202", 7003));
        jedisClusterNodes.add(new HostAndPort("10.243.170.203", 7003));
        jedisClusterNodes.add(new HostAndPort("10.243.170.204", 7003));
        jedisClusterNodes.add(new HostAndPort("10.243.170.205", 7003));
        jedisClusterNodes.add(new HostAndPort("10.243.170.206", 7003));
        jedisClusterNodes.add(new HostAndPort("10.243.170.207", 7003));
        jedisClusterNodes.add(new HostAndPort("10.243.170.208", 7003));
        jedisClusterNodes.add(new HostAndPort("10.243.170.209", 7003));
        jedisClusterNodes.add(new HostAndPort("10.243.170.210", 7003));
        jedisClusterNodes.add(new HostAndPort("10.243.170.166", 7003));
        jedisClusterNodes.add(new HostAndPort("10.243.170.167", 7003));
        jedisClusterNodes.add(new HostAndPort("10.243.170.168", 7003));
        jedisClusterNodes.add(new HostAndPort("10.243.170.169", 7003));
        jedisClusterNodes.add(new HostAndPort("10.243.170.172", 7003));
        jedisClusterNodes.add(new HostAndPort("10.243.170.173", 7003));
        jedisClusterNodes.add(new HostAndPort("10.243.170.174", 7003));
        jedisClusterNodes.add(new HostAndPort("10.243.170.175", 7003));

        return jedisClusterNodes;
    }

    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);
        System.out.println("please enter a num:");
        int v = scanner.nextInt();
        long start = System.currentTimeMillis();
        JedisCluster cluster = new JedisCluster(getRedisClusterNodes(), 5000, 3000, 3, "1qazXSW@", new JedisPoolConfig());
        RedisClusterUtil2 rc = RedisClusterUtil2.pipelined(cluster);
        rc.initJedisCluster(cluster);
        rc.refreshCluster();
        rc.close();
        Path path = Paths.get("data-1000.txt");
        List<String> list = Files.readAllLines(path, Charset.forName("utf-8"));
        int count = 1;
        String[] split;
        String key;
        Map<String, String> field_value;
        String[] fileds = getFileds();
        try {
            for (String lineData : list) {
                int index = 0;
                split = lineData.split("\\|");
                key = "yaxin:" + split[0];
                field_value = new HashMap<>();
                for (String value : split) {
                    field_value.put(fileds[index++], value);
                }
                rc.hmset(key, field_value);
                rc.expire(key, 60);
                if (count % v == 0) {
                    rc.sync();
                }
                count++;
            }
            rc.sync();
        } finally {
            rc.close();
        }

        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    // 获取hash的域名
    public static String[] getFileds() {
        return new String[]{"age", "name", "sex", "height", "weight", "money", "house", "addr", "qq", "phone"};
    }
}