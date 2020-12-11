package com.lee.cacheManager;


import com.lee.cache.Cache;
import com.lee.cache.config.CacheConfiguration;
import com.lee.cache.config.EhcacheConfiguration;
import com.lee.cache.manager.CacheManager;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class EhcacheManagerTest {

    @Test
    public void testLevelDbJni() throws IOException {
        String file = this.getClass().getResource(".").getFile();

        File leveldbjni = new File(file, "leveldbjna");
        FileUtils.forceMkdir(leveldbjni);

        CacheManager cacheManager = CacheManager.newCacheManager();
        try {
            Cache<Integer, String> ehcacheManagerTest = cacheManager.getCache(
                    new EhcacheConfiguration()
                            .name("LevelDbJnaManagerTest")
                            .path(leveldbjni.getPath())
                            .evictionPolicy(CacheConfiguration.EvictionPolicy.LRU)
                            .timeToLiveSeconds(500)
                            .maxEntriesInMemory(5)
                            .maxBytesLocalDisk(5, CacheConfiguration.Unit.MB)
            );

            for (int i = 0; i < 10; i++) {
                ehcacheManagerTest.put(i, UUID.randomUUID().toString());
            }

            for (int i = 0; i < 10; i++) {
                System.out.println(ehcacheManagerTest.get(i));
            }
            Map<Integer, String> map = new HashMap<Integer, String>() {
                {
                    put(11, UUID.randomUUID().toString());
                    put(12, UUID.randomUUID().toString());
                    put(13, UUID.randomUUID().toString());
                    put(14, UUID.randomUUID().toString());
                }
            };

            ehcacheManagerTest.put(map);
            System.out.println(ehcacheManagerTest.get(11));
            System.out.println(ehcacheManagerTest.get(12));
            System.out.println(ehcacheManagerTest.get(13));
            System.out.println(ehcacheManagerTest.get(14));

            for (int i = 0; i < 10; i++) {
                ehcacheManagerTest.delete(i);
            }
        } finally {
            cacheManager.close();
            FileUtils.deleteQuietly(leveldbjni);
        }
    }
}