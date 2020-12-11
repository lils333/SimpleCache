package com.lee.cacheManager;

import com.lee.cache.Cache;
import com.lee.cache.config.CacheConfiguration;
import com.lee.cache.config.LevelDbJniConfiguration;
import com.lee.cache.manager.CacheManager;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.Assert.*;

public class LevelDbJniManagerTest {


    @Test
    public void testLevelDbJniGetCache2() throws IOException {

        String file = this.getClass().getResource(".").getFile();

        File leveldbjni = new File(file, "leveldbjni");
        FileUtils.forceMkdir(leveldbjni);

        CacheManager cacheManager = CacheManager.newCacheManager();

        try {
            Cache<Integer, String> ehcacheManagerTest1 = cacheManager.getCache(
                    new LevelDbJniConfiguration<Integer, String>()
                            .name("LevelDbJniManagerTest1")
                            .path(leveldbjni.getPath())
                            .cacheSize(8, CacheConfiguration.Unit.MB)
                            .writeBufferSize(4, CacheConfiguration.Unit.MB)
                            .createdIfMissing(true)
                            .maxOpenFiles(1000)
                            .truncate(false)
                            .verifyChecksums(false)
            );

            try {
                Cache<Long, String> ehcacheManagerTest2 = cacheManager.getCache(
                        new LevelDbJniConfiguration<>(Long.class, String.class)
                                .name("LevelDbJniManagerTest1")
                                .path(leveldbjni.getPath())
                                .cacheSize(8, CacheConfiguration.Unit.MB)
                                .writeBufferSize(4, CacheConfiguration.Unit.MB)
                                .createdIfMissing(true)
                                .maxOpenFiles(1000)
                                .truncate(false)
                                .verifyChecksums(false)
                );
            } catch (Exception e) {
                e.printStackTrace();
            }

        } finally {
            cacheManager.close();
            FileUtils.deleteQuietly(leveldbjni);
        }
    }


    @Test
    public void testLevelDbJniGetCache() throws IOException {

        String file = this.getClass().getResource(".").getFile();

        File leveldbjni = new File(file, "leveldbjni");
        FileUtils.forceMkdir(leveldbjni);

        CacheManager cacheManager = CacheManager.newCacheManager();

        try {
            Cache<Integer, String> ehcacheManagerTest1 = cacheManager.getCache(
                    new LevelDbJniConfiguration<>(Integer.class, String.class)
                            .name("LevelDbJniManagerTest1")
                            .path(leveldbjni.getPath())
                            .cacheSize(8, CacheConfiguration.Unit.MB)
                            .writeBufferSize(4, CacheConfiguration.Unit.MB)
                            .createdIfMissing(true)
                            .maxOpenFiles(1000)
                            .truncate(false)
                            .verifyChecksums(false)
            );

            try {
                Cache<Long, String> ehcacheManagerTest2 = cacheManager.getCache(
                        new LevelDbJniConfiguration<>(Long.class, String.class)
                                .name("LevelDbJniManagerTest2")
                                .path(leveldbjni.getPath())
                                .cacheSize(8, CacheConfiguration.Unit.MB)
                                .writeBufferSize(4, CacheConfiguration.Unit.MB)
                                .createdIfMissing(true)
                                .maxOpenFiles(1000)
                                .truncate(false)
                                .verifyChecksums(false)
                );
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                Cache<Long, String> ehcacheManagerTest3 = cacheManager.getCache(
                        new LevelDbJniConfiguration<Long, String>()
                                .name("LevelDbJniManagerTest2")
                                .path(leveldbjni.getPath())
                                .cacheSize(8, CacheConfiguration.Unit.MB)
                                .writeBufferSize(4, CacheConfiguration.Unit.MB)
                                .createdIfMissing(true)
                                .maxOpenFiles(1000)
                                .truncate(false)
                                .verifyChecksums(false)
                );
            } catch (Exception e) {
                e.printStackTrace();
            }

            Cache<Integer, String> ehcacheManagerTest4 = cacheManager.getCache(
                    new LevelDbJniConfiguration<>(Integer.class, String.class)
                            .name("LevelDbJniManagerTest1")
                            .path(leveldbjni.getPath())
                            .cacheSize(8, CacheConfiguration.Unit.MB)
                            .writeBufferSize(4, CacheConfiguration.Unit.MB)
                            .createdIfMissing(true)
                            .maxOpenFiles(1000)
                            .truncate(false)
                            .verifyChecksums(false)
            );

            assertTrue(ehcacheManagerTest1 == ehcacheManagerTest4);

        } finally {
            cacheManager.close();
            FileUtils.deleteQuietly(leveldbjni);
        }
    }

    @Test
    public void testLevelDbJni() throws IOException {
        String file = this.getClass().getResource(".").getFile();

        File leveldbjni = new File(file, "leveldbjni");
        FileUtils.forceMkdir(leveldbjni);

        CacheManager cacheManager = CacheManager.newCacheManager();
        try {
            Cache<Integer, String> ehcacheManagerTest = cacheManager.getCache(
                    new LevelDbJniConfiguration()
                            .name("LevelDbJniManagerTest")
                            .path(leveldbjni.getPath())
                            .cacheSize(8, CacheConfiguration.Unit.MB)
                            .writeBufferSize(4, CacheConfiguration.Unit.MB)
                            .createdIfMissing(true)
                            .maxOpenFiles(1000)
                            .truncate(false)
                            .verifyChecksums(false)
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

            ehcacheManagerTest.consume(new BiConsumer<Integer, String>() {
                @Override
                public void accept(Integer integer, String s) {
                    System.out.println(integer + " = " + s);
                }
            });

            ehcacheManagerTest.consumeKey(new Consumer<Integer>() {
                @Override
                public void accept(Integer integer) {
                    System.out.println(integer);
                }
            });

            ehcacheManagerTest.consumeValue(new Consumer<String>() {
                @Override
                public void accept(String s) {
                    System.out.println(s);
                }
            });

            ehcacheManagerTest.delete(Arrays.asList(11, 12));

            ehcacheManagerTest.consume(new BiConsumer<Integer, String>() {
                @Override
                public void accept(Integer integer, String s) {
                    System.out.println(integer + " = " + s);
                }
            });

            ehcacheManagerTest.put(15, UUID.randomUUID().toString());
            ehcacheManagerTest.put(16, UUID.randomUUID().toString());
            ehcacheManagerTest.put(17, UUID.randomUUID().toString());
            ehcacheManagerTest.put(18, UUID.randomUUID().toString());

            Cache<Integer, String> ehcacheManagerTest1 = cacheManager.getCache(
                    new LevelDbJniConfiguration<>(Integer.class, String.class)
                            .name("LevelDbJniManagerTest1")
                            .path(leveldbjni.getPath())
                            .cacheSize(8, CacheConfiguration.Unit.MB)
                            .writeBufferSize(4, CacheConfiguration.Unit.MB)
                            .createdIfMissing(true)
                            .maxOpenFiles(1000)
                            .truncate(false)
                            .verifyChecksums(false)
            );

            for (int i = 0; i < 10; i++) {
                ehcacheManagerTest1.put(i, UUID.randomUUID().toString());
            }

            for (int i = 0; i < 10; i++) {
                System.out.println(ehcacheManagerTest1.get(i));
            }
            map = new HashMap<Integer, String>() {
                {
                    put(11, UUID.randomUUID().toString());
                    put(12, UUID.randomUUID().toString());
                    put(13, UUID.randomUUID().toString());
                    put(14, UUID.randomUUID().toString());
                }
            };

            ehcacheManagerTest1.put(map);
            System.out.println(ehcacheManagerTest1.get(11));
            System.out.println(ehcacheManagerTest1.get(12));
            System.out.println(ehcacheManagerTest1.get(13));
            System.out.println(ehcacheManagerTest1.get(14));

            for (int i = 0; i < 10; i++) {
                ehcacheManagerTest1.delete(i);
            }

            ehcacheManagerTest1.consume(new BiConsumer<Integer, String>() {
                @Override
                public void accept(Integer integer, String s) {
                    System.out.println(integer + " = " + s);
                }
            });

            ehcacheManagerTest1.consumeKey(new Consumer<Integer>() {
                @Override
                public void accept(Integer integer) {
                    System.out.println(integer);
                }
            });

            ehcacheManagerTest1.consumeValue(new Consumer<String>() {
                @Override
                public void accept(String s) {
                    System.out.println(s);
                }
            });

            ehcacheManagerTest1.delete(Arrays.asList(11, 12));

            ehcacheManagerTest1.consume(new BiConsumer<Integer, String>() {
                @Override
                public void accept(Integer integer, String s) {
                    System.out.println(integer + " = " + s);
                }
            });

            ehcacheManagerTest1.put(15, UUID.randomUUID().toString());
            ehcacheManagerTest1.put(16, UUID.randomUUID().toString());
            ehcacheManagerTest1.put(17, UUID.randomUUID().toString());
            ehcacheManagerTest1.put(18, UUID.randomUUID().toString());

        } finally {
            cacheManager.close();
            FileUtils.deleteQuietly(leveldbjni);
        }
    }
}