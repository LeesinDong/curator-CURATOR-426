/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.curator.framework.recipes.locks;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.curator.RetryLoop;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class LockInternals {
    private final WatcherRemoveCuratorFramework client;
    private final String path;
    private final String basePath;
    private final LockInternalsDriver driver;
    private final String lockName;
    private final AtomicReference<RevocationSpec> revocable = new AtomicReference<RevocationSpec>(null);
    private final CuratorWatcher revocableWatcher = new CuratorWatcher() {
        @Override
        public void process(WatchedEvent event) throws Exception {
            if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                checkRevocableWatcher(event.getPath());
            }
        }
    };

    private final Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            notifyFromWatcher();
        }
    };

    private volatile int maxLeases;

    static final byte[] REVOKE_MESSAGE = "__REVOKE__".getBytes();

    /**
     * Attempt to delete the lock node so that sequence numbers get reset
     *
     * @throws Exception errors
     */
    public void clean() throws Exception {
        try {
            client.delete().forPath(basePath);
        } catch (KeeperException.BadVersionException ignore) {
            // ignore - another thread/process got the lock
        } catch (KeeperException.NotEmptyException ignore) {
            // ignore - other threads/processes are waiting
        }
    }

    LockInternals(CuratorFramework client, LockInternalsDriver driver, String path, String lockName, int maxLeases) {
        this.driver = driver;
        this.lockName = lockName;
        this.maxLeases = maxLeases;

        this.client = client.newWatcherRemoveCuratorFramework();
        this.basePath = PathUtils.validatePath(path);
        this.path = ZKPaths.makePath(path, lockName);
    }

    synchronized void setMaxLeases(int maxLeases) {
        this.maxLeases = maxLeases;
        notifyAll();
    }

    void makeRevocable(RevocationSpec entry) {
        revocable.set(entry);
    }

    final void releaseLock(String lockPath) throws Exception {
        client.removeWatchers();
        revocable.set(null);
        //// 删除临时顺序节点，只会触发后一顺序节点去
        // 获取锁，理论上不存在竞争，只排队，非抢占，公平锁，先到先得
        deleteOurPath(lockPath);
    }

    CuratorFramework getClient() {
        return client;
    }

    public static Collection<String> getParticipantNodes(CuratorFramework client, final String basePath, String lockName, LockInternalsSorter sorter) throws Exception {
        List<String> names = getSortedChildren(client, basePath, lockName, sorter);
        Iterable<String> transformed = Iterables.transform
                (
                        names,
                        new Function<String, String>() {
                            @Override
                            public String apply(String name) {
                                return ZKPaths.makePath(basePath, name);
                            }
                        }
                );
        return ImmutableList.copyOf(transformed);
    }

    public static List<String> getSortedChildren(CuratorFramework client, String basePath, final String lockName, final LockInternalsSorter sorter) throws Exception {
        List<String> children = client.getChildren().forPath(basePath);
        List<String> sortedList = Lists.newArrayList(children);
        Collections.sort
                (
                        sortedList,
                        new Comparator<String>() {
                            @Override
                            public int compare(String lhs, String rhs) {
                                return sorter.fixForSorting(lhs, lockName).compareTo(sorter.fixForSorting(rhs, lockName));
                            }
                        }
                );
        return sortedList;
    }

    public static List<String> getSortedChildren(final String lockName, final LockInternalsSorter sorter, List<String> children) {
        List<String> sortedList = Lists.newArrayList(children);
        Collections.sort
                (
                        sortedList,
                        new Comparator<String>() {
                            @Override
                            public int compare(String lhs, String rhs) {
                                return sorter.fixForSorting(lhs, lockName).compareTo(sorter.fixForSorting(rhs, lockName));
                            }
                        }
                );
        return sortedList;
    }

    List<String> getSortedChildren() throws Exception {
        return getSortedChildren(client, basePath, lockName, driver);
    }

    String getLockName() {
        return lockName;
    }

    LockInternalsDriver getDriver() {
        return driver;
    }

    //// 尝试获取锁，并返回锁对应的 Zookeeper 临时顺
    // 序节点的路径
    String attemptLock(long time, TimeUnit unit, byte[] lockNodeBytes) throws Exception {
        final long startMillis = System.currentTimeMillis();
        //// 无限等待时， millisToWait 为 null
        final Long millisToWait = (unit != null) ? unit.toMillis(time) : null;
        //// 创建 ZNode 节点时的数据内容，无关紧要，
        // 这里为 null，采用默认值（IP 地址）
        final byte[] localLockNodeBytes = (revocable.get() != null) ? new byte[0] : lockNodeBytes;
        //// 当前已经重试次数，与CuratorFramework的
        // 重试策略有关
        int retryCount = 0;
        //// 在 Zookeeper 中创建的临时顺序节点的路径，相当于一把待激活的分布式锁
        // // 激活条件：同级目录子节点，名称排序最小
        //（排队，公平锁），后续继续分析

        //ourPath不是最小的那个，是每一个
        //ourPath 是当前的path，当前的path在当前这个类的构造函数的时候被改变成了/path/lockname
        String ourPath = null;
        //// 是否已经持有分布式锁
        boolean hasTheLock = false;
        //// 是否已经完成尝试获取分布式锁的操作
        boolean isDone = false;
        while (!isDone) {
            isDone = true;

            try {
                //// 从 InterProcessMutex 的构造函数
                // 可知实际 driver 为 StandardLockInternalsDriver 的实例
                // // 在Zookeeper中创建临时顺序节点
                ourPath = driver.createsTheLock(client, path, localLockNodeBytes);
                //// 循环等待来激活分布式锁，实现锁
                // 的公平性，后续继续分析
                hasTheLock = internalLockLoop(startMillis, millisToWait, ourPath);
            } catch (KeeperException.NoNodeException e) {
                // gets thrown by StandardLockInternalsDriver when it can't find the lock node
                // this can happen when the session expires, etc. So, if the retry allows, just try it all again
                //// 容错处理，不影响主逻辑的理解，可
                // 跳过
                // // 因 为 会 话 过 期 等 原 因 ，
                // StandardLockInternalsDriver 因为无法找到创建的临时
                // 顺序节点而抛出 NoNodeException 异常
                if (client.getZookeeperClient().getRetryPolicy().allowRetry(retryCount++, System.currentTimeMillis() - startMillis, RetryLoop.getDefaultRetrySleeper())) {
                    //// 满足重试策略尝试重新获取锁
                    isDone = false;
                } else {
                    //// 不满足重试策略则继续抛出
                    // NoNodeException
                    throw e;
                }
            }
        }

        if (hasTheLock) {
            //// 成功获得分布式锁，返回临时顺序节点
            // 的路径，上层将其封装成锁信息记录在映射表，方便锁重入
            return ourPath;
        }
        //// 获取分布式锁失败，返回 null
        return null;
    }

    private void checkRevocableWatcher(String path) throws Exception {
        RevocationSpec entry = revocable.get();
        if (entry != null) {
            try {
                byte[] bytes = client.getData().usingWatcher(revocableWatcher).forPath(path);
                if (Arrays.equals(bytes, REVOKE_MESSAGE)) {
                    entry.getExecutor().execute(entry.getRunnable());
                }
            } catch (KeeperException.NoNodeException ignore) {
                // ignore
            }
        }
    }

    //// 循环等待来激活分布式锁，实现锁的公平性
    private boolean internalLockLoop(long startMillis, Long millisToWait, String ourPath) throws Exception {
        // 是否已经持有分布式锁
        boolean haveTheLock = false;
        //// 是否需要删除子节点
        boolean doDelete = false;
        try {
            if (revocable.get() != null) {
                client.getData().usingWatcher(revocableWatcher).forPath(ourPath);
            }

            while ((client.getState() == CuratorFrameworkState.STARTED) && !haveTheLock) {
                //// 获取排序后的子节点列表
                //获取排序后的子节点列表，并且从小到大根据节点名称后10位数字进行排序。
                List<String> children = getSortedChildren();
                // 获取前面自己创建的临时顺序子节点的名称
                String sequenceNodeName = ourPath.substring(basePath.length() + 1); // +1 to include the slash
                // 实现锁的公平性的核心逻辑，看下面的分析，进去
                PredicateResults predicateResults = driver.getsTheLock(client, children, sequenceNodeName, maxLeases);
                if (predicateResults.getsTheLock()) {
                    //获得了锁，中断循环，继续返回上层
                    haveTheLock = true;
                } else {
                    //// 没有获得到锁，监听上一临时
                    // 顺序节点
                    //如果上面没有进入if，说明这里的getPathToWatch是上一个节点，不可能是null
                    String previousSequencePath = basePath + "/" + predicateResults.getPathToWatch();
                    synchronized (this) {
                        try {
                            // use getData() instead of exists() to avoid leaving unneeded watchers which is a type of resource leak
                            //exists()会导致导致资源泄漏，因此 exists()可以监听不存在的 ZNode，因此采用 getData()
                            //上一临时顺序节点如果被删除，会唤醒当前线程继续竞争锁，正常情况下能直接获得锁，因为锁是公平的
                            //哪里监听删除的操作了？明明是监听了所有的操作，这里先不深究  ，可能是对面只可能删除，不会执行其他的操作
                            client.getData().usingWatcher(watcher).forPath(previousSequencePath);
                            if (millisToWait != null) {
                                millisToWait -= (System.currentTimeMillis() - startMillis);
                                startMillis = System.currentTimeMillis();
                                if (millisToWait <= 0) {
                                    doDelete = true;    // 获取锁超时，标记删除之前创建的临时顺序节点 // timed out - delete our node
                                    break;
                                }

                                wait(millisToWait);// 等待被唤醒，限时等待
                            } else {
                                wait();// 等待被唤醒，无限等待
                            }
                        } catch (KeeperException.NoNodeException e) {
                            //// 容错处理，逻辑稍微有点
                            // 绕，可跳过，不影响主逻辑的理解
                            // // client.getData()可能调用
                            // 时抛出 NoNodeException，原因可能是锁被释放或会话
                            // 过期（连接丢失）等
                            // // 这里并没有做任何处理，
                            // 因为外层是 while 循环，再次执行 driver.getsTheLock 时
                            // 会调用 validateOurIndex
                            // // 此 时 会 抛 出
                            // NoNodeException，从而进入下面的 catch 和 finally 逻
                            // 辑，重新抛出上层尝试重试获取锁并删除临时顺序节点
                            // it has been deleted (i.e. lock released). Try to acquire again
                        }
                    }
                }
            }
        } catch (Exception e) {
            ThreadUtils.checkInterrupted(e);
            // 标记删除，在 finally 删除之前创建的临时顺序节点（后台不断尝试）
            doDelete = true;
            // 重新抛出，尝试重新获取锁
            throw e;
        } finally {
            if (doDelete) {
                deleteOurPath(ourPath);
            }
        }
        return haveTheLock;
    }

    private void deleteOurPath(String ourPath) throws Exception {
        try {
            //// 后台不断尝试删除
            client.delete().guaranteed().forPath(ourPath);
        } catch (KeeperException.NoNodeException e) {
            // ignore - already deleted (possibly expired session, etc.)
            //    // 已经删除(可能会话过期导致)，不做处理
            // // 实际使用 Curator-2.12.0 时，并不会抛
            // 出该异常
        }
    }

    private synchronized void notifyFromWatcher() {
        notifyAll();
    }
}
