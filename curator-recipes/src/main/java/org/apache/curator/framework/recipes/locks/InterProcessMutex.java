/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.curator.framework.recipes.locks;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.curator.framework.CuratorFramework;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.utils.PathUtils;

/**
 * A re-entrant mutex that works across JVMs. Uses Zookeeper to hold the lock. All processes in all JVMs that
 * use the same lock path will achieve an inter-process critical section. Further, this mutex is
 * "fair" - each user will get the mutex in the order requested (from ZK's point of view)
 */

// 最常用
public class InterProcessMutex implements InterProcessLock, Revocable<InterProcessMutex>
{
    private final LockInternals internals;
    private final String basePath;
    //// 映射表
    // // 记录线程与锁信息的映射关系
    private final ConcurrentMap<Thread, LockData> threadData = Maps.newConcurrentMap();

    // / 锁信息
// Zookeeper 中一个临时顺序节点对应一个“锁”，但
//     让锁生效激活需要排队（公平锁），下面会继续分析
    private static class LockData
    {
        final Thread owningThread;
        final String lockPath;
        final AtomicInteger lockCount = new AtomicInteger(1);// 分布式锁重入次数

        private LockData(Thread owningThread, String lockPath)
        {
            this.owningThread = owningThread;
            this.lockPath = lockPath;
        }
    }

    private static final String LOCK_NAME = "lock-";

    /**
     * @param client client
     * @param path   the path to lock
     */
    public InterProcessMutex(CuratorFramework client, String path)
    {
        // Zookeeper 利用 path 创建临时顺序节点，实现公
        // 平锁的核心
        this(client, path, new StandardLockInternalsDriver());
    }

    /**
     * @param client client
     * @param path   the path to lock
     * @param driver lock driver
     */

    public InterProcessMutex(CuratorFramework client, String path, LockInternalsDriver driver)
    {
        // maxLeases=1，表示可以获得分布式锁的线程数量
        // （跨 JVM）为 1，即为互斥锁
        //只能获得一个锁
        this(client, path, LOCK_NAME, 1, driver);
    }

    /**
     * Acquire the mutex - blocking until it's available. Note: the same thread
     * can call acquire re-entrantly. Each call to acquire must be balanced by a call
     * to {@link #release()}
     *
     * @throws Exception ZK errors, connection interruptions
     */
    @Override
    //// 无限等待
    public void acquire() throws Exception
    {
        if ( !internalLock(-1, null) )
        {
            throw new IOException("Lost connection while trying to acquire lock: " + basePath);
        }
    }

    /**
     * Acquire the mutex - blocks until it's available or the given time expires. Note: the same thread
     * can call acquire re-entrantly. Each call to acquire that returns true must be balanced by a call
     * to {@link #release()}
     *
     * @param time time to wait
     * @param unit time unit
     * @return true if the mutex was acquired, false if not
     * @throws Exception ZK errors, connection interruptions
     */
    @Override
    // 限时等待
    public boolean acquire(long time, TimeUnit unit) throws Exception
    {
        return internalLock(time, unit);
    }

    /**
     * Returns true if the mutex is acquired by a thread in this JVM
     *
     * @return true/false
     */
    @Override
    public boolean isAcquiredInThisProcess()
    {
        return (threadData.size() > 0);
    }

    /**
     * Perform one release of the mutex if the calling thread is the same thread that acquired it. If the
     * thread had made multiple calls to acquire, the mutex will still be held when this method returns.
     *
     * @throws Exception ZK errors, interruptions, current thread does not own the lock
     */
    @Override
    //释放锁的逻辑
    public void release() throws Exception
    {
        /*
            Note on concurrency: a given lockData instance
            can be only acted on by a single thread so locking isn't necessary
         */

        Thread currentThread = Thread.currentThread();
        LockData lockData = threadData.get(currentThread);
        if ( lockData == null )
        {
            //// 无法从映射表中获取锁信息，不持有锁
            throw new IllegalMonitorStateException("You do not own the lock: " + basePath);
        }

        int newLockCount = lockData.lockCount.decrementAndGet();
        if ( newLockCount > 0 )
        {
            //// 锁是可重入的，初始值为 1，原子-1 到
            // 0，锁才释放
            return;
        }
        if ( newLockCount < 0 )
        {
            //// 理论上无法执行该路径
            throw new IllegalMonitorStateException("Lock count has gone negative for lock: " + basePath);
        }
        try
        {
            //// lockData != null && newLockCount ==
            // 0，释放锁资源
            internals.releaseLock(lockData.lockPath);
        }
        finally
        {
            //// 最后从映射表中移除当前线程的锁信息
            threadData.remove(currentThread);
        }
    }

    /**
     * Return a sorted list of all current nodes participating in the lock
     *
     * @return list of nodes
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Collection<String> getParticipantNodes() throws Exception
    {
        return LockInternals.getParticipantNodes(internals.getClient(), basePath, internals.getLockName(), internals.getDriver());
    }

    @Override
    public void makeRevocable(RevocationListener<InterProcessMutex> listener)
    {
        makeRevocable(listener, MoreExecutors.sameThreadExecutor());
    }

    @Override
    public void makeRevocable(final RevocationListener<InterProcessMutex> listener, Executor executor)
    {
        internals.makeRevocable(new RevocationSpec(executor, new Runnable()
            {
                @Override
                public void run()
                {
                    listener.revocationRequested(InterProcessMutex.this);
                }
            }));
    }
    // protected 构造函数
    InterProcessMutex(CuratorFramework client, String path, String lockName, int maxLeases, LockInternalsDriver driver)
    {
        basePath = PathUtils.validatePath(path);
        // internals 的 类 型 为 LockInternals ，
        // InterProcessMutex 将分布式锁的申请和释放操作委托给
        // internals 执行
        internals = new LockInternals(client, driver, path, lockName, maxLeases);
    }

    /**
     * Returns true if the mutex is acquired by the calling thread
     * 
     * @return true/false
     */
    public boolean isOwnedByCurrentThread()
    {
        LockData lockData = threadData.get(Thread.currentThread());
        return (lockData != null) && (lockData.lockCount.get() > 0);
    }

    protected byte[] getLockNodeBytes()
    {
        return null;
    }

    protected String getLockPath()
    {
        LockData lockData = threadData.get(Thread.currentThread());
        return lockData != null ? lockData.lockPath : null;
    }

    private boolean internalLock(long time, TimeUnit unit) throws Exception
    {
        /*
           Note on concurrency: a given lockData instance
           can be only acted on by a single thread so locking isn't necessary
        */

        Thread currentThread = Thread.currentThread();

        LockData lockData = threadData.get(currentThread);
        if ( lockData != null )
        {
            // re-entering
            //// 实现可重入
            // // 同一线程再次 acquire，首先判断当前的
            // 映射表内（threadData）是否有该线程的锁信息，如果有
            // 则原子+1，然后返回
            lockData.lockCount.incrementAndGet();
            return true;
        }

        // 映射表内没有对应的锁信息 ，尝试通过
        // LockInternals 获取锁
        String lockPath = internals.attemptLock(time, unit, getLockNodeBytes());
        if ( lockPath != null )
        {
            //// 成功获取锁，记录信息到映射表
            LockData newLockData = new LockData(currentThread, lockPath);
            threadData.put(currentThread, newLockData);
            return true;
        }

        return false;
    }
}
