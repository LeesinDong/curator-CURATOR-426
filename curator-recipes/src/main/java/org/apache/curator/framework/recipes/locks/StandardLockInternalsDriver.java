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

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

public class StandardLockInternalsDriver implements LockInternalsDriver
{
    static private final Logger log = LoggerFactory.getLogger(StandardLockInternalsDriver.class);

    @Override
    public PredicateResults getsTheLock(CuratorFramework client, List<String> children, String sequenceNodeName, int maxLeases) throws Exception
    {
        //// 之前创建的临时顺序节点在排序后的子节点
        // 列表中的索引
        int   ourIndex = children.indexOf(sequenceNodeName);
        //// 校验之前创建的临时顺序节点是否有效
        validateOurIndex(sequenceNodeName, ourIndex);
        // 锁公平性的核心逻辑由 InterProcessMutex 的构造函数可知，
        // maxLeases 为 1，即只有 ourIndex 为 0 时，线程才能持
        // 有锁，或者说该线程创建的临时顺序节点激活了锁
        // Zookeeper 的临时顺序节点特性能保证跨多
        // 个 JVM 的线程并发创建节点时的顺序性，越早创建临时
        // 顺序节点成功的线程会更早地激活锁或获得锁
        boolean   getsTheLock = ourIndex < maxLeases;
        // 如果已经获得了锁，则无需监听任何节点，否则需要监听上一顺序节点（ourIndex-1）
        //因 为 锁 是 公 平 的 ， 因 此 无 需 监 听 除 了（ourIndex-1）以外的所有节点，这是为了减少羊群效应，
        // 非常巧妙的设计！！

        //pathToWatch 是前一个节点
        String   pathToWatch = getsTheLock ? null : children.get(ourIndex - maxLeases);
        //// 返回获取锁的结果，交由上层继续处理（添
        // 加监听等操作）

        //如果是null就不监听
        return new PredicateResults(pathToWatch, getsTheLock);
    }

    @Override
    // From StandardLockInternalsDriver
// 在 Zookeeper 中创建临时顺序节点
    public String createsTheLock(CuratorFramework client, String path, byte[] lockNodeBytes) throws Exception
    {
        String ourPath;
        // lockNodeBytes 不为 null 则作为数据节点内
        // 容，否则采用默认内容（IP 地址）
        if ( lockNodeBytes != null )
        {
            //// 下面对 CuratorFramework 的一些细节
            // 做解释，不影响对分布式锁主逻辑的解释，可跳过
            // // creatingParentContainersIfNeeded：用
            // 于创建父节点，如果不支持 CreateMode.CONTAINER
            // // 那么将采用 CreateMode.PERSISTENT// withProtection：临时子节点会添加GUID
            // 前缀
            ourPath = client.create().creatingParentContainersIfNeeded().withProtection().
                    ////
                    // CreateMode.EPHEMERAL_SEQUENTIAL ：临时顺序节
                    // 点， Zookeeper 能保证在节点产生的顺序性
                    // // 依据顺序来激活分布式锁，从而也
                    // 实现了分布式锁的公平性，后续继续分析
                    withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path, lockNodeBytes);
        }
        else
        {
            ourPath = client.create().creatingParentContainersIfNeeded().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path);
        }
        return ourPath;
    }


    @Override
    public String fixForSorting(String str, String lockName)
    {
        return standardFixForSorting(str, lockName);
    }

    public static String standardFixForSorting(String str, String lockName)
    {
        int index = str.lastIndexOf(lockName);
        if ( index >= 0 )
        {
            index += lockName.length();
            return index <= str.length() ? str.substring(index) : "";
        }
        return str;
    }

    static void validateOurIndex(String sequenceNodeName, int ourIndex) throws KeeperException
    {
        if ( ourIndex < 0 )
        {
            //// 容错处理，可跳过
            // // 由于会话过期或连接丢失等原因，该线
            // 程创建的临时顺序节点被 Zookeeper 服务端删除，往外
            // 抛出 NoNodeException
            // // 如果在重试策略允许范围内，则进行重
            // 新尝试获取锁，这会重新重新生成临时顺序节点
            // // 佩服 Curator 的作者将边界条件考虑得
            // 如此周到！
            throw new KeeperException.NoNodeException("Sequential path not found: " + sequenceNodeName);
        }
    }
}
