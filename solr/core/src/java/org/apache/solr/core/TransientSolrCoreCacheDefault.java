/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.core;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Sets;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache of the most frequently accessed transient cores. Keeps track of all the registered
 * transient cores descriptors, including the cores in the cache as well as all the others.
 */
public class TransientSolrCoreCacheDefault extends TransientSolrCoreCache {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final CoreContainer coreContainer;

  /**
   * "Lazily loaded" cores cache with limited size. When the max size is reached, the least accessed
   * core is evicted to make room for a new core.
   */
  protected final Cache<String, SolrCore> transientCores;

  /**
   * This explicitly models capacity overflow of transientCores with cores that are still in use. A
   * cores cache which holds cores evicted from transientCores because of limited size but which
   * shall remain cached because they are still referenced or because they are part of an ongoing
   * operation (pending ops).
   */
  protected final Cache<String, SolrCore> overflowCores;

  /**
   * Unlimited map of all the descriptors for all the registered transient cores, including the
   * cores in the {@link #transientCores} as well as all the others.
   */
  protected final Map<String, CoreDescriptor> transientDescriptors;

  /**
   * @param coreContainer The enclosing {@link CoreContainer}.
   */
  public TransientSolrCoreCacheDefault(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
    int cacheMaxSize = getConfiguredCacheMaxSize(coreContainer);

    // Now don't allow ridiculous allocations here, if the size is > 1,000, we'll just deal with
    // adding cores as they're opened. This blows up with the marker value of -1.
    int initialCapacity = Math.min(cacheMaxSize, 1024);
    log.info(
        "Allocating transient core cache for max {} cores with initial capacity of {}",
        cacheMaxSize,
        initialCapacity);
    Caffeine<String, SolrCore> transientCoresCacheBuilder =
        Caffeine.newBuilder()
            .initialCapacity(initialCapacity)
            // Do NOT use the current thread to queue evicted cores for closing. Although this
            // ensures the cache max size is respected only eventually, it should be a very
            // short period of time until the cache maintenance task kicks in.
            // The onEvict method needs the writeLock from SolrCores to operate safely.
            // However, the current thread most probably has acquired a read-lock already
            // somewhere up the call stack and would deadlock.
            // Note that Caffeine cache has an internal maintenance task rescheduling logic which
            // explicitly checks on the common pool.
            .executor(ForkJoinPool.commonPool())
            .removalListener(
                (coreName, core, cause) -> {
                  if (core != null && cause.wasEvicted()) {
                    onEvict(core);
                  }
                });
    if (cacheMaxSize != Integer.MAX_VALUE) {
      transientCoresCacheBuilder.maximumSize(cacheMaxSize);
    }
    transientCores = transientCoresCacheBuilder.build();

    overflowCores = Caffeine.newBuilder().initialCapacity(initialCapacity / 2).build();

    transientDescriptors = new LinkedHashMap<>(initialCapacity);
  }

  private void onEvict(SolrCore core) {
    final SolrCores solrCores = coreContainer.solrCores;

    solrCores.getWriteLock().lock();
    try {
      // note: the cache's maximum size isn't strictly enforced; it can grow some if we un-evict
      if (solrCores.hasPendingCoreOps(core.getName())) {
        // core is loading, unloading, or reloading
        if (log.isInfoEnabled()) {
          log.info(
              "NOT evicting transient core [{}]; it's loading or something else.  transientCores size: {} overflowCores size: {}",
              core.getName(),
              transientCores.estimatedSize(),
              overflowCores.estimatedSize());
        }

        // we want to get informed when this core closes to do the maintenance of our overflow cache
        core.addCloseHook(new OverflowCacheMaintenanceHook());
        // add core to overflow cache as it still has pending ops
        overflowCores.put(core.getName(), core);

      } else if (core.getOpenCount() > 1) {

        // maybe a *long* running operation is happening or intense load
        if (log.isInfoEnabled()) {
          log.info(
              "NOT evicting transient core [{}]; it's still in use.  transientCores size: {} overflowCores size: {}",
              core.getName(),
              transientCores.estimatedSize(),
              overflowCores.estimatedSize());
        }

        // we want to get informed when this core closes to do the maintenance of our overflow cache
        core.addCloseHook(new OverflowCacheMaintenanceHook());
        // add core to overflow cache as it still has references to it
        overflowCores.put(core.getName(), core);

      } else {
        // common case -- can evict it
        if (log.isInfoEnabled()) {
          log.info("Closing transient core [{}] evicted from the cache", core.getName());
        }
        solrCores.queueCoreToClose(core);
      }
    } finally {
      solrCores.getWriteLock().unlock();
      ;
    }
  }

  private int getConfiguredCacheMaxSize(CoreContainer container) {
    int configuredCacheMaxSize = NodeConfig.NodeConfigBuilder.DEFAULT_TRANSIENT_CACHE_SIZE;
    NodeConfig cfg = container.getNodeConfig();
    if (cfg.getTransientCachePluginInfo() == null) {
      // Still handle just having transientCacheSize defined in the body of solr.xml
      // not in a transient handler clause.
      configuredCacheMaxSize = cfg.getTransientCacheSize();
    } else {
      NamedList<?> args = cfg.getTransientCachePluginInfo().initArgs;
      Object obj = args.get("transientCacheSize");
      if (obj != null) {
        configuredCacheMaxSize = (int) obj;
      }
    }
    if (configuredCacheMaxSize < 0) { // Trap old flag
      configuredCacheMaxSize = Integer.MAX_VALUE;
    }
    return configuredCacheMaxSize;
  }

  @Override
  public Collection<SolrCore> prepareForShutdown() {
    // Return a copy of the values.
    List<SolrCore> ret = new ArrayList<>(transientCores.asMap().values());
    ret.addAll(overflowCores.asMap().values());

    transientCores.invalidateAll();
    transientCores.cleanUp();

    overflowCores.invalidateAll();
    overflowCores.cleanUp();

    return ret;
  }

  @Override
  public CoreContainer getContainer() {
    return coreContainer;
  }

  @Override
  public SolrCore addCore(String name, SolrCore core) {
    return transientCores.asMap().put(name, core);
  }

  @Override
  public Set<String> getAllCoreNames() {
    return Collections.unmodifiableSet(transientDescriptors.keySet());
  }

  @Override
  public Set<String> getLoadedCoreNames() {
    return Collections.unmodifiableSet(
        Sets.union(transientCores.asMap().keySet(), overflowCores.asMap().keySet()));
  }

  @Override
  public SolrCore removeCore(String name) {
    SolrCore core = transientCores.asMap().remove(name);
    SolrCore overflowCore = overflowCores.asMap().remove(name);
    if (core == null) {
      return overflowCore;
    }

    return core;
  }

  @Override
  public SolrCore getCore(String name) {
    if (name == null) {
      return null;
    }

    SolrCore core = transientCores.getIfPresent(name);

    if (core == null) {
      return overflowCores.getIfPresent(name);
    }

    return core;
  }

  @Override
  public boolean containsCore(String name) {
    return name != null
        && (transientCores.asMap().containsKey(name) || overflowCores.asMap().containsKey(name));
  }

  @Override
  public void addTransientDescriptor(String rawName, CoreDescriptor cd) {
    transientDescriptors.put(rawName, cd);
  }

  @Override
  public CoreDescriptor getTransientDescriptor(String name) {
    return transientDescriptors.get(name);
  }

  @Override
  public Collection<CoreDescriptor> getTransientDescriptors() {
    return Collections.unmodifiableCollection(transientDescriptors.values());
  }

  @Override
  public CoreDescriptor removeTransientDescriptor(String name) {
    return transientDescriptors.remove(name);
  }

  @Override
  public int getStatus(String coreName) {
    // no_op for default handler.
    return 0;
  }

  @Override
  public void setStatus(String coreName, int status) {
    // no_op for default handler.
  }

  private class OverflowCacheMaintenanceHook implements CloseHook {
    @Override
    public void postClose(SolrCore core) {
      overflowCores.invalidate(core.getName());
    }
  }
}
