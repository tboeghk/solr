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

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.bench.BaseBenchState;
import org.apache.solr.logging.MDCLoggingContext;
import org.mockito.Mockito;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.MDC;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(Threads.MAX) // number of cores
@Warmup(time = 10, iterations = 3)
@Measurement(time = 20, iterations = 4)
@Fork(value = 1)
@Timeout(time = 60)
public class SolrCoresBenchTest {

    @State(Scope.Benchmark)
    public static class BenchState {

        private int cores = 10;
        private SolrCores solrCores;
        private SplittableRandom random;

        private Map<String,AtomicInteger> refcounts;
        public BenchState() {
        }

        @Setup(Level.Trial)
        public void doSetup() {
            random = new SplittableRandom(BaseBenchState.getRandomSeed());
            refcounts = new HashMap<>();

            NodeConfig config = Mockito.mock(NodeConfig.class);
            CoreContainer container = Mockito.mock(CoreContainer.class);
            Mockito.when(container.getConfig()).thenReturn(config);
            Mockito.when(container.getNodeConfig()).thenReturn(config);

            SolrResourceLoader loader = new SolrResourceLoader(Paths.get("build/work"));

            solrCores = new SolrCores(container);
            solrCores.load(loader);

            for (int i = 0; i < cores; i++) {
                CoreDescriptor cd = Mockito.mock(CoreDescriptor.class);
                Mockito.when(cd.getName()).thenReturn("core" + i);
                SolrCore core = Mockito.mock(SolrCore.class);
                refcounts.put("core" + i,new AtomicInteger(1));
                solrCores.putCore(cd, core);
            }

        }
    }

    @Benchmark
    @Timeout(time = 300)
    public Object getCoreFromAnyList(
            BenchState state) {
        int id = state.random.nextInt(state.cores);

        String core;
        if (id % 100000 == 0) { // requesting a non known core, nearly never happens.
            core = "NAN";
        } else {
            core = "core";
        }
        String name = core + id;
        return state.solrCores.getCoreFromAnyList(name, false, null, (c) -> {
            if(!name.startsWith("NAN")) {
                // do SolrCore.open()
                state.refcounts.get(name).incrementAndGet();
                // fake MDCLoggingContext.setCore(c); via same number of put calls
                for (int i = 0; i < 5; i++) {
                    MDC.put("core" + i, "Something Random" + i);
                }
                // do SolrCore.close()
                state.refcounts.get(name).decrementAndGet();
            }
        });
    }
}
