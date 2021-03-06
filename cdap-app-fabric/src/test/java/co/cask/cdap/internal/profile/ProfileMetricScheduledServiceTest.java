/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.profile;

import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ProfileMetricScheduledServiceTest {
  private static Injector injector;

  @BeforeClass
  public static void setupClass() {
    injector = AppFabricTestHelper.getInjector();
  }

  @Test
  public void testProfileMetrics() throws Exception {
    ProgramRunId runId = NamespaceId.DEFAULT.app("myApp").workflow("myProgram").run(RunIds.generate());
    ProfileId profileId = NamespaceId.DEFAULT.profile("myProfile");
    MetricsCollectionService collectionService = injector.getInstance(MetricsCollectionService.class);
    MetricStore metricStore = injector.getInstance(MetricStore.class);

    // There are 5 nodes, we emit the metrics each 2 mins, so each time the node minute should go up by 10 min
    ProfileMetricScheduledService scheduledService = new ProfileMetricScheduledService(collectionService, runId,
                                                                                       profileId, 5, 2, null);
    // emit and verify the results
    scheduledService.emitMetric();
    Tasks.waitFor(10L, () -> getMetric(metricStore, runId, profileId,
                                      "system." + Constants.Metrics.Program.PROGRAM_NODE_MINUTES),
                  10, TimeUnit.SECONDS);
    scheduledService.emitMetric();
    Tasks.waitFor(20L, () -> getMetric(metricStore, runId, profileId,
                                      "system." + Constants.Metrics.Program.PROGRAM_NODE_MINUTES),
                  10, TimeUnit.SECONDS);
    scheduledService.emitMetric();
    Tasks.waitFor(30L, () -> getMetric(metricStore, runId, profileId,
                                      "system." + Constants.Metrics.Program.PROGRAM_NODE_MINUTES),
                  10, TimeUnit.SECONDS);
  }

  private long getMetric(MetricStore metricStore, ProgramRunId programRunId, ProfileId profileId, String metricName) {
    Map<String, String> tags = ImmutableMap.<String, String>builder()
      .put(Constants.Metrics.Tag.PROFILE_SCOPE, profileId.getScope().name())
      .put(Constants.Metrics.Tag.PROFILE, profileId.getScopedName())
      .put(Constants.Metrics.Tag.NAMESPACE, programRunId.getNamespace())
      .put(Constants.Metrics.Tag.PROGRAM_TYPE, programRunId.getType().getPrettyName())
      .put(Constants.Metrics.Tag.APP, programRunId.getApplication())
      .put(Constants.Metrics.Tag.PROGRAM, programRunId.getProgram())
      .build();

    MetricDataQuery query = new MetricDataQuery(0, 0, Integer.MAX_VALUE, metricName, AggregationFunction.SUM,
                                                tags, new ArrayList<>());
    Collection<MetricTimeSeries> result = metricStore.query(query);
    if (result.isEmpty()) {
      return 0;
    }
    List<TimeValue> timeValues = result.iterator().next().getTimeValues();
    if (timeValues.isEmpty()) {
      return 0;
    }
    return timeValues.get(0).getValue();
  }
}
