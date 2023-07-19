package com.linkedin.kafka.cruisecontrol.analyzer;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.SystemTime;
import org.easymock.EasyMock;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

public class ThesisTest {


  @Test
  public void testDoThis() throws Exception {
    for(int seed = 0; seed < 100; seed++) {
      var random = new Random(seed);
      var clusterModel = createModel(BalancingProblemGenerator.generate(300, random));
      rebalance(clusterModel);
    }
  }

  private void rebalance(ClusterModel clusterModel) throws Exception {
    // Sorted by priority.
    List<String> goalNameByPriority = Arrays.asList(
        NetworkInboundUsageDistributionGoal.class.getName(),
        NetworkOutboundUsageDistributionGoal.class.getName()
    );

    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.FAST_MODE_PER_BROKER_MOVE_TIMEOUT_MS_CONFIG, "1000000");
    props.setProperty(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(30000L));

    BalancingConstraint balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    balancingConstraint.setResourceBalancePercentage(1.0001);
    balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);

    var goals = goals(goalNameByPriority, balancingConstraint);
    var goalOptimizer = new GoalOptimizer(new KafkaCruiseControlConfig(balancingConstraint.setProps(props)),
        null,
        new SystemTime(),
        new MetricRegistry(),
        EasyMock.mock(Executor.class),
        EasyMock.mock(AdminClient.class));
    var result = goalOptimizer.optimizations(clusterModel, goals, new OperationProgress());

    var netInBefore = result.brokerStatsBeforeOptimization()._brokerStats.stream()
        .mapToDouble(x ->
            (double) x.getJsonStructure().get("LeaderNwInRate") +
            (double) x.getJsonStructure().get("FollowerNwInRate"))
        .summaryStatistics();
    var netOutBefore = result.brokerStatsBeforeOptimization()._brokerStats.stream()
        .map(x -> x.getJsonStructure().get("NwOutRate"))
        .mapToDouble(x -> (double) x)
        .summaryStatistics();
    var replicaBefore = result.brokerStatsBeforeOptimization()._brokerStats.stream()
        .map(x -> x.getJsonStructure().get("Replicas"))
        .mapToInt(x -> (int) x)
        .summaryStatistics();

    var netInAfter = result.brokerStatsAfterOptimization()._brokerStats.stream()
        .mapToDouble(x ->
            (double) x.getJsonStructure().get("LeaderNwInRate") +
            (double) x.getJsonStructure().get("FollowerNwInRate"))
        .summaryStatistics();
    var netOutAfter = result.brokerStatsAfterOptimization()._brokerStats.stream()
        .map(x -> x.getJsonStructure().get("NwOutRate"))
        .mapToDouble(x -> (double) x)
        .summaryStatistics();
    var replicaAfter = result.brokerStatsAfterOptimization()._brokerStats.stream()
        .map(x -> x.getJsonStructure().get("Replicas"))
        .mapToInt(x -> (int) x)
        .summaryStatistics();

    long bin = (long)(netInBefore.getMax() - netInBefore.getMin()) * 1000;
    long bout = (long)(netOutBefore.getMax() - netOutBefore.getMin()) * 1000;
    long breplica = (long)(replicaBefore.getMax() - replicaBefore.getMin());

    long in = (long)(netInAfter.getMax() - netInAfter.getMin()) * 1000;
    long out = (long)(netOutAfter.getMax() - netOutAfter.getMin()) * 1000;
    long replica = (long)(replicaAfter.getMax() - replicaAfter.getMin());

    System.out.printf("%15d %15d %5d %n", (long) netInAfter.getSum() * 1000, (long) netOutAfter.getSum() * 1000, netInBefore.getCount());
    System.out.printf("%15d %15d %5d %n", bin, bout, breplica);
    System.out.printf("%15d %15d %5d %n", in, out, replica);
    System.out.println("NetIn : " + NetworkInboundUsageDistributionGoal.acceptCount.sumThenReset() + "/" + NetworkInboundUsageDistributionGoal.rejectionCount.sumThenReset());
    System.out.println("NetOut : " + NetworkOutboundUsageDistributionGoal.acceptCount.sumThenReset() + "/" + NetworkOutboundUsageDistributionGoal.rejectionCount.sumThenReset());
  }

  public static List<Goal> goals(List<String> goalClasses, BalancingConstraint constraint) {
    return goalClasses.stream()
        .map(className -> {
          try {
            Class<? extends Goal> goalClass = (Class<? extends Goal>) Class.forName(className);
            try {
              Constructor<? extends Goal> constructor = goalClass.getDeclaredConstructor(BalancingConstraint.class);
              constructor.setAccessible(true);
              return constructor.newInstance(constraint);
            } catch (NoSuchMethodException badConstructor) {
              //Try default constructor
              return goalClass.newInstance();
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toList());
  }

  public static ClusterModel createModel(BalancingProblemGenerator.BalancingProblem statement) {
    ClusterModel cluster = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    cluster.createRack("");

    // create brokers
    statement.brokerIds.forEach(id ->
        cluster.createBroker("",
            Integer.toString(id),
            id,
            new BrokerCapacityInfo(Map.ofEntries(
                Map.entry(Resource.CPU, 64.0),
                Map.entry(Resource.DISK, 3e15),
                Map.entry(Resource.NW_IN, 3e12),
                Map.entry(Resource.NW_OUT, 3e12))),
            false));

    cluster.createBroker("", "1000", 1000,
        new BrokerCapacityInfo(Map.ofEntries(
            Map.entry(Resource.CPU, 64.0),
            Map.entry(Resource.DISK, 3e15),
            Map.entry(Resource.NW_IN, 3e12),
            Map.entry(Resource.NW_OUT, 3e12))),
        false);

    // create topics
    statement.replicaPosition.forEach((tp, list) -> {
      var theTp = new TopicPartition(
          tp.substring(0, tp.lastIndexOf('-')),
          Integer.parseInt(tp.substring(tp.lastIndexOf('-') + 1)));
      for(int i = 0; i < list.size(); i++) {
        var isLeader = i == 0;
        var metrics = new AggregatedMetricValues();
        KafkaCruiseControlUnitTestUtils.setValueForResource(metrics, Resource.CPU, 1);
        KafkaCruiseControlUnitTestUtils.setValueForResource(metrics, Resource.DISK,
            100 * statement.partitionNetIn.get(tp));
        KafkaCruiseControlUnitTestUtils.setValueForResource(metrics, Resource.NW_IN,
            statement.partitionNetIn.get(tp));
        KafkaCruiseControlUnitTestUtils.setValueForResource(metrics, Resource.NW_OUT,
            isLeader ? statement.partitionNetOut.get(tp) : 0);
        cluster.createReplica("", list.get(i), theTp, i, isLeader);
        cluster.setReplicaLoad("", list.get(i), theTp, metrics, List.of(1L));
      }
    });

    return cluster;
  }
}
