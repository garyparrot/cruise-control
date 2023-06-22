package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerEvenRackAwareGoal;
import com.linkedin.kafka.cruisecontrol.common.ClusterProperty;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.ModuloBasedBrokerSetAssignmentPolicy;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.RandomCluster;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;

import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.BROKEN_BROKERS;
import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.GOAL_VIOLATION;
import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.NEW_BROKERS;
import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.REGRESSION;

public class ThesisTest {


  @Test
  public void testDoThis() throws Exception {
    for(int i = 0; i < 100; i++) {
      ClusterModel clusterModel = rebalance(i);
    }
  }

  private ClusterModel rebalance(int seed) throws Exception {
    // Sorted by priority.
    List<String> goalNameByPriority = Arrays.asList(
        // ReplicaDistributionGoal.class.getName(),
        NetworkInboundUsageDistributionGoal.class.getName(),
        NetworkOutboundUsageDistributionGoal.class.getName()
    );

    List<String> kafkaAssignerGoals = Arrays.asList(KafkaAssignerEvenRackAwareGoal.class.getName(),
        KafkaAssignerDiskUsageDistributionGoal.class.getName());

    List<OptimizationVerifier.Verification> verifications = Arrays.asList(NEW_BROKERS, BROKEN_BROKERS, REGRESSION);
    List<OptimizationVerifier.Verification> kafkaAssignerVerifications =
        Arrays.asList(GOAL_VIOLATION, BROKEN_BROKERS, REGRESSION);

    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(1500L));
    String brokerSetsDataFile = Objects.requireNonNull(KafkaCruiseControlUnitTestUtils.class.getClassLoader().getResource(
        TestConstants.BROKER_SET_RESOLVER_FILE_4)).getFile();
    props.setProperty(AnalyzerConfig.BROKER_SET_CONFIG_FILE_CONFIG, brokerSetsDataFile);
    props.setProperty(AnalyzerConfig.BROKER_SET_ASSIGNMENT_POLICY_CLASS_CONFIG, ModuloBasedBrokerSetAssignmentPolicy.class.getName());

    BalancingConstraint balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    // balancingConstraint.setResourceBalancePercentage(TestConstants.LOW_BALANCE_PERCENTAGE);
    // balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);

    // Test: Increase Replica Count
    props.setProperty(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(3000L));
    balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    // balancingConstraint.setResourceBalancePercentage(TestConstants.LOW_BALANCE_PERCENTAGE);
    balancingConstraint.setResourceBalancePercentage(1.00001);
    balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);

    // ClusterModel clusterModel = RandomCluster.generate(clusterProperties);
    // RandomCluster.populate(clusterModel, clusterProperties, TestConstants.Distribution.LINEAR);
    ClusterModel clusterModel = createModel(BalancingProblemGenerator.generate(300, new Random(seed)));
    Assertions.assertTrue(OptimizationVerifier.executeGoalsFor(balancingConstraint, clusterModel, goalNameByPriority, verifications), "Random Cluster Test failed to improve the existing state.");

    var beforeNetOutSummary = OptimizationVerifier.result.brokerStatsBeforeOptimization()._brokerStats.stream()
        .map(x -> x.getJsonStructure().get("NwOutRate"))
        .mapToDouble(x -> (double) x)
        .summaryStatistics();
    var beforeNetInSummary = OptimizationVerifier.result.brokerStatsBeforeOptimization()._brokerStats.stream()
        .mapToDouble(x ->
            (double) x.getJsonStructure().get("LeaderNwInRate") +
                (double) x.getJsonStructure().get("FollowerNwInRate"))
        .summaryStatistics();
    var netInSummary = OptimizationVerifier.result.brokerStatsAfterOptimization()._brokerStats.stream()
        .mapToDouble(x ->
            (double) x.getJsonStructure().get("LeaderNwInRate") +
            (double) x.getJsonStructure().get("FollowerNwInRate"))
        .summaryStatistics();
    var netOutSummary = OptimizationVerifier.result.brokerStatsAfterOptimization()._brokerStats.stream()
        .map(x -> x.getJsonStructure().get("NwOutRate"))
        .mapToDouble(x -> (double) x)
        .summaryStatistics();

    long bin = (long)(beforeNetInSummary.getMax() - beforeNetInSummary.getMin()) * 1000;
    long bout = (long)(beforeNetOutSummary.getMax() - beforeNetOutSummary.getMin()) * 1000;
    long in = (long)(netInSummary.getMax() - netInSummary.getMin()) * 1000;
    long out = (long)(netOutSummary.getMax() - netOutSummary.getMin()) * 1000;

    System.out.println(bin + " " + bout);
    System.out.println(in + " " + out);

    return clusterModel;
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
                Map.entry(Resource.CPU, 16.0),
                Map.entry(Resource.DISK, 3e6),
                Map.entry(Resource.NW_IN, 1e6),
                Map.entry(Resource.NW_OUT, 1e6))),
            false));

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
