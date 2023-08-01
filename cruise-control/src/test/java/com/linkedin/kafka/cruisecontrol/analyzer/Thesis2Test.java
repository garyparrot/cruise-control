package com.linkedin.kafka.cruisecontrol.analyzer;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.Well19937c;
import org.apache.commons.math3.util.Pair;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.SystemTime;
import org.easymock.EasyMock;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.DoubleSummaryStatistics;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Thesis2Test {

  static final List<String> defaultGoals = List.of(
      // "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal",
      "com.linkedin.kafka.cruisecontrol.analyzer.goals.MinTopicLeadersPerBrokerGoal",
      "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal",
      "com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal",
      "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal",
      "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal",
      "com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal",
      "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal",
      "com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal",
      "com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal",
      "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal",
      "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal",
      "com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal",
      "com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal",
      "com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderReplicaDistributionGoal",
      "com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoal");

  static final Function<Random, IntegerDistribution> PARTITION_FIXED_10 =
      (random) -> new UniformIntegerDistribution(new Well19937c(random.nextInt()), 10, 10);
  static final Function<Random, IntegerDistribution> PARTITION_FIXED_30 =
      (random) -> new UniformIntegerDistribution(new Well19937c(random.nextInt()), 30, 30);
  static final Function<Random, IntegerDistribution> PARTITION_FIXED_1 =
      (random) -> new UniformIntegerDistribution(new Well19937c(random.nextInt()), 1, 1);
  static final BiFunction<Integer, Integer, Function<Random, IntegerDistribution>> PARTITION_UNIFORM_LOWER_UPPER =
      (lower, upper) ->
        (random) ->
            new UniformIntegerDistribution(
                new Well19937c(random.nextInt()),
                lower,
                upper);

  static final Function<Random, IntegerDistribution> REPLICA_FIXED_1 =
      (random) -> new UniformIntegerDistribution(new Well19937c(random.nextInt()), 1, 1);
  static final Function<Random, IntegerDistribution> REPLICA_FIXED_2 =
      (random) -> new UniformIntegerDistribution(new Well19937c(random.nextInt()), 2, 2);
  static final Function<Random, IntegerDistribution> REPLICA_FIXED_3 =
      (random) -> new UniformIntegerDistribution(new Well19937c(random.nextInt()), 3, 3);

  static final Function<Random, IntegerDistribution> CONSUMER_FANOUT_FIXED_1 =
      (random) -> new UniformIntegerDistribution(new Well19937c(random.nextInt()), 1, 1);
  static final Function<Random, IntegerDistribution> CONSUMER_FANOUT_UNIFORM_3 =
      (random) -> new UniformIntegerDistribution(new Well19937c(random.nextInt()), 1, 3);
  static final BiFunction<Integer, Integer, Function<Random, IntegerDistribution>>
      CONSUMER_FANOUT_UNIFORM_LOWER_UPPER =
      (lower, upper) ->
          (random) ->
              new UniformIntegerDistribution(
                  new Well19937c(random.nextInt()),
                  lower,
                  upper);

  static final Function<Random, IntegerDistribution> THROUGHPUT_UNIFORM_10KB_10MB=
      (random) -> new UniformIntegerDistribution(new Well19937c(random.nextInt()), 10_000, 10_000_000);
  static final BiFunction<Integer, Integer, Function<Random, IntegerDistribution>>
      THROUGHPUT_UNIFORM_LOWER_UPPER =
      (lower, upper) ->
        (random) ->
            new UniformIntegerDistribution(
                new Well19937c(random.nextInt()),
                lower,
                upper);

  static final Function<Random, Function<Integer, double[]>> WRITE_WEIGHT_UNIFORM =
      (random) -> (partitions) -> IntStream.range(0, partitions)
          .mapToDouble(x -> 1.0)
          .toArray();
  static final Function<Random, Function<Integer, double[]>> WRITE_WEIGHT_NOT_SO_EVEN =
      (random) -> {
        var myRandom = new Well19937c(random.nextInt());
        return (partitions) -> IntStream.range(0, partitions)
            .mapToDouble(x -> (0.5 + myRandom.nextDouble()))
            .toArray();
      };
  static final Function<Random, Function<Integer, double[]>> WRITE_WEIGHT_ZIPF_1 =
      (random) -> (partitions) -> {
        var zipf = new ZipfDistribution(partitions, 1.0);
        return IntStream.range(0, partitions)
            .mapToDouble(i -> zipf.cumulativeProbability(i + 1) - zipf.cumulativeProbability(i))
            .toArray();
      };
  static final Function<Random, Function<Integer, double[]>> WRITE_WEIGHT_VARIOUS =
      (random) -> {
        var myRandom = new Well19937c(random.nextInt());
        var even = (Function<Integer, double[]>) (p) -> IntStream.range(0, p)
            .mapToDouble(x -> 1.0)
            .toArray();
        var notSoEven = (Function<Integer, double[]>) (p) -> Arrays.stream(even.apply(p))
            .map(x -> x * (myRandom.nextDouble() / 0.4 + 0.8))
            .toArray();
        var zipf = WRITE_WEIGHT_ZIPF_1.apply(random);

        var selection = new EnumeratedDistribution<>(
            new Well19937c(random.nextInt()),
            List.of(
                Pair.create(even, 0.3),
                Pair.create(notSoEven, 0.5),
                Pair.create(zipf, 0.2)));
        return (partitions) -> selection.sample().apply(partitions);
      };

  @Test
  public void testHell() throws Exception {
    var netIn = new ArrayList<Double>();
    var netOut = new ArrayList<Double>();
    for(int i = 0;i < 30; i++) {
      System.out.println("TEST SEED: " + i);
      // Optimization
      var problem = thesis(i);
      var cluster = problem.cluster;
      var model = createModel(cluster);
      var report = rebalance(model, problem.balancingRatio, problem.prioritizedGoals);

      netIn.add((report.afterNetIn.getMax() - report.afterNetIn.getMin()) / 1e6);
      netOut.add((report.afterNetOut.getMax() - report.afterNetOut.getMin()) / 1e6);

      // Output
      System.out.printf("%15d %15d %5d %n",
          report.diffNetInBefore(),
          report.diffNetOutBefore(),
          report.diffReplicaCountBefore());
      System.out.printf("%15d %15d %5d %n",
          report.diffNetIn(),
          report.diffNetOut(),
          report.diffReplicaCount());
      System.out.printf("%15.1f %15.1f %15.1f %n",
          (report.afterNetOut.getAverage() - report.afterNetOut.getMin()) / 1_000_000,
          (report.afterNetOut.getMax() - report.afterNetOut.getAverage()) / 1_000_000,
          (report.afterNetOut.getAverage()) / 1_000_000);
      System.out.println("NetIn : " + report.optimizationNetInAccepts + "/" + report.optimizationNetInRejects);
      System.out.println("NetOut : " + report.optimizationNetOutAccepts + "/" + report.optimizationNetOutRejects);
    }
    System.out.println(netIn);
    System.out.println(netOut);
  }

  public Problem thesis(int seed) {
    var cluster = new BalancingProblem(seed)
        .defineWorkload(0.6, BalancingProblem.TopicWorkloads.workload(
            "Normal",
            PARTITION_FIXED_10,
            REPLICA_FIXED_2,
            CONSUMER_FANOUT_UNIFORM_LOWER_UPPER.apply(1,2),
            THROUGHPUT_UNIFORM_LOWER_UPPER.apply(4_000_000, 5_000_000),
            WRITE_WEIGHT_VARIOUS))
        .defineWorkload(0.15, BalancingProblem.TopicWorkloads.workload(
            "Backbone",
            PARTITION_FIXED_10,
            REPLICA_FIXED_1,
            CONSUMER_FANOUT_UNIFORM_LOWER_UPPER.apply(1,2),
            THROUGHPUT_UNIFORM_LOWER_UPPER.apply(90_000_000, 100_000_000),
            WRITE_WEIGHT_VARIOUS))
        .defineWorkload(0.25, BalancingProblem.TopicWorkloads.workload(
            "ManyConsumer",
            PARTITION_FIXED_10,
            REPLICA_FIXED_2,
            CONSUMER_FANOUT_UNIFORM_LOWER_UPPER.apply(4, 5),
            THROUGHPUT_UNIFORM_LOWER_UPPER.apply(800_000, 1_000_000),
            WRITE_WEIGHT_VARIOUS))
        .addBrokers((random) -> IntStream.range(0, 3 + random.nextInt(12))
            .boxed()
            .map(i -> new BalancingProblem.Broker(i, 2_250_000_000L))
            .collect(Collectors.toList()))
        .addTopics(300)
        .addBrokers(List.of(
                 new BalancingProblem.Broker(10000, 2_250_000_000L)))
        .cluster();

    return new Problem(
        cluster,
        1.001,
        List.of(
            NetworkInboundUsageDistributionGoal.class.getName(),
            NetworkOutboundUsageDistributionGoal.class.getName()
        ));
  }

  /**
   * CC performs poorly when there are many brokers and fewer replicas.
   *
   * I don't know why
   */
  public Problem backboneZipf(int seed) {
    var cluster = new BalancingProblem(seed)
        .defineWorkload(0.5, BalancingProblem.TopicWorkloads.workload(
            "Normal",
            PARTITION_FIXED_30,
            REPLICA_FIXED_2,
            CONSUMER_FANOUT_UNIFORM_3,
            THROUGHPUT_UNIFORM_LOWER_UPPER.apply(1_000_000, 20_000_000),
            WRITE_WEIGHT_VARIOUS))
        .defineWorkload(0.03, BalancingProblem.TopicWorkloads.workload(
            "Backbone",
            PARTITION_FIXED_30,
            REPLICA_FIXED_1,
            CONSUMER_FANOUT_FIXED_1,
            THROUGHPUT_UNIFORM_LOWER_UPPER.apply(300_000_000, 700_000_000),
            WRITE_WEIGHT_ZIPF_1))
        .defineWorkload(0.25, BalancingProblem.TopicWorkloads.workload(
            "ManyConsumer",
            PARTITION_FIXED_30,
            REPLICA_FIXED_3,
            CONSUMER_FANOUT_UNIFORM_LOWER_UPPER.apply(7, 13),
            THROUGHPUT_UNIFORM_LOWER_UPPER.apply(1_000_000, 10_000_000),
            WRITE_WEIGHT_VARIOUS))
        .addBrokers((random) -> IntStream.range(0, 55 + random.nextInt(5))
            .boxed()
            .map(i -> new BalancingProblem.Broker(i, 1_250_000_000))
            .collect(Collectors.toList()))
        .addTopics(100)
        .cluster();

    return new Problem(
        cluster,
        1.0001,
        List.of(
            ReplicaDistributionGoal.class.getName(),
            NetworkInboundUsageDistributionGoal.class.getName(),
            NetworkOutboundUsageDistributionGoal.class.getName()));
  }

  public Problem manyBrokerLessReplica(int seed) {
    var cluster = new BalancingProblem(seed)
        .addBrokers((random) -> IntStream.range(0, 10 + random.nextInt(5))
            .boxed()
            .map(i -> new BalancingProblem.Broker(i, 1_250_000_000))
            .collect(Collectors.toList()))
        .defineWorkload(1, BalancingProblem.TopicWorkloads.workload(
            "Backbone",
            PARTITION_FIXED_30,
            REPLICA_FIXED_1,
            CONSUMER_FANOUT_FIXED_1,
            THROUGHPUT_UNIFORM_LOWER_UPPER.apply(300_000_000, 700_000_000),
            WRITE_WEIGHT_ZIPF_1))
        .addTopics(1)
        .removeWorkload(0)
        .defineWorkload(0.5, BalancingProblem.TopicWorkloads.workload(
            "Normal",
            PARTITION_FIXED_30,
            REPLICA_FIXED_2,
            CONSUMER_FANOUT_UNIFORM_3,
            THROUGHPUT_UNIFORM_LOWER_UPPER.apply(1_000_000, 20_000_000),
            WRITE_WEIGHT_VARIOUS))
        .defineWorkload(0.25, BalancingProblem.TopicWorkloads.workload(
            "ManyConsumer",
            PARTITION_FIXED_30,
            REPLICA_FIXED_3,
            CONSUMER_FANOUT_UNIFORM_LOWER_UPPER.apply(7, 13),
            THROUGHPUT_UNIFORM_LOWER_UPPER.apply(1_000_000, 10_000_000),
            WRITE_WEIGHT_VARIOUS))
        .addTopics(30)
        .cluster();

    return new Problem(
        cluster,
        1.000001,
        List.of(
            ReplicaDistributionGoal.class.getName(),
            NetworkInboundUsageDistributionGoal.class.getName(),
            NetworkOutboundUsageDistributionGoal.class.getName()));
  }

  public static ClusterModel createModel(BalancingProblem.Cluster statement) {
    ClusterModel cluster = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    cluster.createRack("");

    // create brokers
    statement.brokers.forEach(broker -> cluster.createBroker("",
        Integer.toString(broker.brokerId),
        broker.brokerId,
        new BrokerCapacityInfo(Map.ofEntries(
            Map.entry(Resource.CPU, 64.0),
            Map.entry(Resource.DISK, 3e15),
            Map.entry(Resource.NW_IN, broker.networkBandwidth / 1000.0),
            Map.entry(Resource.NW_OUT, broker.networkBandwidth / 1000.0))),
        false));

    var stat = statement.brokers
        .stream()
        .collect(Collectors.toMap(b -> b.brokerId, b -> 0));

    // create topics
    statement.topics.forEach(topic -> {
      topic.replicaAssignments.forEach((part, replicas) -> {
        for(int i = 0; i < replicas.size(); i++) {
          var isLeader = i == 0;
          var metrics = new AggregatedMetricValues();
          KafkaCruiseControlUnitTestUtils.setValueForResource(metrics, Resource.CPU, 0);
          KafkaCruiseControlUnitTestUtils.setValueForResource(metrics, Resource.DISK, 0);
          KafkaCruiseControlUnitTestUtils.setValueForResource(metrics, Resource.NW_IN,
              topic.partitionWriteRate.get(part) / 1000.0);
          KafkaCruiseControlUnitTestUtils.setValueForResource(metrics, Resource.NW_OUT,
              isLeader ? topic.partitionReadRate.get(part) / 1000.0 : 0);
          var tp = new TopicPartition(topic.name, part);
          cluster.createReplica("", replicas.get(i), tp, i, isLeader);
          cluster.setReplicaLoad("", replicas.get(i), tp, metrics, List.of(1L));
          stat.compute(replicas.get(i), (k, v) -> v + 1);
        }
      });
    });

    // if broker has no replicas, set state to New .
    // this is how cc implementation detail expect user to handle this.
    stat.forEach((id, count) -> {
      if(count == 0)
        cluster.setBrokerState(id, Broker.State.NEW);
    });


    return cluster;
  }

  public static Report rebalance(ClusterModel clusterModel, double balancingPercentage, List<String> prioritizedGoals) {
    // configurations
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.FAST_MODE_PER_BROKER_MOVE_TIMEOUT_MS_CONFIG, "1000000");
    props.setProperty(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(30000L));
    BalancingConstraint balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    balancingConstraint.setResourceBalancePercentage(balancingPercentage);
    balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);

    var goals = goals(prioritizedGoals, balancingConstraint);
    var goalOptimizer = new GoalOptimizer(new KafkaCruiseControlConfig(balancingConstraint.setProps(props)),
        null,
        new SystemTime(),
        new MetricRegistry(),
        EasyMock.mock(Executor.class),
        EasyMock.mock(AdminClient.class));

    OptimizerResult result = null;
    try {
      result = goalOptimizer.optimizations(clusterModel, goals, new OperationProgress());
    } catch (KafkaCruiseControlException e) {
      throw new RuntimeException(e);
    }

    var netInBefore = result.brokerStatsBeforeOptimization()._brokerStats.stream()
        .mapToDouble(x ->
            (double) x.getJsonStructure().get("LeaderNwInRate") +
                (double) x.getJsonStructure().get("FollowerNwInRate"))
        .map(x -> x * 1000) // KB to Bytes
        .summaryStatistics();
    var netOutBefore = result.brokerStatsBeforeOptimization()._brokerStats.stream()
        .map(x -> x.getJsonStructure().get("NwOutRate"))
        .mapToDouble(x -> (double) x)
        .map(x -> x * 1000) // KB to Bytes
        .summaryStatistics();
    var replicaBefore = result.brokerStatsBeforeOptimization()._brokerStats.stream()
        .map(x -> x.getJsonStructure().get("Replicas"))
        .mapToInt(x -> (int) x)
        .summaryStatistics();

    var netInAfter = result.brokerStatsAfterOptimization()._brokerStats.stream()
        .mapToDouble(x ->
            (double) x.getJsonStructure().get("LeaderNwInRate") +
                (double) x.getJsonStructure().get("FollowerNwInRate"))
        .map(x -> x * 1000) // KB to Bytes
        .summaryStatistics();
    var netOutAfter = result.brokerStatsAfterOptimization()._brokerStats.stream()
        .map(x -> x.getJsonStructure().get("NwOutRate"))
        .mapToDouble(x -> (double) x)
        .map(x -> x * 1000) // KB to Bytes
        .summaryStatistics();
    var replicaAfter = result.brokerStatsAfterOptimization()._brokerStats.stream()
        .map(x -> x.getJsonStructure().get("Replicas"))
        .mapToInt(x -> (int) x)
        .summaryStatistics();

    var report = new Report();
    report.beforeNetIn = netInBefore;
    report.beforeNetOut = netOutBefore;
    report.beforeReplicaCount = replicaBefore;
    report.afterNetIn = netInAfter;
    report.afterNetOut = netOutAfter;
    report.afterReplicaCount = replicaAfter;
    report.optimizationNetInAccepts = NetworkInboundUsageDistributionGoal.acceptCount.sumThenReset();
    report.optimizationNetInRejects = NetworkInboundUsageDistributionGoal.rejectionCount.sumThenReset();
    report.optimizationNetOutAccepts = NetworkOutboundUsageDistributionGoal.acceptCount.sumThenReset();
    report.optimizationNetOutRejects = NetworkOutboundUsageDistributionGoal.rejectionCount.sumThenReset();
    return report;
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

  public static class Problem {
    public final BalancingProblem.Cluster cluster;
    public final double balancingRatio;
    public final List<String> prioritizedGoals;

    public Problem(BalancingProblem.Cluster cluster, double balancingRatio, List<String> prioritizedGoals) {
      this.cluster = cluster;
      this.balancingRatio = balancingRatio;
      this.prioritizedGoals = prioritizedGoals;
    }
  }

  public static class Report {
    public DoubleSummaryStatistics beforeNetIn;
    public DoubleSummaryStatistics beforeNetOut;
    public IntSummaryStatistics beforeReplicaCount;

    public DoubleSummaryStatistics afterNetIn;
    public DoubleSummaryStatistics afterNetOut;
    public IntSummaryStatistics afterReplicaCount;

    public long optimizationNetInAccepts;
    public long optimizationNetInRejects;

    public long optimizationNetOutAccepts;
    public long optimizationNetOutRejects;

    public long diffNetInBefore() {
      return (long)(beforeNetIn.getMax() - beforeNetIn.getMin());
    }

    public long diffNetOutBefore() {
      return (long)(beforeNetOut.getMax() - beforeNetOut.getMin());
    }

    public long diffReplicaCountBefore() {
      return (long)(beforeReplicaCount.getMax() - beforeReplicaCount.getMin());
    }

    public long diffNetIn() {
      return (long)(afterNetIn.getMax() - afterNetIn.getMin());
    }

    public long diffNetOut() {
      return (long)(afterNetOut.getMax() - afterNetOut.getMin());
    }

    public long diffReplicaCount() {
      return (long)(afterReplicaCount.getMax() - afterReplicaCount.getMin());
    }

  }
}
