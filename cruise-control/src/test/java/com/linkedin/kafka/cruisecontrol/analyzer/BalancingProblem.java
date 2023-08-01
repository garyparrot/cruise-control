package com.linkedin.kafka.cruisecontrol.analyzer;

import io.netty.resolver.dns.MultiDnsServerAddressStreamProvider;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.Well19937c;
import org.apache.commons.math3.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

// non thread safe
public class BalancingProblem {

  private final Random random;
  private final Cluster cluster;
  private final List<Pair<TopicWorkload, Double>> workloads;

  private EnumeratedDistribution<TopicWorkload> workloadSelector;

  public BalancingProblem(int randomSeed) {
    this.random = new Random(randomSeed);
    this.cluster = new Cluster(new ArrayList<>(), new ArrayList<>());
    this.workloads = new ArrayList<>();
  }

  /**
   * @param workloadGen a function that accept an int seed then return a topic workload generator.
   * @return this.
   */
  public BalancingProblem defineWorkload(double weight, Function<Random, TopicWorkload> workloadGen) {
    this.workloads.add(Pair.create(workloadGen.apply(random), weight));
    this.workloadSelector = new EnumeratedDistribution<>(new Well19937c(random.nextInt()), workloads);
    return this;
  }

  public BalancingProblem removeWorkload(int index) {
    this.workloads.remove(index);
    if(this.workloads.size() == 0)
      this.workloadSelector = null;
    else
      this.workloadSelector = new EnumeratedDistribution<>(new Well19937c(random.nextInt()), workloads);
    return this;
  }

  public BalancingProblem addBrokers(Function<Random, List<Broker>> useState) {
    this.cluster.brokers.addAll(useState.apply(random));
    return this;
  }

  public BalancingProblem addBrokers(List<Broker> broker) {
    this.cluster.brokers.addAll(broker);
    return this;
  }

  public BalancingProblem addTopics(int count) {
    for (int i = 0; i < count; i++) {
      var generatedTopic = this.workloadSelector.sample().apply(this.cluster);
      this.cluster.topics.add(generatedTopic);
    }
    return this;
  }

  public BalancingProblem addTopics(Function<Random, Integer> useState) {
    return this.addTopics(useState.apply(this.random));
  }

  /**
   * @return a copy of the current cluster
   */
  public Cluster cluster() {
    return new Cluster(
        List.copyOf(this.cluster.brokers),
        List.copyOf(this.cluster.topics));
  }

  public static class Cluster {
    public final List<Broker> brokers;
    public final List<Topic> topics;

    public Cluster(List<Broker> brokers, List<Topic> topics) {
      this.brokers = brokers;
      this.topics = topics;
    }
  }

  public static class Broker {
    public final int brokerId;
    public final long networkBandwidth;
    // TODO: add disk info if need to.

    public Broker(int brokerId, long networkBandwidth) {
      this.brokerId = brokerId;
      this.networkBandwidth = networkBandwidth;
    }
  }

  public static class Topic {
    public final String name;
    public final int partitionCount;
    public final List<Long> partitionWriteRate;
    public final List<Long> partitionReadRate;
    // TODO: add disk assignment info if need to.
    public final Map<Integer, List<Integer>> replicaAssignments;
    public final Map<String, Object> metadata = new HashMap<>();

    public Topic(String name, int partitionCount, List<Long> partitionWriteRate, List<Long> partitionReadRate, Map<Integer, List<Integer>> replicaAssignments) {
      this.name = name;
      this.partitionCount = partitionCount;
      this.partitionWriteRate = partitionWriteRate;
      this.partitionReadRate = partitionReadRate;
      this.replicaAssignments = replicaAssignments;
      if(replicaAssignments.keySet().size() != partitionCount)
        throw new IllegalArgumentException();
      if(IntStream.range(0, partitionCount).anyMatch(i -> !replicaAssignments.containsKey(i)))
        throw new IllegalArgumentException();
    }
  }

  public interface TopicWorkload extends Function<Cluster, Topic> {}

  public static final class TopicWorkloads {
    private TopicWorkloads() {}

    private static final AtomicInteger counter = new AtomicInteger();

    public static Function<Random, TopicWorkload> workload(
        String name,
        Function<Random, IntegerDistribution> randomPartitionDist,
        Function<Random, IntegerDistribution> randomReplicaDist,
        Function<Random, IntegerDistribution> randomConsumerFanoutDist,
        Function<Random, IntegerDistribution> randomWriteThroughputDist,
        Function<Random, Function<Integer, double[]>> randomPartitionWriteWeight) {
      return (random) -> {
        var workloadCounter = counter.incrementAndGet();
        var counter = new AtomicInteger();
        var myRandom = new Random(random.nextInt());
        var randomPartition = randomPartitionDist.apply(random);
        var randomReplica = randomReplicaDist.apply(random);
        var randomConsumerFanout = randomConsumerFanoutDist.apply(random);
        var randomWriteThroughput = randomWriteThroughputDist.apply(random);
        var randomWriteWeight = randomPartitionWriteWeight.apply(random);

        return (cluster) -> {
          // prepare name
          var topicName = name + "_" + workloadCounter + "_" + counter.incrementAndGet();
          // number of partitions
          var partitions = randomPartition.sample();
          // prepare write rate
          var topicWrite = randomWriteThroughput.sample();
          var writeWeight = randomWriteWeight.apply(partitions);
          var writeWeightSum = Arrays.stream(writeWeight).sum();
          var partitionWrite = Arrays.stream(writeWeight)
              .map(weight -> weight / writeWeightSum)
              .mapToLong(ratio -> (long) (ratio * topicWrite))
              .boxed()
              .collect(Collectors.toList());
          // prepare read rate
          var consumerGroups = randomConsumerFanout.sample();
          var partitionRead = partitionWrite.stream()
              .map(i -> i * consumerGroups)
              .collect(Collectors.toList());

          return new Topic(
              topicName,
              partitions,
              partitionWrite,
              partitionRead,
              BalancingProblem.kafkaAssignReplicasToBrokersRackUnaware(
                  myRandom,
                  partitions,
                  randomReplica.sample(),
                  cluster.brokers.stream().mapToInt(x -> x.brokerId).toArray(),
                  -1,
                  -1));
        };
      };
    }
  }

  /**
   * A clone of
   * https://github.com/apache/kafka/blob/d9b898b678158626bd2872bbfef883ca60a41c43/core/src/main/scala/kafka/admin/AdminUtils.scala#L125C32-L125C32
   */
  public static Map<Integer, List<Integer>> kafkaAssignReplicasToBrokersRackUnaware(
      Random random,
      int partitions,
      int replicationFactor,
      int[] brokers,
      int fixedStartIndex,
      int startPartitionId) {
    var ret = new HashMap<Integer, List<Integer>>();
    var brokerArray = brokers;
    var startIndex = (fixedStartIndex >= 0) ? fixedStartIndex : random.nextInt(brokerArray.length);
    var currentPartitionId = Math.max(0, startPartitionId);
    var nextReplicaShift = (fixedStartIndex >= 0) ? fixedStartIndex : random.nextInt(brokerArray.length);
    for(int i = 0; i < partitions; i++) {
      if(currentPartitionId > 0 && (currentPartitionId % brokerArray.length == 0))
        nextReplicaShift += 1;
      var firstReplicaIndex = (currentPartitionId + startIndex) % brokerArray.length;
      var replicaBuffer = new ArrayList<>(List.of(brokerArray[firstReplicaIndex]));
      for(int j = 0; j < replicationFactor - 1; j++) {
        replicaBuffer.add(brokerArray[replicaIndex(
            firstReplicaIndex,
            nextReplicaShift,
            j,
            brokerArray.length)]);
      }
      ret.put(currentPartitionId, replicaBuffer);
      currentPartitionId += 1;
    }
    return ret;
  }

  public static int replicaIndex(
      int firstReplicaIndex,
      int secondReplicaShift,
      int replicaIndex,
      int nBrokers) {
    var shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1);
    return (firstReplicaIndex + shift) % nBrokers;
  }

}
