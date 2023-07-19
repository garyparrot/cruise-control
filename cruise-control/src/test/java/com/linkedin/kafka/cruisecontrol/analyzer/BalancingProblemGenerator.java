package com.linkedin.kafka.cruisecontrol.analyzer;

import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.Well19937c;
import org.apache.commons.math3.util.Pair;
import scala.collection.immutable.HashCollisionMapNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class BalancingProblemGenerator {

  public static void main(String[] args) {
    // BalancingProblem generate = generate(100, new Random());
    ZipfDistribution zipfDistribution = new ZipfDistribution(12, 1.5);
    for(int i =0 ; i < 20; i++)
      System.out.println(i + ": " + (zipfDistribution.cumulativeProbability(i + 1) - zipfDistribution.cumulativeProbability(i)));
    System.out.println("YES");
  }

  public static BalancingProblem generate(int topicCount, Random random) {
    var normalRate = new UniformIntegerDistribution(new Well19937c(random.nextInt()), 40_000, 50_000);
    var backboneRate = new UniformIntegerDistribution(new Well19937c(random.nextInt()), 50_000, 300_000);
    var manyConsumerRate = new UniformIntegerDistribution(new Well19937c(random.nextInt()), 5_000, 10_000);
    var fixedPartitions = 30;
    var topicStyle = new EnumeratedDistribution<>(new Well19937c(random.nextInt()), List.of(
        Pair.create("Normal", 0.1),
        Pair.create("Backbone", 0.25),
        Pair.create("ManyConsumer", 0.25)));
    var writeStyle = new EnumeratedDistribution<>(new Well19937c(random.nextInt()), List.of(
        Pair.create("Even", 0.2),
        Pair.create("NotSoEven", 0.5),
        Pair.create("NotEven", 0.3)));
    var brokerIds = IntStream.range(0, 15 + random.nextInt(5))
        .boxed()
        .collect(Collectors.toList());
    var index = new AtomicInteger();
    var topics = IntStream.range(0, topicCount)
        .mapToObj(i -> topicStyle.sample() + "_" + index.incrementAndGet())
        .collect(Collectors.toList());
    var partitions = topics.stream()
        .collect(Collectors.toUnmodifiableMap(
            t -> t,
            t -> fixedPartitions));
    var topicNetIn = topics.stream()
        .collect(Collectors.toUnmodifiableMap(
            t -> t,
            t -> {
              if(t.startsWith("Normal"))
                return normalRate.sample();
              else if(t.startsWith("Backbone"))
                return backboneRate.sample();
              else if(t.startsWith("ManyConsumer"))
                return manyConsumerRate.sample();
              else
                throw new IllegalArgumentException();
            }));
    var partitionNetIn = topics.stream()
        .map(topic -> {
          var s = writeStyle.sample();
          var tpWeight = IntStream.range(0, partitions.get(topic))
              .boxed()
              .collect(Collectors.toUnmodifiableMap(
                  p -> topic + "-" + p,
                  p -> {
                    if(s.equals("Even"))
                      return 1.0;
                    else if(s.equals("NotSoEven"))
                      return 50.0 + random.nextInt(-10, 10);
                    else if(s.equals("NotEven")) {
                      var zipf = new ZipfDistribution(partitions.get(topic), 1.0);
                      return zipf.cumulativeProbability(p + 1) - zipf.cumulativeProbability(p);
                    }
                    else
                      throw new IllegalArgumentException();
                  }));

          var sum = tpWeight.values().stream()
              .mapToDouble(x -> x)
              .sum();

          return tpWeight.entrySet()
              .stream()
              .collect(Collectors.toUnmodifiableMap(
                  Map.Entry::getKey,
                  e -> (double)topicNetIn.get(e.getKey().split("-")[0]) * e.getValue() / sum
              ));
        })
        .flatMap(x -> x.entrySet().stream())
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, x -> x.getValue().longValue()));

    var partitionNetOut = partitionNetIn.entrySet().stream()
        .map(e -> {
          var tp = e.getKey();
          var rate = e.getValue();
          var baseRandom = new Random(tp.split("-")[0].hashCode());
          var fanout = tp.startsWith("ManyConsumer") ?
              baseRandom.nextInt(7, 15) :
              baseRandom.nextInt(1, 3);

          return Map.entry((String) tp, (Long) (rate * fanout));
        })
        .collect(Collectors.toUnmodifiableMap(
            Map.Entry::getKey,
            Map.Entry::getValue));

    var replicas = topics.stream()
        .collect(Collectors.toUnmodifiableMap(
            x -> x,
            x -> {
              if(x.startsWith("Backbone"))
                return (short) 1;
              else
                return (short) 2;
            }));

    var allocationRandom = new Random(random.nextInt());
    var brokerArray = brokerIds.stream().mapToInt(x -> x).toArray();
    var position = topics.stream()
        .flatMap(topic -> {
          var partitionCount = partitions.get(topic);
          var replicaFactor = replicas.get(topic);
          var allocation = kafkaAssignReplicasToBrokersRackUnaware(
              allocationRandom,
              partitionCount,
              replicaFactor,
              brokerArray,
              -1,
              -1);

          return allocation.entrySet()
              .stream()
              .map(e -> Map.entry(topic + "-" + e.getKey(), e.getValue()));
        })
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

    var problem = new BalancingProblem();
    problem.brokerIds = brokerIds;
    problem.topics = topics;
    problem.topicPartitionCount = partitions;
    problem.partitionNetIn = partitionNetIn;
    problem.partitionNetOut = partitionNetOut;
    problem.replicaFactor = replicas;
    problem.replicaPosition = position;
    return problem;
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
      var replicaBuffer = new ArrayList<>(List.of(firstReplicaIndex));
      for(int j = 0; j < replicationFactor - 1; j++) {
        replicaBuffer.add(brokerArray[replicaIndex(
            firstReplicaIndex,
            nextReplicaShift,
            j,
            brokerArray.length)]);
        ret.put(currentPartitionId, replicaBuffer);
        currentPartitionId += 1;
      }
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

  public static class BalancingProblem {
    List<Integer> brokerIds;
    List<String> topics;
    Map<String, Integer> topicPartitionCount;
    Map<String, Long> partitionNetIn;
    Map<String, Long> partitionNetOut;
    Map<String, Short> replicaFactor;
    Map<String, List<Integer>> replicaPosition;
  }
}
