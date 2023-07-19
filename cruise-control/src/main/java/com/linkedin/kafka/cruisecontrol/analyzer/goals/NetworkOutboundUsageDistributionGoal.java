/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;

import java.util.concurrent.atomic.LongAdder;


public class NetworkOutboundUsageDistributionGoal extends ResourceDistributionGoal {

  /**
   * Constructor for Resource Distribution Goal.
   */
  public NetworkOutboundUsageDistributionGoal() {
    super();
  }

  /**
   * Package private for unit test.
   */
  NetworkOutboundUsageDistributionGoal(BalancingConstraint constraint) {
    super(constraint);
  }

  @Override
  protected Resource resource() {
    return Resource.NW_OUT;
  }

  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    var r = super.actionAcceptance(action, clusterModel);
    if(r != ActionAcceptance.ACCEPT)
      rejectionCount.increment();
    else
      acceptCount.increment();
    return r;
  }

  public static final LongAdder acceptCount = new LongAdder();
  public static final LongAdder rejectionCount = new LongAdder();
}
