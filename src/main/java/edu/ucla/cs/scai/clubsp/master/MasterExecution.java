/*
 * Copyright 2015 ScAi, CSD, UCLA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucla.cs.scai.clubsp.master;

import edu.ucla.cs.scai.clubsp.commons.BestSplitResult;
import edu.ucla.cs.scai.clubsp.commons.Range;
import edu.ucla.cs.scai.clubsp.commons.RegisteredWorker;
import edu.ucla.cs.scai.clubsp.commons.SplitResult;
import edu.ucla.cs.scai.clubsp.messages.ComputeBestSplitRequest;
import edu.ucla.cs.scai.clubsp.messages.ComputeValleyCriterionRequest;
import edu.ucla.cs.scai.clubsp.messages.FinalRefinementRequest;
import edu.ucla.cs.scai.clubsp.messages.InitRootRequest;
import edu.ucla.cs.scai.clubsp.messages.IntermediateRefinementRequest;
import edu.ucla.cs.scai.clubsp.messages.LoadDataSetRequest;
import edu.ucla.cs.scai.clubsp.messages.RestrictedCountRequest;
import edu.ucla.cs.scai.clubsp.messages.SendMarginalsRequest;
import edu.ucla.cs.scai.clubsp.messages.SplitRequest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;

/**
 *
 * @author Giuseppe M. Mazzeo <mazzeo@cs.ucla.edu>
 */
public class MasterExecution {

    final Master master;
    final ArrayList<String> workerIds = new ArrayList<>();
    final String executionId;
    final String dataSetId;
    final double scaleFactor;
    Range globalDomain;
    int receivedLocalDomains;
    int receivedRootInfo;
    PriorityQueue<MasterClusterBlock> splittingQueue = new PriorityQueue<>();
    MasterClusterBlock root;
    HashMap<Integer, MasterClusterBlock> blocks = new HashMap<>();
    LinkedList<MasterClusterBlock> clusters = new LinkedList<>();
    ArrayList<MasterClusterBlock> outliers = new ArrayList<>();
    double SSQ, SSQ0, SSQ0withoutOutliers;
    double deltaSSQTot = 0;
    double totalConditionsWeight = 2;
    double BCSSQ = 0;
    double chIndex = 0;
    double maxCHindex;
    HashMap<Integer, MarginalComputationExecutionPlan> marginalComputationExecutionPlans = new HashMap<>();
    int dimensionality;
    HashMap<Integer, HashMap<Integer, BestSplitResult>> bestSplitReceived = new HashMap<>();
    HashMap<Integer, Integer> valleyCriterionReceived = new HashMap<>();
    HashSet<Integer> valleyCriterionSatisfied = new HashSet<>();
    HashMap<Integer, Integer> receivedSplitInfo = new HashMap<>();
    HashMap<Integer, Integer> dimensionOfBestSplitReceived = new HashMap<>();
    private static final double radiusMultiplier = 3;
    ArrayList<Integer> noiseBlockCandidates = new ArrayList<>();
    int receivedRestrictedCounts = 0;
    int receivedIntermediateRefinement = 0;
    int receivedFinalRefinement = 0;
    Sequence sequence = new Sequence();
    int nLeaves = 1;

    public MasterExecution(final Master master, final String dataSetId, final double scaleFactor) {
        this.master = master;
        this.dataSetId = dataSetId;
        this.scaleFactor = scaleFactor;
        this.executionId = dataSetId + "_" + System.currentTimeMillis();
        this.workerIds.addAll(master.registeredWorkers.keySet());
        final HashMap<String, RegisteredWorker> currentWorkers = new HashMap<>(master.registeredWorkers);
        new Thread() {
            @Override
            public void run() {
                LoadDataSetRequest c = new LoadDataSetRequest(dataSetId, executionId, currentWorkers, scaleFactor);
                for (String workerId : workerIds) {
                    master.sendMessage(workerId, c);
                }
            }
        }.start();
    }

    public synchronized void increaseReceivedLocalDoamins(Range localDomain) {
        if (receivedLocalDomains == 0) {
            globalDomain = localDomain;
            dimensionality = localDomain.getDimensionality();
        } else {
            for (int i = 0; i < localDomain.inf.length; i++) {
                globalDomain.inf[i] = Math.min(globalDomain.inf[i], localDomain.inf[i]);
                globalDomain.sup[i] = Math.max(globalDomain.sup[i], localDomain.sup[i]);
            }
        }
        receivedLocalDomains++;
        if (receivedLocalDomains == workerIds.size()) {
            root = MasterClusterBlock.initRoot(globalDomain, 0);
            blocks.put(0, root);
            InitRootRequest c = new InitRootRequest(executionId, globalDomain);
            for (String workerId : workerIds) {
                master.sendMessage(workerId, c);
            }
        }
    }

    public synchronized void increaseReceivedRootInfo(int n, double[] LS, double[] SS) {
        receivedRootInfo++;
        root.addGlobalNLSSS(n, LS, SS, receivedRootInfo == 1);
        if (receivedRootInfo == workerIds.size()) {
            root.computeGlobalSSQ();
            SSQ0 = root.getGlobalSSQ();
            SSQ = SSQ0;
            splittingQueue.offer(root);
            startSplitNextEnqueuedBlock();
        }
    }

    void startSplitNextEnqueuedBlock() {
        final MasterClusterBlock block = splittingQueue.poll();
        if (workerIds.size() > 1) {
            final MarginalComputationExecutionPlan ep = new MarginalComputationExecutionPlan(dimensionality, workerIds);
            marginalComputationExecutionPlans.put(block.id, ep);
            new Thread() {
                @Override
                public void run() {
                    for (int i = 0; i < dimensionality; i++) {
                        final String[][] plan = ep.initComputation(i);
                        for (int k = 0; k < plan.length; k++) {
                            String s = plan[k][1];
                            String r = plan[k][0];
                            master.sendMessage(s, new SendMarginalsRequest(executionId, block.id, i, r));
                        }
                    }
                }

            }.start();
        } else { //request the best split for each dimension to the only worker
            new Thread() {
                @Override
                public void run() {
                    for (int i = 0; i < dimensionality; i++) {
                        master.sendMessage(workerIds.get(0), new ComputeBestSplitRequest(executionId, block.id, i, block.globalN, block.globalLS, block.globalSS));
                    }
                }
            }.start();
        }
    }

    public synchronized void increaseReceivedMarginals(final int blockId, final int dimension) {
        final MarginalComputationExecutionPlan ex = marginalComputationExecutionPlans.get(blockId);
        final MasterClusterBlock block = blocks.get(blockId);
        boolean allReceived = ex.decreasePairsLeft(dimension);
        if (allReceived) {
            if (ex.completedDimensions.contains(dimension)) {
                String workerId = ex.workerAllocations[dimension].get(0);
                master.sendMessage(workerId, new ComputeBestSplitRequest(executionId, blockId, dimension, block.globalN, block.globalLS, block.globalSS));
            } else {
                new Thread() {
                    @Override
                    public void run() {
                        String[][] plan = ex.initComputation(dimension);
                        for (int k = 0; k < plan.length; k++) {
                            String s = plan[k][1];
                            String r = plan[k][0];
                            master.sendMessage(s, new SendMarginalsRequest(executionId, blockId, dimension, r));
                        }
                    }
                }.start();
            }
        }
    }

    public synchronized void increaseReceivedBestSplits(int blockId, int dimension, BestSplitResult split) {

        HashMap<Integer, BestSplitResult> bestSplits = bestSplitReceived.get(blockId);
        if (bestSplits == null) {
            bestSplits = new HashMap<>();
            bestSplitReceived.put(blockId, bestSplits);
        }
        bestSplits.put(dimension, split);
        if (bestSplits.size() == dimensionality) {
            double maxSSQ = 0;
            for (Map.Entry<Integer, BestSplitResult> e : bestSplits.entrySet()) {
                if (e.getValue().getDeltaSSQ() > maxSSQ) {
                    maxSSQ = e.getValue().getDeltaSSQ();
                    dimensionOfBestSplitReceived.put(blockId, e.getKey());
                }
            }
            //now we need to check if the best split is effective
            double newBCSSQ = BCSSQ + maxSSQ;
            double newSSQ = SSQ - maxSSQ;
            double newCHindex = (newBCSSQ * (root.globalN - (nLeaves + 1))) / (newSSQ * nLeaves); //k=k-1+1

            if (nLeaves > 1 && newCHindex >= maxCHindex) { //do split                
                doSplit(blockId);
            } else if (newCHindex >= 0.7 * maxCHindex) { //compute valley criterion                
                for (int i = 0; i < marginalComputationExecutionPlans.get(blockId).workerAllocations.length; i++) {
                    String workerId = marginalComputationExecutionPlans.get(blockId).workerAllocations[i].get(0);
                    master.sendMessage(workerId, new ComputeValleyCriterionRequest(executionId, blockId, i, 0.1));
                }
            } else { //the split is not effective - a new block is extracted from the queue
                clusters.add(blocks.get(blockId));
                if (splittingQueue.isEmpty()) { //the divisive phase has ended, the intermediate refinement must be started
                    startIntermediateRefinement();
                } else { //extract next block from queue and start computing its best split
                    startSplitNextEnqueuedBlock();
                }
            }
        }
    }

    private void doSplit(int blockId) {
        //send the split request to the workerIds

        int bestDimension = dimensionOfBestSplitReceived.get(blockId);
        BestSplitResult bestSplit = bestSplitReceived.get(blockId).get(bestDimension);
        MasterClusterBlock block = blocks.get(blockId);
        int leftId = sequence.next();
        int rightId = sequence.next();
        nLeaves++;
        MasterClusterBlock[] newBlocks = block.split(bestDimension, bestSplit.getPosition(), leftId, rightId);
        blocks.put(leftId, newBlocks[0]);
        blocks.put(rightId, newBlocks[1]);
        for (String workerId : workerIds) {
            master.sendMessage(workerId, new SplitRequest(executionId, blockId, bestDimension, bestSplit.getPosition(), leftId, rightId, block.globalN, block.globalLS, block.globalSS));
        }
    }

    public synchronized void increaseReceivedValleyCriterion(int blockId, int dimension, boolean satisfied) {
        Integer alreadyReceived = valleyCriterionReceived.get(blockId);
        if (alreadyReceived == null) {
            valleyCriterionReceived.put(blockId, 1);
            alreadyReceived = 1;
        } else {
            valleyCriterionReceived.put(blockId, alreadyReceived + 1);
            if (valleyCriterionSatisfied.contains(blockId)) { //the criterion was already satisfied, therefore the split was already triggered
                return;
            }
            alreadyReceived++;
        }
        if (satisfied) {
            valleyCriterionSatisfied.add(blockId);
            doSplit(blockId);
        } else if (alreadyReceived == workerIds.size()) {
            clusters.add(blocks.get(blockId));
            if (splittingQueue.isEmpty()) {
                startIntermediateRefinement();
            } else {
                startSplitNextEnqueuedBlock();
            }
        }
    }

    public synchronized void increaseReceivedSplit(int blockId, SplitResult split) {
        Integer count = receivedSplitInfo.get(blockId);
        if (count == null) {
            count = 1;
        } else {
            count++;
        }
        receivedSplitInfo.put(blockId, count);
        MasterClusterBlock block = blocks.get(blockId);
        block.lc.addGlobalNLSSS(split.leftN, split.leftLS, split.leftSS, count == 1);
        block.rc.addGlobalNLSSS(split.rightN, split.rightLS, split.rightSS, count == 1);
        if (count == workerIds.size()) { //all the workerIds have replied
            block.lc.computeGlobalSSQ();
            block.rc.computeGlobalSSQ();
            updateCHindeAfterSplit(block);
            splittingQueue.offer(block.lc);
            splittingQueue.offer(block.rc);
            startSplitNextEnqueuedBlock();
        }
    }

    public synchronized void increaseReceivedRestrictedCount(ArrayList<Integer> blockIds, ArrayList<Integer> restrictedCounts) {
        for (int i = 0; i < blockIds.size(); i++) {
            MasterClusterBlock block = blocks.get(blockIds.get(i));
            block.restrictedCount += restrictedCounts.get(i);
        }
        receivedRestrictedCounts++;
        if (receivedRestrictedCounts == workerIds.size()) { //compute the actual noise-blocks
            HashSet<Integer> actualNoiseBlockIds = new HashSet<>();
            for (int blockId : blockIds) {
                MasterClusterBlock block = blocks.get(blockId);
                block.computeOutlierFlagBasedOnRestrictedDensity();
                if (block.isOutlierNode) {
                    actualNoiseBlockIds.add(blockId);
                }
            }
            for (Iterator<MasterClusterBlock> it = clusters.iterator(); it.hasNext();) {
                if (actualNoiseBlockIds.contains(it.next().id)) {
                    it.remove();
                }
            }
            sendIntermediateRefinementRequest();
        }
    }

    public synchronized void increaseReceivedIntermediateRefinement(ArrayList<Integer> blockIds, ArrayList<Integer> n, ArrayList<double[]> LS, ArrayList<double[]> SS) {
        receivedIntermediateRefinement++;
        for (int i = 0; i < blockIds.size(); i++) {
            MasterClusterBlock block = blocks.get(blockIds.get(i));
            block.addGlobalNLSSS(n.get(i), LS.get(i), SS.get(i), receivedIntermediateRefinement == 1);
        }
        if (receivedIntermediateRefinement == workerIds.size()) {
            startAgglomerativePhase();
        }
    }

    public void updateCHindeAfterSplit(MasterClusterBlock block) {
        double deltaSSQ = block.globalSSQ - block.lc.globalSSQ - block.rc.globalSSQ;
        block.actualDeltaSSQ = deltaSSQ;
        BCSSQ += deltaSSQ;
        SSQ -= deltaSSQ;
        chIndex = (BCSSQ * (root.globalN - nLeaves)) / (SSQ * (nLeaves - 1));
        if (chIndex > maxCHindex) {
            maxCHindex = chIndex;
        }

    }

    public void startIntermediateRefinement() {

        //first computes the noise-block candidates
        ArrayList<Double> densities = new ArrayList<>();

        for (MasterClusterBlock block : clusters) {
            double vol = block.r.getVolume();
            block.globalDensity = block.globalN / vol;
            densities.add(block.globalDensity);
            block.computeRestrictedRangeAndVolume();
        }

        Collections.sort(densities);
        double[] densityVariation = new double[densities.size() - 1];

        double ssd = 0;
        for (int i = 0; i < densityVariation.length; i++) {
            densityVariation[i] = densities.get(i + 1) / densities.get(i);
            ssd += densityVariation[i];
        }

        double asd = densityVariation.length > 0 ? ssd / densityVariation.length : 0;
        double dsd = 0;
        for (int i = 0; i < densityVariation.length; i++) {
            dsd += Math.pow(densityVariation[i] - asd, 2);
        }
        dsd /= densityVariation.length > 0 ? densityVariation.length : 1;
        dsd = Math.sqrt(dsd);

        double variationThreshold = asd + dsd;

        int indexThreshold = 0;

        while (indexThreshold < densityVariation.length && densityVariation[indexThreshold] <= variationThreshold) {
            indexThreshold++;
        }

        double densityThreshold = densities.get(indexThreshold);
        ArrayList<Range> restrictedRanges = new ArrayList<>();
        for (MasterClusterBlock block : clusters) {
            if (block.globalDensity <= densityThreshold) { //candidate for being an outlier node
                noiseBlockCandidates.add(block.id);
                restrictedRanges.add(block.restrictedRange);
            }
        }

        if (noiseBlockCandidates.isEmpty()) {
            //send the refinement requests with the empty set of actualNoiseBlocks
            sendIntermediateRefinementRequest();
        } else {
            //check if each noiseBlockCandidate is actually a noise-block by sending the
            //requests for restricted count
            for (String workerId : workerIds) {
                master.sendMessage(workerId, new RestrictedCountRequest(executionId, noiseBlockCandidates, restrictedRanges));
            }
        }
    }

    public void sendIntermediateRefinementRequest() {
        ArrayList<Integer> clusterBlockIds = new ArrayList<>();
        ArrayList<double[]> clusterCentroids = new ArrayList<>();
        ArrayList<double[]> clusterDetailedRadii = new ArrayList<>();
        for (MasterClusterBlock block : clusters) {
            clusterBlockIds.add(block.id);
            clusterCentroids.add(block.getCentroid());
            clusterDetailedRadii.add(block.getDetailedRadius(radiusMultiplier));
        }
        for (String workerId : workerIds) {
            master.sendMessage(workerId, new IntermediateRefinementRequest(executionId, clusterBlockIds, clusterCentroids, clusterDetailedRadii));
        }
    }

    public void startAgglomerativePhase() {
        //first find the actual cluster
        //and compute the SSQ of the whole dataset without outlier
        //and the sum of SSQ of clusters
        int nDatasetWO = 0;
        double[] LSdatasetWO = new double[dimensionality];
        double[] SSdatasetWO = new double[dimensionality];
        double SSQclustersWO = 0;
        for (MasterClusterBlock block : clusters) { //heare, clusters are only non-noise refined blocks
            block.computeGlobalSSQ();
            SSQclustersWO += block.globalSSQ;
            nDatasetWO += block.globalN;
            for (int i = 0; i < dimensionality; i++) {
                LSdatasetWO[i] += block.globalLS[i];
                SSdatasetWO[i] += block.globalSS[i];
            }
        }
        double SSQdatasetWO = 0;
        for (int i = 0; i < dimensionality; i++) {
            SSQdatasetWO += SSdatasetWO[i] - (LSdatasetWO[i] / nDatasetWO) * LSdatasetWO[i];
        }

        BCSSQ = SSQdatasetWO - SSQclustersWO;
        SSQ = SSQclustersWO;
        if (clusters.size() > 1) {
            chIndex = (BCSSQ * (nDatasetWO - clusters.size())) / (SSQ * (clusters.size() - 1));
        }

        int k = clusters.size();
        if (k <= 2) { //TODO: even if clusters are not merged, we need the final refinement to assign the cluster id
            sendFinalRefinementRequest();
            return;
        }

        ArrayList<ClusterPair> merges = new ArrayList<>();
        double maxCHindex = chIndex;
        ClusterBlocksHeap q = new ClusterBlocksHeap(new ArrayList(clusters), sequence, false);
        if (q.isEmpty()) {
            sendFinalRefinementRequest();
            return;
        }
        double minInc = q.peek().SSQinc;
        double newCHindex = ((BCSSQ - minInc) * (nDatasetWO - (k - 1))) / ((SSQ + minInc) * (k - 2));
        HashMap<Integer, MasterClusterBlock> clusterMap = new HashMap<>();
        for (MasterClusterBlock c : clusters) {
            clusterMap.put(c.id, c);
        }
        double tempSSQ = SSQ;
        double tempBCSSQ = BCSSQ;
        while (newCHindex > maxCHindex * 0.9 && !q.isEmpty()) {
            //chIndex = newCHindex;
            tempSSQ += minInc;
            tempBCSSQ -= minInc;
            k--;
            maxCHindex = Math.max(maxCHindex, newCHindex); //max function needed because newIndex could be less than maxCHindex!
            merges.add(q.peek());
            MasterClusterBlock mergedBlock = q.updateQueueByMerge();
            if (k > 2 && !q.isEmpty()) {
                minInc = q.peek().SSQinc;
                newCHindex = ((tempBCSSQ - minInc) * (nDatasetWO - (k - 1))) / ((tempSSQ + minInc) * (k - 2));
            } else {
                newCHindex = 0;
            }
        }

        if (merges.isEmpty()) {
            sendFinalRefinementRequest();
            return;
        }
        Iterator<ClusterPair> it = merges.iterator();
        ClusterPair nextMerge = it.next();
        k = clusters.size();
        while (chIndex != maxCHindex) {
            clusterMap.remove(nextMerge.c1.id);
            clusterMap.remove(nextMerge.c2.id);
            clusterMap.put(nextMerge.cMerge.id, nextMerge.cMerge);
            SSQ += nextMerge.SSQinc;
            BCSSQ -= nextMerge.SSQinc;
            k--;
            chIndex = (BCSSQ * (nDatasetWO - k)) / (SSQ * (k - 1));
            if (!it.hasNext()) {
                break;
            }
            nextMerge = it.next();
        }

        //clusters = q.getClusters();
        clusters = new LinkedList(clusterMap.values());
        sendFinalRefinementRequest();

    }

    public void sendFinalRefinementRequest() {
        ArrayList<double[]> clusterCentroids = new ArrayList<>();
        ArrayList<double[]> clusterDetailedRadii = new ArrayList<>();
        int id = 1;
        for (MasterClusterBlock block : clusters) {
            block.setId(id);
            clusterCentroids.add(block.getCentroid());
            clusterDetailedRadii.add(block.getDetailedRadius(radiusMultiplier));
            id++;
        }
        for (String workerId : workerIds) {
            master.sendMessage(workerId, new FinalRefinementRequest(executionId, clusterCentroids, clusterDetailedRadii));
        }
    }

    public synchronized void increaseReceivedFinalRefinement(int[] n, double[][] LS, double[][] SS) {
        receivedFinalRefinement++;
        int i = 0;
        for (MasterClusterBlock cluster : clusters) {
            cluster.addGlobalNLSSS(n[i], LS[i], SS[i], receivedFinalRefinement == 1);
            i++;
        }
        if (receivedFinalRefinement == workerIds.size()) {
            for (MasterClusterBlock c:clusters) {
                c.computeGlobalSSQ();
            }
            System.out.println("Finished execution " + executionId);
        }
    }

}
