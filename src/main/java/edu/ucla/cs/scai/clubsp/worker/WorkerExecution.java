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
package edu.ucla.cs.scai.clubsp.worker;

import edu.ucla.cs.scai.clubsp.commons.BestSplitResult;
import edu.ucla.cs.scai.clubsp.commons.DataSetPoint;
import edu.ucla.cs.scai.clubsp.commons.MarginalDistribution;
import edu.ucla.cs.scai.clubsp.commons.Range;
import edu.ucla.cs.scai.clubsp.commons.SplitResult;
import edu.ucla.cs.scai.clubsp.commons.Utils;
import edu.ucla.cs.scai.clubsp.master.Sequence;
import edu.ucla.cs.scai.clubsp.messages.ComputeBestSplitResponse;
import edu.ucla.cs.scai.clubsp.messages.ComputeValleyCriterionResponse;
import edu.ucla.cs.scai.clubsp.messages.FinalRefinementResponse;
import edu.ucla.cs.scai.clubsp.messages.InitRootResponse;
import edu.ucla.cs.scai.clubsp.messages.IntermediateRefinementResponse;
import edu.ucla.cs.scai.clubsp.messages.LoadDataSetResponse;
import edu.ucla.cs.scai.clubsp.messages.ReceiveMarginalsRequest;
import edu.ucla.cs.scai.clubsp.messages.ReceiveMarginalsResponse;
import edu.ucla.cs.scai.clubsp.messages.RestrictedCountResponse;
import edu.ucla.cs.scai.clubsp.messages.SplitResponse;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.StringTokenizer;

/**
 *
 * @author Giuseppe M. Mazzeo <mazzeo@cs.ucla.edu>
 */
public class WorkerExecution {

    String dataSetId;
    String executionId;
    HashMap<Integer, WorkerClusterBlock> blocks = new HashMap<>();
    int dimensionality;
    ArrayList<DataSetPoint> dataSet;
    Sequence sequence = new Sequence();
    int dataSetSize;
    long initTime;
    Range localDomain, globalDomain;
    Worker worker;
    double scaleFactor;
    private static final double radiusMultiplier = 3;

    public WorkerExecution(final Worker worker, final String executionId, final String dataSetId, final double scaleFactor) {
        this.worker = worker;
        this.executionId = executionId;
        this.dataSetId = dataSetId;
        this.scaleFactor = scaleFactor;
        new Thread() {

            @Override
            public void run() {
                try {
                    BufferedReader in = new BufferedReader(new FileReader(worker.datasetsPath + dataSetId));
                    dataSet = new ArrayList<>();
                    String l = in.readLine();
                    if (l != null && l.trim().length() > 0) {
                        StringTokenizer st = new StringTokenizer(l, ", \t");
                        dimensionality = st.countTokens();
                    } else {
                        dimensionality = 0;
                    }
                    int j = 0;
                    int[] inf = new int[dimensionality];
                    int[] sup = new int[dimensionality];
                    for (int i = 0; i < dimensionality; i++) {
                        inf[i] = Integer.MAX_VALUE;
                        sup[i] = Integer.MIN_VALUE;
                    }
                    while (l != null && l.trim().length() > 0) {
                        StringTokenizer st = new StringTokenizer(l, ", \t");
                        int[] p = new int[dimensionality];
                        for (int i = 0; i < dimensionality; i++) {
                            p[i] = (int) (Integer.parseInt(st.nextToken()) / scaleFactor + 0.5);
                            inf[i] = Math.min(p[i], inf[i]);
                            sup[i] = Math.max(p[i], sup[i]);
                        }
                        dataSet.add(new DataSetPoint(p, j));
                        l = in.readLine();
                        j++;
                    }
                    dataSetSize = dataSet.size();
                    localDomain = new Range(inf, sup);
                    worker.sendMessageToMaster(new LoadDataSetResponse(executionId, localDomain));
                } catch (IOException | NumberFormatException e) {
                    e.printStackTrace();
                }
            }
        }.start();
    }

    public String getDataSet() {
        return dataSetId;
    }

    public HashMap<Integer, WorkerClusterBlock> getBlockMap() {
        return blocks;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + Objects.hashCode(this.dataSetId);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final WorkerExecution other = (WorkerExecution) obj;
        if (!Objects.equals(this.dataSetId, other.dataSetId)) {
            return false;
        }
        return true;
    }

    public void initRoot(Range globalDomain) {
        try {
            this.globalDomain = globalDomain;
            long startTime = System.currentTimeMillis();
            //data are scanned, marginals, LS, SS and SSQ are computed
            WorkerClusterBlock root = WorkerClusterBlock.initRoot(dataSet, globalDomain, 0);
            blocks.put(0, root);
            initTime = System.currentTimeMillis() - startTime;
            try (Socket socket = new Socket(worker.masterIp, worker.masterPort);
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());) {
                out.writeObject(new InitRootResponse(executionId, root.getLocalN(), root.getLocalLS(), root.getLocalSS()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void sendMarginals(int blockId, int dimension, String receiverId) {
        MarginalDistribution marginals = blocks.get(blockId).getGlobalMarginals(dimension);
        worker.sendMessageToWorker(receiverId, new ReceiveMarginalsRequest(executionId, blockId, dimension, marginals));
    }

    public void receiveMarginals(final int blockId, final int dimension, final MarginalDistribution marginals) {
        blocks.get(blockId).sumToGlobalMarginals(marginals, dimension);
        worker.sendMessageToMaster(new ReceiveMarginalsResponse(executionId, blockId, dimension));
    }

    public void computeBestSplit(int blockId, int dimension, int globalN, double[] globalLS, double[] globalSS) {
        WorkerClusterBlock block = blocks.get(blockId);
        block.setGlobalN(globalN);
        block.setGlobalLS(globalLS);
        block.setGlobalSS(globalSS);
        BestSplitResult split = block.computeBestSplit(dimension);
        try (Socket socket = new Socket(worker.masterIp, worker.masterPort);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
            out.writeObject(new ComputeBestSplitResponse(executionId, blockId, dimension, split));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void doSplit(int blockId, int splitDimension, int splitPosition, int leftId, int rightId, int globalN, double[] globalLS, double globalSS[]) {
        WorkerClusterBlock block = blocks.get(blockId);
        //the following three values could be already assigned at this point, because the computeBestSplit was previously called on this worker
        //however, it is possibile the for some workers the values were not assigned yet
        block.globalN = globalN;
        block.globalLS = globalLS;
        block.globalSS = globalSS;
        WorkerClusterBlock[] newBlocks = block.split(splitDimension, splitPosition, leftId, rightId);
        blocks.put(leftId, newBlocks[0]);
        blocks.put(rightId, newBlocks[1]);
        worker.sendMessageToMaster(new SplitResponse(executionId, blockId,
                new SplitResult(newBlocks[0].localN, newBlocks[0].localLS, newBlocks[0].localSS,
                        newBlocks[1].localN, newBlocks[1].localLS, newBlocks[1].localSS)));
    }

    public void computeValleyCriterion(int blockId, int dimension, double delta) {
        WorkerClusterBlock block = blocks.get(blockId);
        boolean satisfied = block.valleyCriterion(dimension, delta);
        worker.sendMessageToMaster(new ComputeValleyCriterionResponse(executionId, blockId, dimension, satisfied));
    }

    public void computeRestrictedCount(ArrayList<Integer> blockIds, ArrayList<Range> restrictedRanges) {
        ArrayList<Integer> count = new ArrayList<>();
        for (int i = 0; i < blockIds.size(); i++) {
            int c = blocks.get(blockIds.get(i)).rangeCount(restrictedRanges.get(i));
            count.add(c);
        }
        worker.sendMessageToMaster(new RestrictedCountResponse(executionId, blockIds, count));
    }

    public void doIntermediateRefinement(ArrayList<Integer> clusterBlockIds, ArrayList<double[]> originalCentroids, ArrayList<double[]> originalDetailedRadii) {
        ArrayList<WorkerClusterBlock> allLeafBlocks = new ArrayList<>();
        for (WorkerClusterBlock block : blocks.values()) {
            if (block.isLeaf()) {
                allLeafBlocks.add(block);
            }
        }
        ArrayList<WorkerClusterBlock> clusterBlocks = new ArrayList<>();
        HashMap<Integer, Double> minimumDetailedRadii = new HashMap<>();
        HashMap<Integer, double[]> centroids = new HashMap<>();
        HashMap<Integer, double[]> radii = new HashMap<>();
        for (int i = 0; i < clusterBlockIds.size(); i++) {
            int blockId = clusterBlockIds.get(i);
            WorkerClusterBlock block = blocks.get(blockId);
            clusterBlocks.add(block);
            double[] detailedRadius = originalDetailedRadii.get(i);
            double minDetailedRadius = Double.POSITIVE_INFINITY;
            for (int k = 0; k < dimensionality; k++) {
                minDetailedRadius = Math.min(detailedRadius[k], minDetailedRadius);
            }
            minimumDetailedRadii.put(blockId, minDetailedRadius);
            centroids.put(blockId, originalCentroids.get(i));
            radii.put(blockId, originalDetailedRadii.get(i));
        }
        HashMap<Integer, HashSet<Integer>> reachability = getReachability(allLeafBlocks, clusterBlocks, centroids, radii);
        for (int blockId : clusterBlockIds) {
            WorkerClusterBlock block = blocks.get(blockId);
            block.globalN = 0;
            block.globalLS = new double[dimensionality];
            block.globalSS = new double[dimensionality];
        }
        for (WorkerClusterBlock n : allLeafBlocks) {
            HashSet<Integer> reachableClusters = reachability.get(n.id);
            for (DataSetPoint point : n.data) {
                int[] p = point.p;
                WorkerClusterBlock nearestClusterBlock = null;
                double minDist = Double.POSITIVE_INFINITY;
                boolean outlier = true;
                for (int blockId : reachableClusters) {
                    WorkerClusterBlock block = blocks.get(blockId);
                    double dist = Utils.ellipticalRelativeDistanceWithLimit1(centroids.get(blockId), radii.get(block.id), p);
                    if (dist <= 1) {
                        outlier = false;
                    }
                    if (dist < minDist) {
                        minDist = dist;
                        nearestClusterBlock = block;
                    }
                }
                if (!outlier) {
                    nearestClusterBlock.globalN++;
                    for (int i = 0; i < dimensionality; i++) {
                        nearestClusterBlock.globalLS[i] += p[i];
                        nearestClusterBlock.globalSS[i] += p[i] * p[i];
                    }
                }
            }
        }

        ArrayList<Integer> clusterIds = new ArrayList<>();
        ArrayList<Integer> clusterN = new ArrayList<>();
        ArrayList<double[]> clusterLS = new ArrayList<>();
        ArrayList<double[]> clusterSS = new ArrayList<>();

        for (WorkerClusterBlock block : clusterBlocks) {
            clusterIds.add(block.id);
            clusterN.add(block.globalN);
            clusterLS.add(block.globalLS);
            clusterSS.add(block.globalSS);
        }

        worker.sendMessageToMaster(new IntermediateRefinementResponse(executionId, clusterIds, clusterN, clusterLS, clusterSS));
    }

    private HashMap<Integer, HashSet<Integer>> getReachability(ArrayList<WorkerClusterBlock> allBlocks, ArrayList<WorkerClusterBlock> clusterBlocks, HashMap<Integer, double[]> centroids, HashMap<Integer, double[]> radii) {
        HashMap<Integer, HashSet<Integer>> res = new HashMap<>();

        for (WorkerClusterBlock c1 : allBlocks) {
            HashSet<Integer> a = new HashSet<>();
            res.put(c1.id, a);
            for (WorkerClusterBlock c2 : clusterBlocks) {
                if (c1.id == c2.id) {
                    a.add(c2.id);
                } else {
                    double[] nearestBorderPoint = new double[dimensionality];
                    double[] c = centroids.get(c2.id);
                    for (int i = 0; i < dimensionality; i++) {
                        if (c[i] >= c1.r.inf[i]) {
                            if (c[i] <= c1.r.sup[i]) {
                                nearestBorderPoint[i] = c[i];
                            } else {
                                nearestBorderPoint[i] = c1.r.sup[i];
                            }
                        } else {
                            nearestBorderPoint[i] = c1.r.inf[i];
                        }
                    }
                    if (Utils.ellipticalRelativeDistanceWithLimit1(c, radii.get(c2.id), nearestBorderPoint) <= 1) {
                        a.add(c2.id);
                    }
                }
            }
        }
        return res;
    }

    public void doFinalRefinement(ArrayList<double[]> centroids, ArrayList<double[]> detailedRadii) {
        int[] clusterAssignment = new int[dataSet.size()];
        int[] clusterN = new int[centroids.size()];
        double[][] clusterLS = new double[centroids.size()][dimensionality];
        double[][] clusterSS = new double[centroids.size()][dimensionality];
        int nOutliers = 0;
        for (DataSetPoint point : dataSet) {
            int[] p = point.p;
            int bestId = -1;
            double minDist = Double.POSITIVE_INFINITY;
            boolean outlier = true;
            for (int i = 0; i < centroids.size(); i++) {
                double dist = Utils.ellipticalRelativeDistanceWithLimit1(centroids.get(i), detailedRadii.get(i), p);
                if (dist <= 1) {
                    outlier = false;
                }
                if (dist < minDist) {
                    minDist = dist;
                    bestId = i;
                }
            }

            if (!outlier) {
                clusterN[bestId]++;
                for (int i = 0; i < dimensionality; i++) {
                    clusterLS[bestId][i] += p[i];
                    clusterSS[bestId][i] += p[i] * p[i];
                }
            } else {
                nOutliers++;
            }
            clusterAssignment[point.filePosition] = bestId + 1;
        }
        worker.sendMessageToMaster(new FinalRefinementResponse(executionId, clusterN, clusterLS, clusterSS, nOutliers));
        //now print the labels on file
        try (PrintWriter out = new PrintWriter(new FileOutputStream(worker.datasetsPath + executionId + "_" + worker.id + ".labels"), true)) {
            for (int c : clusterAssignment) {
                out.println(c);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
