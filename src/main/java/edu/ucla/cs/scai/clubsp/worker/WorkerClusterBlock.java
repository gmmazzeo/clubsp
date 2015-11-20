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
import edu.ucla.cs.scai.clubsp.commons.MarginalDistributionWithSquares;
import edu.ucla.cs.scai.clubsp.commons.Range;
import edu.ucla.cs.scai.clubsp.commons.SplitResult;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

/**
 *
 * @author Giuseppe M. Mazzeo <mazzeo@cs.ucla.edu>
 */
public class WorkerClusterBlock implements Comparable<WorkerClusterBlock> {

    int id;
    double[] localLS; //linear sum of the coordinates of the local points in the block
    double[] localSS; //squared sum of the coordinates of the local points in the block
    int localN; //the number of local points inside the block
    double[] globalLS; //linear sum of the coordinates of the global points in the block
    double[] globalSS;//squared sum of the coordinates of the global points in the block
    int globalN; //the number of global points inside the block
    Range r; //the coordinates of the global range
    WorkerClusterBlock lc; //left child node
    WorkerClusterBlock rc; //right child node
    WorkerClusterBlock parent; //parent node
    WorkerClusterBlock sibling; //sibling node    
    int sd = -1; //the splitting dimension, when node is split sd is in [0..d-1];
    int sp; //the splitting position    
    ArrayList<DataSetPoint> data;
    double localSSQ;
    double globalSSQ;
    double localSSQd[];
    double globalSSQd[];
    double globalRadius;
    double globalRadiusD[];
    double globalAvgMarg[];
    double actualDeltaSSQ = 0;
    boolean isOutlierNode = false;
    MarginalDistributionWithSquares[] localMarginals;
    MarginalDistribution[] globalMarginals;
    int dimensionality;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public static WorkerClusterBlock initRoot(Collection<DataSetPoint> data, Range globalDomain, int id) {
        return new WorkerClusterBlock(data, globalDomain, id, true);
    }

    //this constructor must be called only by a worker
    //data are scanned, marginals, LS, SS and SSQ are computed
    private WorkerClusterBlock(Collection<DataSetPoint> data, Range globalDomain, int id, boolean computeMarginals) {
        this.data = new ArrayList(data);
        this.id = id;
        this.r = globalDomain;
        dimensionality = globalDomain.inf.length;
        localN = data.size();
        localMarginals = new MarginalDistributionWithSquares[dimensionality];
        globalMarginals = new MarginalDistribution[dimensionality];
        if (computeMarginals) {
            computeMarginalsFromData();
        }
    }

    //this constructor must be called only by the master
    public static WorkerClusterBlock initBlock(Range globalDomain, int globalCount, int id) {
        return new WorkerClusterBlock(globalDomain, globalCount, id);
    }

    //this contructor must be called only for by the master
    private WorkerClusterBlock(Range globalDomain, int globalCount, int id) {
        this.r = globalDomain;
        this.id = 0;
        dimensionality = globalDomain.inf.length;
        localLS = new double[globalDomain.getDimensionality()];
        localSS = new double[globalDomain.getDimensionality()];
        globalLS = new double[dimensionality];
        globalSS = new double[dimensionality];
        this.globalN = globalCount;
    }

    private void computeMarginalsFromData() {
        for (int i = 0; i < dimensionality; i++) {
            localMarginals[i] = new MarginalDistributionWithSquares(r.getWidth(i));
        }
        for (DataSetPoint point : data) {
            int[] p = point.p;
            for (int d = 0; d < dimensionality; d++) {
                int coord = p[d];
                localMarginals[d].add(p, coord - r.inf[d]);
            }
        }
        computeLocalLSSSfromMarginals();
    }

    private void computeLocalLSSSfromMarginals() {
        localN = data.size();
        localLS = new double[dimensionality];
        localSS = new double[dimensionality];
        int shortestMarginal=0;
        for (int i=1; i<dimensionality; i++) {
            if (localMarginals[i].count.length<localMarginals[shortestMarginal].count.length) {
                shortestMarginal=i;
            }
        }
        for (int j = 0; j < localMarginals[shortestMarginal].count.length; j++) {
            for (int i = 0; i < dimensionality; i++) {            
                if (localMarginals[shortestMarginal].count[j] > 0) {
                    localLS[i] += localMarginals[shortestMarginal].sum[j][i];
                    localSS[i] += localMarginals[shortestMarginal].sumSqr[j][i];
                }
            }
        }
    }

    public boolean isLeaf() {
        return lc == null;
    }

    public boolean isRoot() {
        return parent == null;
    }

    public double getLocalSSQ() {
        return localSSQ;
    }

    public double getGlobalSSQ() {
        return globalSSQ;
    }

    public void setIsOutlierNode(boolean isOutlierNode) {
        this.isOutlierNode = isOutlierNode;
    }

    public boolean isOutlierNode() {
        return isOutlierNode;
    }

    //this method is invoked on a worker that has the global marginals of the block
    //if the global marginal is not initialized, it means that there is one only worker
    //and there was no previous computation of the global marginals
    public BestSplitResult computeBestSplit(int dimension) {
        double maxDeltaSSQ = 0; //the maximum SSQ reduction found on the dimension
        int bestPosition = -1; //the best position found on the dimension
        double[] leftLS = new double[dimensionality];
        double[] rightLS = new double[dimensionality];
        if (globalMarginals[dimension] == null) {
            globalMarginals[dimension] = localMarginals[dimension].getSimpleCopy();
        }
        int nLeft = 0;
        int nRight = globalN;
        System.arraycopy(globalLS, 0, rightLS, 0, dimensionality);
        //compute the splitting position set
        MarginalDistribution marginals = globalMarginals[dimension];
        int width = r.getWidth(dimension);
        for (int pos = 0; pos < width - 1; pos++) {
            if (marginals.count[pos] == 0) {
                continue;
            }
            nLeft += marginals.count[pos];
            nRight -= marginals.count[pos];
            if (nRight == 0) {
                break;
            }
            for (int k = 0; k < dimensionality; k++) {
                leftLS[k] += marginals.sum[pos][k];
                rightLS[k] -= marginals.sum[pos][k];
            }
            double deltaSSQ = 0;
            for (int k = 0; k < dimensionality; k++) {
                deltaSSQ += Math.pow(leftLS[k] / nLeft - rightLS[k] / nRight, 2);
            }
            deltaSSQ *= (1.0 * Math.max(nLeft, nRight) / globalN) * Math.min(nLeft, nRight);
            if (deltaSSQ > maxDeltaSSQ) {
                maxDeltaSSQ = deltaSSQ;
                bestPosition = r.getInfCoord(dimension) + pos;
            }
        }
        return new BestSplitResult(bestPosition, maxDeltaSSQ);
    }

    //splits the block, partition data and computes the marginals, LS, and SS
    //this method is for the worker
    public WorkerClusterBlock[] split(int splitDimension, int splitPosition, int leftId, int rightId) {
        Range[] newRanges = r.getSplit(splitDimension, splitPosition);
        ArrayList<DataSetPoint> dataLeft = new ArrayList<>();
        ArrayList<DataSetPoint> dataRight = new ArrayList<>();
        for (DataSetPoint p : data) {
            if (p.p[splitDimension] <= splitPosition) {
                dataLeft.add(p);
            } else {
                dataRight.add(p);
            }
        }
        data.clear();
        sd = splitDimension;
        sp = splitPosition;
        WorkerClusterBlock leftBlock = new WorkerClusterBlock(dataLeft, newRanges[0], leftId, dataLeft.size() <= dataRight.size());
        WorkerClusterBlock rightBlock = new WorkerClusterBlock(dataRight, newRanges[1], rightId, dataLeft.size() > dataRight.size());
        leftBlock.parent = this;
        rightBlock.parent = this;
        lc = leftBlock;
        rc = rightBlock;
        leftBlock.sibling = rightBlock;
        rightBlock.sibling = leftBlock;

        (dataLeft.size() > dataRight.size() ? leftBlock : rightBlock).computeMarginalsFromSibling();

        return new WorkerClusterBlock[]{leftBlock, rightBlock};
    }

    private void computeMarginalsFromSibling() {
        localN = data.size();
        localMarginals = new MarginalDistributionWithSquares[dimensionality];
        for (int i = 0; i < dimensionality; i++) {
            if (i == parent.sd) {
                if (this==parent.lc) {
                    localMarginals[i] = parent.localMarginals[i].getCopy(0, parent.sp-parent.r.inf[i]);
                } else {
                    localMarginals[i] = parent.localMarginals[i].getCopy(parent.sp-parent.r.inf[i]+1, parent.localMarginals[i].count.length-1);
                }
            } else {
                localMarginals[i] = parent.localMarginals[i].getCopy();
                localMarginals[i].sub(sibling.localMarginals[i]);
            }
        }
        computeLocalLSSSfromMarginals();
    }

    @Override
    public int compareTo(WorkerClusterBlock cb) {
        if (globalSSQ < cb.globalSSQ) {
            return 1;
        }
        if (globalSSQ > cb.globalSSQ) {
            return -1;
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof WorkerClusterBlock)) {
            return false;
        }
        WorkerClusterBlock cb = (WorkerClusterBlock) o;
        return this.r.equals(cb.r);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + (this.r != null ? this.r.hashCode() : 0);
        return hash;
    }

    public Range getR() {
        return r;
    }

    public double[] getLocalLS() {
        return localLS;
    }

    public void setLocalLS(double[] localLS) {
        this.localLS = localLS;
    }

    public double[] getLocalSS() {
        return localSS;
    }

    public void setLocalSS(double[] localSS) {
        this.localSS = localSS;
    }

    public int getLocalN() {
        return localN;
    }

    public void setLocalN(int localN) {
        this.localN = localN;
    }

    public double[] getGlobalLS() {
        return globalLS;
    }

    public void setGlobalLS(double[] globalLS) {
        this.globalLS = globalLS;
    }

    public double[] getGlobalSS() {
        return globalSS;
    }

    public void setGlobalSS(double[] globalSS) {
        this.globalSS = globalSS;
    }

    public int getGlobalN() {
        return globalN;
    }

    public void setGlobalN(int globalN) {
        this.globalN = globalN;
    }

    public void addLocalLS(double[] LS) {
        for (int i = 0; i < LS.length; i++) {
            this.localLS[i] += LS[i];
        }
    }

    public void addGlobalN(int n) {
        this.globalN += n;
    }

    public void addGlobalLS(double[] LS) {
        for (int i = 0; i < LS.length; i++) {
            this.globalLS[i] += LS[i];
        }
    }

    public void addLocalSS(double[] SS) {
        for (int i = 0; i < SS.length; i++) {
            this.localSS[i] += SS[i];
        }
    }

    public void addGlobalSS(double[] SS) {
        for (int i = 0; i < SS.length; i++) {
            this.globalSS[i] += SS[i];
        }
    }

    public double[] getCentroid() {
        double[] centroid = new double[dimensionality];
        for (int k = 0; k < dimensionality; k++) {
            centroid[k] = globalLS[k] / globalN;
        }
        return centroid;
    }

    public MarginalDistribution[] getLocalMarginals() {
        return localMarginals;
    }

    public MarginalDistribution[] getGlobalMarginals() {
        return globalMarginals;
    }

    public boolean valleyCriterion(int dimension, double delta) {

        int width = r.sup[dimension] - r.inf[dimension] + 1;
        int windowSemiWidth = width / 20;
        if (windowSemiWidth < 2) {
            return false;
        }

        double[] mobileAvg = new double[width];
        int currentWindowsCount = 0;
        for (int j = 0; j < windowSemiWidth; j++) {
            currentWindowsCount += globalMarginals[dimension].count[j];
        }
        int currentWindowSize = windowSemiWidth;
        double maxMobileAvg = mobileAvg[0];
        double minMobileAvg = mobileAvg[1];

        double sum = 0;
        //double sumSqr = 0;

        for (int j = 0; j < width; j++) {
            mobileAvg[j] = 1.0 * currentWindowsCount / currentWindowSize;
            sum += mobileAvg[j];
            //sumSqr += mobileAvg[j] * mobileAvg[j];
            if (mobileAvg[j] > maxMobileAvg) {
                maxMobileAvg = mobileAvg[j];
            } else if (mobileAvg[j] < minMobileAvg) {
                minMobileAvg = mobileAvg[j];
            }
            if (j >= windowSemiWidth) {
                currentWindowsCount -= globalMarginals[dimension].count[j - windowSemiWidth];
                currentWindowSize--;
            }
            if (j + windowSemiWidth < mobileAvg.length) {
                currentWindowsCount += globalMarginals[dimension].count[j + windowSemiWidth];
                currentWindowSize++;
            }
        }

        double avg = sum / width;
        double gap = avg * delta;
        if (gap < 1) {
            gap = 1;
        }

        ArrayList<Integer> restrictedPositions = new ArrayList<>();
        //now find local minima and maxima
        boolean[] localMinima = new boolean[width];
        boolean[] localMaxima = new boolean[width];
        int lastRestrictedPositionType = 0; //-1: min, 1: max

        if (parent != null && maxMobileAvg < 1.0 * globalN / r.getWidth(dimension)) {
            return false;
        }

        double maxInfLimit = minMobileAvg + delta * avg;
        double minSupLimit = maxMobileAvg - delta * avg;

        if (mobileAvg[0] < mobileAvg[1] && mobileAvg[0] <= minSupLimit) {
            localMinima[0] = true;
        } else if (mobileAvg[0] > mobileAvg[1] && mobileAvg[0] >= maxInfLimit) {
            localMaxima[0] = true;
            lastRestrictedPositionType = 1;
            restrictedPositions.add(0);
        }

        for (int j = 1; j < width - 1; j++) {
            if (mobileAvg[j] < mobileAvg[j - 1] && mobileAvg[j] < mobileAvg[j + 1] && mobileAvg[j] <= minSupLimit) {
                localMinima[j] = true;
                if (lastRestrictedPositionType == 1) {
                    restrictedPositions.add(j);
                    lastRestrictedPositionType = -1;
                } else if (lastRestrictedPositionType == -1 && mobileAvg[j] < mobileAvg[restrictedPositions.get(restrictedPositions.size() - 1)]) {
                    restrictedPositions.set(restrictedPositions.size() - 1, j);
                }
            } else if (mobileAvg[j] > mobileAvg[j - 1] && mobileAvg[j] > mobileAvg[j + 1] && mobileAvg[j] >= maxInfLimit) {
                localMaxima[j] = true;
                if (lastRestrictedPositionType != 1) {
                    restrictedPositions.add(j);
                    lastRestrictedPositionType = 1;
                } else if (lastRestrictedPositionType == 1 && mobileAvg[j] > mobileAvg[restrictedPositions.get(restrictedPositions.size() - 1)]) {
                    restrictedPositions.set(restrictedPositions.size() - 1, j);
                }
            }
        }

        if (mobileAvg[width - 1] < mobileAvg[width - 2] && mobileAvg[width - 1] <= minSupLimit) {
            localMinima[width - 1] = true;
            if (lastRestrictedPositionType == 1) {
                restrictedPositions.add(width - 1);
            } else if (lastRestrictedPositionType == -1 && mobileAvg[width - 1] < mobileAvg[restrictedPositions.get(restrictedPositions.size() - 1)]) {
                restrictedPositions.set(restrictedPositions.size() - 1, width - 1);
            }
        } else if (mobileAvg[width - 1] > mobileAvg[width - 2] && mobileAvg[width - 1] >= maxInfLimit) {
            localMaxima[width - 1] = true;
            if (lastRestrictedPositionType != 1) {
                restrictedPositions.add(width - 1);
            } else if (lastRestrictedPositionType == 1 && mobileAvg[width - 1] > mobileAvg[restrictedPositions.get(restrictedPositions.size() - 1)]) {
                restrictedPositions.set(restrictedPositions.size() - 1, width - 1);
            }
        }
        if (restrictedPositions.size() < 3) {
            return false;
        }
        //find i
        int i = 0;

        double minSup = mobileAvg[restrictedPositions.get(0)] - gap;
        //find j
        int j = 1;
        while (j < restrictedPositions.size() && (localMaxima[restrictedPositions.get(j)] || mobileAvg[restrictedPositions.get(j)] > minSup)) {
            if (mobileAvg[restrictedPositions.get(j)] > mobileAvg[restrictedPositions.get(i)]) {
                i = j;
                minSup = mobileAvg[restrictedPositions.get(i)] - gap;
            }
            j++;
        }
        if (j == restrictedPositions.size()) {
            return false;
        }
        double maxInf = mobileAvg[restrictedPositions.get(j)] + gap;
        //find k
        int k = j + 1;
        while (k < restrictedPositions.size() && (localMinima[restrictedPositions.get(k)] || mobileAvg[restrictedPositions.get(k)] < maxInf)) {
            if (mobileAvg[restrictedPositions.get(k)] < mobileAvg[restrictedPositions.get(j)]) {
                j = k;
                maxInf = mobileAvg[restrictedPositions.get(j)] + gap;
            }
            k++;
        }
        if (k < restrictedPositions.size()) {
            return true;
        }

        return false;
    }

    //if the required global marginals were not initialized, they are copied from local marginals
    public MarginalDistribution getGlobalMarginals(int dimension) {
        if (globalMarginals[dimension] == null) {
            globalMarginals[dimension] = localMarginals[dimension].getSimpleCopy();
        }
        return globalMarginals[dimension];
    }

    public void sumToGlobalMarginals(MarginalDistribution marginals, int dimension) {
        if (globalMarginals[dimension] == null) {
            globalMarginals[dimension] = localMarginals[dimension].getSimpleCopy();
        }
        globalMarginals[dimension].add(marginals);
    }

    public int rangeCount(Range range) {
        int res=0;
        for (DataSetPoint d:data) {
            if (range.contains(d.p)) {
                res++;
            }
        }
        return res;
    }
}
