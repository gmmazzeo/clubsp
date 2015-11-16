/*
 * Copyright 2014 ScAi, CSD, UCLA.
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

import edu.ucla.cs.scai.clubsp.commons.Range;
import java.util.Arrays;

/**
 *
 * @author Giuseppe M. Mazzeo <mazzeo@cs.ucla.edu>
 */
public class MasterClusterBlock implements Comparable<MasterClusterBlock> {

    int id;
    double[] globalLS; //linear sum of the coordinates of the global points in the block
    double[] globalSS;//squared sum of the coordinates of the global points in the block
    int globalN; //the number of global points inside the block
    Range r; //the coordinates of the global range
    Range restrictedRange; //the coordinates of the global range
    MasterClusterBlock lc; //left child node
    MasterClusterBlock rc; //right child node
    MasterClusterBlock parent; //parent node
    MasterClusterBlock sibling; //sibling node    
    int sd = -1; //the splitting dimension, when node is split sd is in [0..d-1];
    int sp; //the splitting position    
    double globalSSQ;
    double globalSSQd[];
    double globalRadius;
    double globalRadiusD[];
    double actualDeltaSSQ = 0;
    boolean isOutlierNode = false;
    int dimensionality;
    double globalDensity;
    double restrictedDensity;
    int restrictedCount;
    double restrictedVolume;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    //this constructor must be called only by the master
    public static MasterClusterBlock initRoot(Range globalDomain, int id) {
        return new MasterClusterBlock(globalDomain, 0, id);
    }

    public static MasterClusterBlock merge(ClusterPair cp, int id) {
        if (cp.cMerge == null) {
            Range r = cp.c1.r.getCopy();
            for (int i = 0; i < r.inf.length; i++) {
                if (cp.c2.r.inf[i] < r.inf[i]) {
                    r.inf[i] = cp.c2.r.inf[i];
                } else if (cp.c2.r.sup[i] > r.sup[i]) {
                    r.sup[i] = cp.c2.r.sup[i];
                }
            }
            cp.cMerge = new MasterClusterBlock(r, cp.n, id);
            cp.cMerge.globalLS = Arrays.copyOf(cp.LS, cp.LS.length);
            cp.cMerge.globalSS = Arrays.copyOf(cp.SS, cp.SS.length);
            cp.cMerge.computeGlobalSSQ();
        }
        return cp.cMerge;
    }

    //this contructor must be called only for by the master
    private MasterClusterBlock(Range globalDomain, int globalCount, int id) {
        this.r = globalDomain;
        this.id = id;
        dimensionality = globalDomain.inf.length;
        globalLS = new double[dimensionality];
        globalSS = new double[dimensionality];
        this.globalN = globalCount;
    }

    public boolean isLeaf() {
        return lc == null;
    }

    public boolean isRoot() {
        return parent == null;
    }

    public void computeGlobalSSQ() {
        globalSSQ = 0;
        globalSSQd = new double[dimensionality];
        if (globalN > 1) {
            for (int d = 0; d < dimensionality; d++) {
                globalSSQd[d] = (globalSS[d] - (globalLS[d] / globalN) * globalLS[d]);
                globalSSQ += globalSSQd[d];
            }
        }
    }

    public double getGlobalSSQ() {
        return globalSSQ;
    }

    public double getRadius(int i, double multiplier) {
        return Math.sqrt(globalSSQd[i] / globalN);
    }

    public double[] getDetailedRadius(double multiplier) {
        double[] res = new double[dimensionality];
        for (int i = 0; i < dimensionality; i++) {
            res[i] = multiplier * Math.sqrt(globalSSQd[i] / globalN);
        }
        return res;
    }

    public int getDimensionality() {
        return r.getDimensionality();
    }

    public void setIsOutlierNode(boolean isOutlierNode) {
        this.isOutlierNode = isOutlierNode;
    }

    public boolean isOutlierNode() {
        return isOutlierNode;
    }

    //returns the 
    public MasterClusterBlock[] split(int splitDimension, int splitPosition, int leftId, int rightId) {
        Range[] newRanges = r.getSplit(splitDimension, splitPosition);
        sd = splitDimension;
        sp = splitPosition;
        MasterClusterBlock leftBlock = new MasterClusterBlock(newRanges[0], 0, leftId);
        MasterClusterBlock rightBlock = new MasterClusterBlock(newRanges[1], 0, rightId);
        leftBlock.parent = this;
        rightBlock.parent = this;
        leftBlock.sibling = rightBlock;
        rightBlock.sibling = leftBlock;
        lc = leftBlock;
        rc = rightBlock;
        return new MasterClusterBlock[]{leftBlock, rightBlock};
    }

    @Override
    public int compareTo(MasterClusterBlock cb) {
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
        if (!(o instanceof MasterClusterBlock)) {
            return false;
        }
        MasterClusterBlock cb = (MasterClusterBlock) o;
        return this.r.equals(cb.r);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + (this.r != null ? this.r.hashCode() : 0);
        return hash;
    }

    @Override
    public String toString() {
        String s = this.r.toString() + " " + globalN;
        return s;
    }

    public Range getR() {
        return r;
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

    public void addGlobalNLSSS(int n, double[] LS, double[] SS, boolean first) {
        if (first) {
            this.globalN = n;
            this.globalLS = LS;
            this.globalSS = SS;
        } else {
            this.globalN += n;
            for (int i = 0; i < LS.length; i++) {
                this.globalLS[i] += LS[i];
                this.globalSS[i] += SS[i];
            }
        }
    }

    public double[] getCentroid() {
        double[] centroid = new double[dimensionality];
        for (int k = 0; k < dimensionality; k++) {
            centroid[k] = globalLS[k] / globalN;
        }
        return centroid;
    }

    //globalDensity and restrictedCount were already computed when this method is coalled
    public void computeOutlierFlagBasedOnRestrictedDensity() {
        restrictedDensity = restrictedCount / restrictedVolume;
        isOutlierNode = restrictedCount < 10 || restrictedDensity < 2 * globalDensity;
    }

    public void computeRestrictedRangeAndVolume() {
        restrictedRange = r.getCopy();
        for (int j = 0; j < dimensionality; j++) {
            int restrictedWidth = Math.max(1, (int) ((r.sup[j] - r.inf[j] + 1) * 0.1 + 0.5));
            restrictedRange.sup[j] = Math.min(r.sup[j], (int) (globalLS[j] / globalN + restrictedWidth));
            restrictedRange.inf[j] = Math.max(r.inf[j], (int) (globalLS[j] / globalN - restrictedWidth));
        }
        restrictedVolume = restrictedRange.getVolume();
    }
}
