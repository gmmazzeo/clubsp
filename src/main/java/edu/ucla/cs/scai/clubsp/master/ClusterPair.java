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

/**
 *
 * @author Giuseppe M. Mazzeo <mazzeo@cs.ucla.edu>
 */
public class ClusterPair implements Comparable<ClusterPair> {

    MasterClusterBlock c1;
    MasterClusterBlock c2;
    MasterClusterBlock cMerge;
    double[] LS, SS;
    int n;
    double SSQinc;
    double SSQ;
    double resultingChIndex;

    public ClusterPair(MasterClusterBlock c1, MasterClusterBlock c2, boolean ignoreRadiusCriterion) {
        this.c1 = c1;
        this.c2 = c2;
        LS = new double[c1.dimensionality];
        SS = new double[c1.dimensionality];
        SSQinc = 0;
        for (int i = 0; i < LS.length; i++) {
            LS[i] = c1.globalLS[i] + c2.globalLS[i];
            SS[i] = c1.globalSS[i] + c2.globalSS[i];
            SSQinc += Math.pow(c1.globalLS[i] / c1.globalN - 
                    c2.globalLS[i] / c2.globalN, 2);
        }
        n = c1.globalN + c2.globalN;
        SSQinc *= (1.0 * c1.globalN * c2.globalN) / n;
        if (!ignoreRadiusCriterion) {
            for (int i = 0; i < LS.length; i++) {
                double ssq = SS[i] - (LS[i] / n) * LS[i];
                double r = Math.sqrt(ssq / n);
                double r1 = c1.getRadius(i, 1);                
                double r2 = c2.getRadius(i, 1);
                //double dist = Math.abs(c1.LS[i] / c1.n - c2.LS[i] / c2.n);
                boolean oldCriterion = r <= 2 * (r1 + r2);
                //boolean newCriterion = dist <= r1 + r2; //the two criteria are basically equivalent: the radius obtained by the merge is roughly twice the distance between centroids
                if (!oldCriterion) {
                    SSQinc = Double.POSITIVE_INFINITY;
                    break;
                }
            }
        }
    }

    public MasterClusterBlock getC1() {
        return c1;
    }

    public void setC1(MasterClusterBlock c1) {
        this.c1 = c1;
    }

    public MasterClusterBlock getC2() {
        return c2;
    }

    public void setC2(MasterClusterBlock c2) {
        this.c2 = c2;
    }

    public double getSSQinc() {
        return SSQinc;
    }

    public void setSSQinc(double SSQinc) {
        this.SSQinc = SSQinc;
    }

    @Override
    public int compareTo(ClusterPair o) {
        return Double.compare(SSQinc, o.SSQinc);
    }
}
