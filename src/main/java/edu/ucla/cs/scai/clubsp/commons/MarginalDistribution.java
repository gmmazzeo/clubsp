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
package edu.ucla.cs.scai.clubsp.commons;

import java.io.Serializable;

/**
 *
 * @author Giuseppe M. Mazzeo <mazzeo@cs.ucla.edu>
 */
public class MarginalDistribution implements Serializable {

    public double[][] sum;
    public int[] count;

    public MarginalDistribution(int width) {
        sum = new double[width][];
        count = new int[width];
    }

    public void add(int[] p, int position) {
        count[position]++;
        if (count[position] == 1) {
            sum[position] = new double[p.length];
        }
        for (int i = 0; i < sum[position].length; i++) {
            sum[position][i] += p[i];
        }
    }

    public void add(MarginalDistribution m) {
        for (int i = 0; i < m.count.length; i++) {
            if (m.count[i] > 0) {
                if (count[i] == 0) {
                    sum[i] = new double[m.sum[i].length];
                }
                count[i] += m.count[i];
                for (int k = 0; k < sum[i].length; k++) {
                    sum[i][k] += m.sum[i][k];
                }
            }
        }
    }

    public void sub(MarginalDistribution m) {
        for (int i = 0; i < m.count.length; i++) {
            if (m.count[i] > 0) {
                count[i] -= m.count[i];
                for (int k = 0; k < sum[i].length; k++) {
                    sum[i][k] -= m.sum[i][k];
                }
            }
        }
    }

    public MarginalDistribution getCopy() {
        return getCopy(0, count.length - 1);
    }

    public MarginalDistribution getCopy(int beginIndex, int endIndex) {
        MarginalDistribution copy = new MarginalDistribution(endIndex - beginIndex + 1);
        System.arraycopy(count, beginIndex, copy.count, 0, copy.count.length);
        //for (int j=beginIndex; j<count.length; j++) {
        //    copy.count[j-beginIndex]=count[j];
        //}
        for (int j = beginIndex; j <= endIndex; j++) {
            if (count[j] > 0) {
                copy.sum[j - beginIndex] = new double[sum[j].length];
                System.arraycopy(sum[j], 0, copy.sum[j - beginIndex], 0, sum[j].length);
                //for (int k = 0; k < sum[j].length; k++) {
                //    copy.sum[j - beginIndex][k] = sum[j][k];
                //}
            }
        }
        return copy;
    }

}
