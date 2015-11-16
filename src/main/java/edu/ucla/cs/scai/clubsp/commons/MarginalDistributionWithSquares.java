/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ucla.cs.scai.clubsp.commons;

/**
 *
 * @author Giuseppe M. Mazzeo <mazzeo@cs.ucla.edu>
 */
public class MarginalDistributionWithSquares extends MarginalDistribution {

    public double[][] sumSqr;

    public MarginalDistributionWithSquares(int width) {
        super(width);
        sumSqr = new double[width][];
    }

    @Override
    public void add(int[] p, int position) {
        super.add(p, position);
        if (count[position] == 1) {
            sumSqr[position] = new double[p.length];
        }
        for (int i = 0; i < sumSqr[position].length; i++) {
            sumSqr[position][i] += 1.0 * p[i] * p[i];
        }
    }

    public void add(MarginalDistributionWithSquares m) {
        for (int i = 0; i < m.count.length; i++) {
            if (m.count[i] > 0) {
                if (count[i] == 0) {
                    sum[i] = new double[m.sum[i].length];
                    sumSqr[i] = new double[m.sum[i].length];
                }
                count[i] += m.count[i];
                for (int k = 0; k < sum[i].length; k++) {
                    sum[i][k] += m.sum[i][k];
                    sumSqr[i][k] += m.sumSqr[i][k];
                }
            }
        }
    }

    public void sub(MarginalDistributionWithSquares m) {
        for (int i = 0; i < m.count.length; i++) {
            if (m.count[i] > 0) {
                count[i] -= m.count[i];
                for (int k = 0; k < sum[i].length; k++) {
                    sum[i][k] -= m.sum[i][k];
                    sumSqr[i][k] -= m.sumSqr[i][k];
                }
            }
        }
    }

    public MarginalDistribution getSimpleCopy(int beginIndex, int endIndex) {
        return super.getCopy(beginIndex, endIndex);
    }

    public MarginalDistribution getSimpleCopy() {
        return super.getCopy(0, count.length - 1);
    }

    @Override
    public MarginalDistributionWithSquares getCopy(int beginIndex, int endIndex) {
        MarginalDistributionWithSquares copy = new MarginalDistributionWithSquares(endIndex - beginIndex + 1);
        System.arraycopy(count, beginIndex, copy.count, 0, copy.count.length);
        for (int j = beginIndex; j <= endIndex; j++) {
            if (count[j] > 0) {
                copy.sum[j - beginIndex] = new double[sum[j].length];
                System.arraycopy(sum[j], 0, copy.sum[j - beginIndex], 0, sum[j].length);
                copy.sumSqr[j - beginIndex] = new double[sumSqr[j].length];
                System.arraycopy(sumSqr[j], 0, copy.sumSqr[j - beginIndex], 0, sumSqr[j].length);
            }

        }
        return copy;
    }

    @Override
    public MarginalDistributionWithSquares getCopy() {
        return getCopy(0, count.length - 1);
    }
}
