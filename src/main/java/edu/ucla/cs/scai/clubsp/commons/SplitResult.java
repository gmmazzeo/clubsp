/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ucla.cs.scai.clubsp.commons;

import java.io.Serializable;

/**
 *
 * @author Giuseppe M. Mazzeo <mazzeo@cs.ucla.edu>
 */
public class SplitResult implements Serializable {

    public int leftN;
    public double[] leftLS;
    public double[] leftSS;
    public int rightN;
    public double[] rightLS;
    public double[] rightSS;

    public SplitResult(int leftN, double[] leftLS, double[] leftSS, int rightN, double[] rightLS, double[] rightSS) {
        this.leftN = leftN;
        this.leftLS = leftLS;
        this.leftSS = leftSS;
        this.rightN = rightN;
        this.rightLS = rightLS;
        this.rightSS = rightSS;
    }
}
