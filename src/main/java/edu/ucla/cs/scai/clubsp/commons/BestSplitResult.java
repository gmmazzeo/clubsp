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
public class BestSplitResult implements Serializable {

    int position;
    double deltaSSQ;

    public BestSplitResult(int position, double deltaSSQ) {
        this.position = position;
        this.deltaSSQ = deltaSSQ;
    }

    public int getPosition() {
        return position;
    }

    public double getDeltaSSQ() {
        return deltaSSQ;
    }

}
