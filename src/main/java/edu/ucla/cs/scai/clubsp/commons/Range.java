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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 *
 * @author Giuseppe M. Mazzeo <mazzeo@cs.ucla.edu>
 */

public class Range implements Serializable {

    public int[] inf, sup;

    protected Range() {
    }

    public Range(List<Integer> coords) {
        this.inf = new int[coords.size()/2];
        this.sup = new int[coords.size()/2];
        int i=0;
        for (int c:coords) {
            if (i<inf.length) {
                inf[i]=c;
            } else {
                sup[i-inf.length]=c;
            }
            i++;
        }
    }
    
    public Range(List<Integer> inf, List<Integer> sup) {
        this.inf = new int[inf.size()];
        this.sup = new int[sup.size()];
        int i = 0;
        for (Iterator<Integer> it1 = inf.iterator(), it2 = sup.iterator(); it1.hasNext() && it2.hasNext(); i++) {
            this.inf[i] = it1.next();
            this.sup[i] = it2.next();
        }
    }

    public Range(int[] inf, int[] sup) {
        this.inf = inf;
        this.sup = sup;
    }

    public Range getCopy() {
        Range r = new Range();
        r.inf = Arrays.copyOf(inf, inf.length);
        r.sup = Arrays.copyOf(sup, sup.length);
        return r;
    }

    /**
     *
     * @param i must be between 0 and dimensionality-1
     * @return
     */
    public int getInfCoord(int i) {
        return inf[i];
    }

    /**
     *
     * @param i must be between 0 and dimensionality-1
     * @return
     */
    public int getSupCoord(int i) {
        return sup[i];
    }

    public int[] getInf() {
        return inf;
    }

    public int[] getSup() {
        return sup;
    }

    /**
     *
     * @param i must be between 0 and dimensionality-1
     * @return
     */
    public void setInfCoord(int i, int v) {
        inf[i] = v;
    }

    /**
     *
     * @param i must be between 0 and dimensionality-1
     * @return
     */
    public void setSupCoord(int i, int v) {
        sup[i] = v;
    }

    public Range[] getSplit(int i, int v) {
        Range[] s = new Range[2];
        s[0] = getCopy();
        s[0].sup[i] = v;
        s[1] = getCopy();
        s[1].inf[i] = v+1;
        return s;
    }

    /**
     *
     * @param i must be between 0 and dimensionality-1
     * @return
     */
    public double getVolume() {
        double v = 1;
        for (int i = 0; i < inf.length; i++) {
            v *= getWidth(i);
        }
        return v;
    }

    public int getWidth(int i) {
        return sup[i] - inf[i] + 1;
    }

    public boolean contains(double[] p) {
        for (int i = 0; i < inf.length; i++) {
            if (p[i] < inf[i] || p[i] > sup[i]) {
                return false;
            }
        }
        return true;
    }
    
    public boolean contains(int[] p) {
        for (int i = 0; i < inf.length; i++) {
            if (p[i] < inf[i] || p[i] > sup[i]) {
                return false;
            }
        }
        return true;
    }    

    public boolean contains(Range r) {
        for (int i = 0; i < inf.length; i++) {
            if (r.inf[i] < inf[i] || r.sup[i] > sup[i]) {
                return false;
            }
        }
        return true;
    }

    public double[] getRandomInnerInstance(Random rnd) {
        double[] res = new double[inf.length];
        for (int k = 0; k < inf.length; k++) {
            res[k] = inf[k] + rnd.nextDouble() * (sup[k] - inf[k]);
        }
        return res;
    }

    public int getDimensionality() {
        return inf.length;
    }

    public double[] getCenter() {
        double[] coord = new double[inf.length];
        for (int i = 0; i < inf.length; i++) {
            coord[i] = (inf[i] + sup[i]) / 2;
        }
        return coord;
    }

    @Override
    public String toString() {
        String s = "";
        for (int i = 0; i < inf.length; i++) {
            s += inf[i] + ".." + sup[i] + " ";
        }
        return s;
    }
    
    public ArrayList<Integer> toSingleList() {
        ArrayList<Integer> res=new ArrayList<>();
        for (int i=0; i<inf.length; i++) {
            res.add(inf[i]);
        }
        for (int i=0; i<sup.length; i++) {
            res.add(sup[i]);
        }
        return res;
    }
}