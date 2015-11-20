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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

/**
 *
 * @author Giuseppe M. Mazzeo <mazzeo@cs.ucla.edu>
 */
public class Utils {

    public final static double RECIP_E = 0.36787944117144232159552377016147; // RECIP_E = (E^-1) = (1.0 / E)
    public final static double TWOPI = 6.283185307179586476925286766559;  // TWOPI = 2.0 * PI    

    //We don't care about the exact value if greater than 1
    public static double ellipticalRelativeDistanceWithLimit1(double[] center, double[] radius, double[] p) {
        double d = 0;
        for (int i = 0; i < p.length; i++) {
            d += Math.pow((center[i] - p[i]) / radius[i], 2);
            if (d>1) {
                return d;
            }
        }
        return d;
    }

    //We don't care about the exact value if greater than 1
    public static double ellipticalRelativeDistanceWithLimit1(double[] center, double[] radius, int[] p) {
        double d = 0;
        for (int i = 0; i < p.length; i++) {
            if (radius[i]==0) {
                continue;
            }
            d += Math.pow((center[i] - p[i]) / radius[i], 2);
            if (d>1) {
                return d;
            }
        }
        return d;
    }

    public static double distance(double[] a, double[] b) {
        double d = 0;
        for (int i = 0; i < a.length; i++) {
            d += Math.pow(a[i] - b[i], 2);
        }
        return Math.sqrt(d);
    }

    public static double distance(double[] a, int[] b) {
        double d = 0;
        for (int i = 0; i < a.length; i++) {
            d += Math.pow(a[i] - b[i], 2);
        }
        return Math.sqrt(d);
    }

    public double sqrDistance(double[] a, double[] b) {
        double res = 0;
        for (int i = 0; i < a.length; i++) {
            res += Math.pow(a[i] - b[i], 2);
        }
        return res;
    }

    public double sqrDistance(double[] a, int[] b) {
        double res = 0;
        for (int i = 0; i < a.length; i++) {
            res += Math.pow(a[i] - b[i], 2);
        }
        return res;
    }

    public static void add(double[] a, double[] b, double weight) {
        for (int i = 0; i < a.length; i++) {
            a[i] = a[i] + b[i] * weight;
        }
    }

    public static void add(double[] a, int[] b, double weight) {
        for (int i = 0; i < a.length; i++) {
            a[i] = a[i] + b[i] * weight;
        }
    }

    public static void addSqr(double[] a, double[] b, double weight) {
        for (int i = 0; i < a.length; i++) {
            a[i] = a[i] + Math.pow(b[i], 2) * weight;
        }
    }

    public static void addSqr(double[] a, int[] b, double weight) {
        for (int i = 0; i < a.length; i++) {
            a[i] = a[i] + Math.pow(b[i], 2) * weight;
        }
    }

    public static void multiply(double[] a, double f) {
        for (int i = 0; i < a.length; i++) {
            a[i] = a[i] * f;
        }
    }

    public static double[] copy(double[] src) {
        double[] dest = new double[src.length];
        System.arraycopy(src, 0, dest, 0, dest.length);
        return dest;
    }

    public static String write(double[] data) {
        String res = "[";
        if (data.length > 0) {
            res += data[0];
        }
        for (int i = 1; i < data.length; i++) {
            res += ", " + data[i];
        }
        res += "]";
        return res;
    }   

    public static double findNoiseThreshold(double[] v, int inf, int sup) {
        double[] values = new double[sup - inf + 1];
        for (int j = inf; j <= sup; j++) {
            values[j - inf] = v[j];
        }
        Arrays.sort(values);
        double[] variations = new double[values.length - 1];
        double ssd = 0;
        for (int j = 0; j < values.length - 1; j++) {
            variations[j] = values[j + 1] / values[j];
            ssd += variations[j];
        }

        double asd = variations.length > 0 ? ssd / variations.length : 0;
        double dsd = 0;
        for (int i = 0; i < variations.length; i++) {
            dsd += Math.pow(variations[i] - asd, 2);
        }
        dsd /= variations.length > 0 ? variations.length : 1;
        dsd = Math.sqrt(dsd);

        double variationThreshold = asd + dsd;

        int indexThreshold = 0;

        while (indexThreshold < variations.length && variations[indexThreshold] <= variationThreshold) {
            indexThreshold++;
        }

        return values[indexThreshold];
    }    
}