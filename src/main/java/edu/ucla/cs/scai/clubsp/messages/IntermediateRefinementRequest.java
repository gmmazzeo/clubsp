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
package edu.ucla.cs.scai.clubsp.messages;

import java.util.ArrayList;

/**
 *
 * @author Giuseppe M. Mazzeo <mazzeo@cs.ucla.edu>
 */
public class IntermediateRefinementRequest extends ExecutionMessage {

    public ArrayList<Integer> clusterBlockIds;
    public ArrayList<double[]> detailedRadii;
    public ArrayList<double[]> centroids;

    public IntermediateRefinementRequest(String executionId, ArrayList<Integer> clusterBlockIds, ArrayList<double[]> centroids, ArrayList<double[]> detailedRadii) {
        super(executionId);
        this.clusterBlockIds = clusterBlockIds;
        this.centroids = centroids;
        this.detailedRadii = detailedRadii;
    }

    @Override
    public String toString() {
        return "IntermediateRefinementRequest{" + "clusterBlockIds=" + clusterBlockIds + ", detailedRadii=" + detailedRadii + ", originalCentroids=" + centroids + '}';
    }

}
