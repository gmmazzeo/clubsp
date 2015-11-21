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

import edu.ucla.cs.scai.clubsp.commons.RegisteredWorker;
import edu.ucla.cs.scai.clubsp.messages.ClubsPMessage;
import edu.ucla.cs.scai.clubsp.messages.WorkerConnectionRequest;
import edu.ucla.cs.scai.clustering.syntheticgenerator.MultidimensionalGaussianGenerator;
import static edu.ucla.cs.scai.clustering.syntheticgenerator.MultidimensionalGaussianGenerator.createImage;
import static edu.ucla.cs.scai.clustering.syntheticgenerator.MultidimensionalGaussianGenerator.shuffleDataset;
import edu.ucla.cs.scai.clustering.syntheticgenerator.Range;
import java.io.File;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

/**
 *
 * @author Giuseppe M. Mazzeo <mazzeo@cs.ucla.edu>
 */
public class Worker {

    int port;
    String datasetsPath;
    String masterIp;
    int masterPort;
    final HashMap<String, WorkerExecution> workerExecutions = new HashMap<>();
    HashMap<String, RegisteredWorker> workers = new HashMap<>();
    String ip;
    String id;

    public Worker(int port, String datasetsPath, String masterIp, int masterPort) throws Exception {
        this.port = port;
        File f = new File(datasetsPath);
        if (!f.exists() || !f.isDirectory()) {
            System.out.println("Directory " + datasetsPath + " not found");
            System.out.println("Worker terminated");
            throw new Exception("Wrong path " + datasetsPath);
        }
        this.datasetsPath = datasetsPath;
        if (!datasetsPath.endsWith(File.pathSeparator)) {
            datasetsPath += File.pathSeparator;
        }
        this.masterIp = masterIp;
        this.masterPort = masterPort;
    }

    public void start() throws Exception {
        try (ServerSocket listener = new ServerSocket(port);) {
            System.out.println("Worker started. Waiting for an id");
            //register with the master
            try (Socket s = new Socket(masterIp, masterPort);
                    ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());) {
                out.writeObject(new WorkerConnectionRequest(port));
            } catch (Exception e) {
                System.out.println("Connection to master " + masterIp + ":" + masterPort + " failed");
                e.printStackTrace();
                System.out.println("Worker terminated");
                System.exit(0);
            }
            while (true) {
                Socket socket = listener.accept();
                new WorkerIncomingMessageHandler(this, socket).start();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Worker terminated");
        }
    }

    public void sendMessageToMaster(ClubsPMessage message) {
        try (Socket s = new Socket(masterIp, masterPort);
                ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream())) {
            out.writeObject(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendMessageToWorker(String workerId, ClubsPMessage message) {
        RegisteredWorker worker = workers.get(workerId);
        try (Socket s = new Socket(worker.ip, worker.port);
                ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream())) {
            out.writeObject(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int getPort() {
        return port;
    }

    public String getIp() {
        return ip;
    }

    public String getId() {
        return id;
    }

    //args[0] is the port used by this Worker
    //args[1] is the local path with the datasets
    //args[2] is the ip of the master
    //args[3] is the port of the master
    public static void main(String[] args) {
        if (args == null || args.length != 4) {
            args = new String[]{"" + (10000 + (int) (Math.random() * 10000)), "/home/massimo/", "localhost"/*"131.179.64.170"*/, "9090"};
            //args = new String[]{"" + (10000 + (int) (Math.random() * 10000)), "/home/massimo/", "131.179.64.145"/*"131.179.64.170"*/, "9090"};
            //System.out.println("Parameters needed: port dataSetsPath masterIp masterPort");
            //System.out.println("Worker terminated");
            //return;
        }
        int port;
        try {
            port = Integer.parseInt(args[0]);
        } catch (Exception e) {
            System.out.println("Port " + args[0] + " not valid");
            System.out.println("Worker terminated");
            return;
        }
        int masterPort;
        try {
            masterPort = Integer.parseInt(args[3]);
        } catch (Exception e) {
            System.out.println("Master port " + args[3] + " not valid");
            System.out.println("Worker terminated");
            return;
        }
        try {
            new Worker(port, args[1], args[2], masterPort).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void initId(String assignedId, Exception e) {
        if (assignedId == null) {
            System.out.println("Registration failed: " + e.getMessage());
        } else {
            System.out.println("Received id: " + assignedId);
            id = assignedId;
        }
    }

    //start a new clustering execution
    public synchronized void initExecution(String executionId, String dataSetId, HashMap<String, RegisteredWorker> workers, double scaleFactor) {
        this.workers.putAll(workers);
        WorkerExecution newExec = new WorkerExecution(this, executionId, dataSetId, scaleFactor);
        workerExecutions.put(executionId, newExec);
    }

    public void doGeneration(int nOfTuples, int domainWidth, double noiseRatio, int[][] centers, int[][] radii) {
        int nOfClusters = centers.length;
        int dimensionality = centers[0].length;
        String fileName = datasetsPath + nOfTuples + "p_" + dimensionality + "d_" + nOfClusters + "c_" + noiseRatio + "n.data";
        System.out.println("Generating " + fileName);
        int[] inf = new int[dimensionality];
        int[] sup = new int[dimensionality];
        for (int i = 0; i < dimensionality; i++) {
            inf[i] = 0;
            sup[i] = domainWidth - 1;
        }

        Range r = new Range(inf, sup);
        try {
            MultidimensionalGaussianGenerator.generate(fileName, r, nOfTuples, centers, radii, noiseRatio, 100);
            createImage(fileName, fileName + "_labels", r, true);
            String fileNameOut = datasetsPath + nOfTuples + "p_" + dimensionality + "d_" + nOfClusters + "c_" + noiseRatio + "n.data";
            shuffleDataset(fileName, fileName + "_labels", fileNameOut, fileNameOut + "_labels", new Random());
        } catch (Exception e) {
            e.printStackTrace();;
        }
    }
}
