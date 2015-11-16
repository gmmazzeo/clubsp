/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ucla.cs.scai.clubsp.master;

import edu.ucla.cs.scai.clubsp.commons.RegisteredWorker;
import edu.ucla.cs.scai.clubsp.messages.ClubsPMessage;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

/**
 *
 * @author Giuseppe M. Mazzeo <mazzeo@cs.ucla.edu>
 */
public class Master {

    int port;
    final HashMap<String, RegisteredWorker> registeredWorkers = new HashMap<>();
    final HashMap<String, MasterExecution> masterExecutions = new HashMap<>();

    public Master(int port) throws Exception {
        this.port = port;
    }

    //start listening on the port specified with the constructor
    public void start() throws Exception {

        try (ServerSocket listener = new ServerSocket(port);) {
            System.out.println("Master started at " + listener.getInetAddress().toString() + ":" + listener.getLocalPort());
            while (true) {
                Socket socket = listener.accept();
                new MasterIncomingMessageHandler(socket, this).start();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Master terminated");
        }
    }

    //add the worker to the lists of registered worker
    //assign an id and send it to the worker
    public synchronized String registerWorker(String ip, int port) {
        String id = "w" + (registeredWorkers.size() + 1);
        registeredWorkers.put(id, new RegisteredWorker(id, ip, port));
        System.out.println("Worker " + id + " registered: " + ip + ":" + port);
        return id;
    }

    //send a message to a registered worker
    public void sendMessage(String workerId, ClubsPMessage message) {
        RegisteredWorker w = registeredWorkers.get(workerId);
        try (Socket socket = new Socket(w.ip, w.port);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
            out.writeObject(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //start a new clustering execution
    public synchronized void initExecution(String dataSetId, double scaleFactor) {
        if (registeredWorkers.isEmpty()) {
            System.out.println("No workers available, sorry!");
        } else {
            MasterExecution newExec = new MasterExecution(this, dataSetId, scaleFactor);
            masterExecutions.put(newExec.executionId, newExec);
        }
    }

    //args[0] is the port used by the master
    public static void main(String[] args) {
        if (args == null || args.length != 1) {
            args = new String[]{"9090"};
            //System.out.println("Parameters needed: port");
            //return;
        }
        int port;
        try {
            port = Integer.parseInt(args[0]);
        } catch (Exception e) {
            System.out.println("Port " + args[0] + " not valid");
            System.out.println("Master terminated");
            return;
        }
        try {
            new Master(port).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
