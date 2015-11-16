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
package edu.ucla.cs.scai.clubsp;

import edu.ucla.cs.scai.clubsp.messages.StartClusteringRequest;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 *
 * @author Giuseppe M. Mazzeo <mazzeo@cs.ucla.edu>
 */
public class ClubsPStarter {

    public static void main(String args[]) {
        if (args == null || args.length != 3) {
            //args = new String[]{"localhost", "9090", "test.data", "localhost"/*"131.179.64.170"*/, "9090"};
            args = new String[]{"131.179.64.145", "9090", "test.data"};
            //System.out.println("Parameters needed: masterIp masterPort dataSetId");
            //System.out.println("Starter terminated");
            //return;
        }
        int port;
        try {
            port = Integer.parseInt(args[1]);
        } catch (Exception e) {
            System.out.println("Port " + args[1] + " not valid");
            System.out.println("Starter terminated");
            return;
        }

        try (Socket s = new Socket(args[0], port);
                ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());) {
            out.writeObject(new StartClusteringRequest(args[2]));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
