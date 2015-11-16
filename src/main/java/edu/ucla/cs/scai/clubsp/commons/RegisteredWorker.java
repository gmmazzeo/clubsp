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
public class RegisteredWorker implements Serializable {

    public String id;
    public String ip;
    public int port;

    public RegisteredWorker(String id, String ip, int port) {
        this.id = id;
        this.ip = ip;
        this.port = port;
    }

}
