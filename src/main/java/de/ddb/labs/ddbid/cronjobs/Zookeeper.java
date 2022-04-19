/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package de.ddb.labs.ddbid.cronjobs;

import org.springframework.beans.factory.annotation.Autowired;

public class Zookeeper implements Runnable {
       
    @Autowired
    private DataDumperItem dataDumperItem;
    
    @Autowired
    private DumpComparer dumpComparer;
    
    
    @Override
    public void run() {
        dataDumperItem.run();
        dumpComparer.run();
    }
    
}
