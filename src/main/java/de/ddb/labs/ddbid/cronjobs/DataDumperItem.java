/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package de.ddb.labs.ddbid.cronjobs;

import static de.ddb.labs.ddbid.cronjob.CronJob.ENTITYCOUNT;
import de.ddb.labs.ddbid.model.item.ItemDoc;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class DataDumperItem extends DataDumper implements Runnable {

       private static final String QUERY = "/search/index/search/select?q=*:*&wt=json&fl=id,provider_item_id,label,provider_id,supplier_id,dataset_id,sector_fct&sort=id ASC&rows=" + ENTITYCOUNT;

    @Value(value = "${ddbid.datapath.item}")
    private String dataPath;

    private final Class<ItemDoc> doc = ItemDoc.class;
    
    @Override
    public void run() {
        
           try {
               super.createNewDump(QUERY, dataPath, doc);
           } catch (IOException | NoSuchMethodException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
               log.error("{}", ex.getMessage());
           }
    }
    
}
