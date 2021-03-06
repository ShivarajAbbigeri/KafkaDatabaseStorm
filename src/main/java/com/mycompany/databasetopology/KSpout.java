/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.databasetopology;




import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
//import org.apache.storm.spout.MultiScheme;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.storm.StormSubmitter;

import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.kafka.*;

        
public class KSpout {
   public static void main(String[] args) throws Exception{
      Config config = new Config();
      config.setDebug(true);
      config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
      String zkConnString = "192.168.56.103:2080";
      String topic = "sales-ord";
      BrokerHosts hosts = new ZkHosts(zkConnString);
      
      SpoutConfig kafkaSpoutConfig = new SpoutConfig (hosts, topic, "/" + topic,    
         UUID.randomUUID().toString());
      kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
      kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
//      kafkaSpoutConfig.forceFromStart = true;
      kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig));
      builder.setBolt("word-spitter", new SplitBolt()).shuffleGrouping("kafka-spout");
      builder.setBolt("forwardToKafka", new WriteToKafka()).shuffleGrouping("word-spitter");   
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("KSpout", config, builder.createTopology());

      Thread.sleep(100000);
      
      cluster.shutdown();
   }
}

