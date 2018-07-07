package com.mycompany.databasetopology;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.util.Map;
import com.google.gson.Gson;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.task.TopologyContext;


public class SplitBolt implements IRichBolt {
   private OutputCollector collector;
   Gson gson ;
   @Override
   public void prepare(Map stormConf, TopologyContext context,
      OutputCollector collector) {
      this.collector = collector;
      gson = new Gson();
   }
   
   @Override
   public void execute(Tuple input) {
      String sentence = input.getString(0);
      ord obj = gson.fromJson(sentence, ord.class);
      String out="{sup_name:"+ obj.sup_name+", amt: "+obj.amt+"}";
      collector.emit(new Values(out));
         

      collector.ack(input);
   }
   
   @Override
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("output-records"));
   }

   @Override
   public void cleanup() {}
   
   @Override
   public Map<String, Object> getComponentConfiguration() {
      return null;
   }
   
}

class ord{
    int id;
    int sup_id;
    int cust_id;
    int amt;
    String sup_name;
    //String time;
    ord(){
        
    }
}