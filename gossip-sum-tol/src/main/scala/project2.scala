/**
 * @author Sumeet Pande UFID:4890-9873
 * Drumil Deshpande UFID:8359-8265
 */
package main
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Actor
import scala.collection.mutable.ArrayBuffer
import akka.actor.ActorRef
import akka.actor.ActorSystem
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import util.Random
import akka.actor.PoisonPill
import scala.util.Random
import scala.sys.Prop
import akka.actor.Props

/* case class Initialization */
case class reportStatus(nodeValue:Int,rnd_cnt:Int)
case class NodeFailure(failedNodeValue:Int)
case class gossipMsgPass(message: String)
case class PushSumMsgPass(s: Double, w: Double) 

/* Master Actor */
class MasterActor(totalNodes: Int, Actsys: ActorSystem) extends Actor {
  var num_nodes_rcvd_msg:Int = 0
  var i_time:Long = 0
  var total_time:Long = 0
  def receive = {
    
    //Initiating the master node
    case "InitateMaster" =>
      println("Master Node has been Initiated")
      
    //Each worker reports to the master that iy has received the message for the first time.
    //To find out how many workers in the system have received message atleast once.
    // Useful in finding coverage for each topology.
    case reportStatus(nodeValue:Int,rnd_cnt:Int) =>
      if(num_nodes_rcvd_msg==0)
      {
        startTimer
      } 
      num_nodes_rcvd_msg+=1  
      //println("nodes reported: "+num_nodes_rcvd_msg)
      
    // Convergence criteria reached.
    case "shutProg" =>
      println("/* Final Result Parameters */")
      println("System has converged")
      println("Number of nodes which have received messages = "+num_nodes_rcvd_msg)
      total_time = System.currentTimeMillis - i_time
      println("Time taken to converge : "+total_time+" Milliseconds")
      println("Exiting")
      Actsys.shutdown      
  }
  
   def startTimer = {
      println("Starting Timer")
      i_time = System.currentTimeMillis
    }
}

/* Worker Actor */
class WorkerActor (totalNodes: Int, topology: String, algorithm: String, nodeValue: Int, Master: ActorRef) extends Actor{
    
   var neighbourList= new ArrayBuffer[Int]
   var msg_cnt:Int = 0
   var rnd_cnt:Int = 0
   var node_msg:String = null
   var msg_received:Boolean = false
   var s:Double=nodeValue.toDouble;
   var w:Double=1.0
   var count:Int=0
   var oldratio:Double=1.0
   var newratio: Double=1.0
   var push_flag:Boolean =false
   
   def receive ={
     
     case NodeFailure(failedNodeValue : Int)=>
       /*for(k<-0 to neighbourList.length-1)
         {
           println("n1: "+nodeValue+" "+neighbourList.apply(k))
         }*/
       var k:Int=0
       var check = true
      //Once we node which node is dead , all the other nodes have to make sure that the 
      // dead node is removed fromtheir neighbour list so that it does not relay the message
      // to dead node otherwise the message will be lostand the program will not terminate. 
       while((k<neighbourList.length-1) && check)
       {
         if(neighbourList.apply(k)==failedNodeValue)
         {
           //println("Nodevalue: " + nodeValue + " FailedNode " + failedNodeValue)
           neighbourList-=failedNodeValue
           check = false
         }
         k+=1
       }
       // The method where thenode becomes dead
       if(nodeValue==failedNodeValue)
       {
         context.stop(self)
       }
     
     case PushSumMsgPass (s_rec : Double, w_rec :Double) =>
       {
         s=s+s_rec
         w=w+w_rec
         //Reporting to master when passed a message for the first time.
         if(!push_flag)
         {
           Master ! reportStatus(nodeValue,rnd_cnt)
           push_flag=true
         }
         //Checking if the difference is less than stipulated value for 3 consecutive iterations
         if (count<3)
         {
           oldratio = newratio
           newratio = s/w
           //setting the counter to ensure it is incremented only for 3 consecutive iterations
           if((newratio-oldratio).abs < 0.0000000001)
           {
             count+=1
           }
           else
           {
             count=0
           }
           s=s/2
           w=w/2
           PassMsg
         }
         else
         // Shutting the program since the condition is satisfied for more than 3
         // consecutive iterations. 
         {
           push_flag = false
           Master ! "shutProg"
         }
       }
     
     case gossipMsgPass(message:String) =>
       //Reporting to master when passed a message for the first time.
       if(msg_cnt==0)
       {
         msg_received = true
         msg_cnt+=1
         node_msg = message
         Master ! reportStatus(nodeValue,rnd_cnt)
         PassMsg
       }
       //Incrementing the counter so that we keep track of the number of times a message has
       //been received bya node.
       else if(msg_cnt<20)
       {
         msg_cnt+=1
         node_msg = message
         PassMsg
       }
       else
       {
         msg_cnt+=1
         msg_received = false
         Master ! "shutProg"
       }
         
     //Building topology as per user input.  
     case "BuildTopology" =>
       CreateTopology                    
                    
     def CreateTopology = {
       topology match {
  
        case "line" =>
          //left neighbour except for first node
          if (nodeValue - 1 >= 0)
            neighbourList += nodeValue - 1 
          //right neighbour except for the last node  
          if (nodeValue + 1 < totalNodes)
            neighbourList += nodeValue + 1 
        
        // All other nodes except itself are neighbors
        case "full" =>
          for (j <- 0 to totalNodes - 1) 
          {
            if (j != nodeValue)
              neighbourList += j
          }
        
        // We have basically derieved the code for 3d Topology by adding a dimension to the
        // 2d Grid.Since at max there will be 6 neighbours for any node in 3-d there will be 
        // 6 conditions. The number of levels in our 3-d topology will be cuberoort(cubert) 
        // of total nodes and at each level there will be exactly square(cuberoot) nodes.  
        case "3d" =>
          //No of levels: Cuberoot of nodes
          var cubert: Int = Math.cbrt(totalNodes).toInt
          //scr is basically square of cuberoot i.e. total number of nodes at each levels
          var scr: Int = Math.pow(cubert, 2).toInt
          
          //left neighbour in that level
          if (nodeValue % cubert != 0)
            neighbourList += nodeValue - 1 
          //right neighbour in that level  
          if (nodeValue % cubert != cubert - 1)
            neighbourList += nodeValue + 1 
          //above neighbour in that level  
          if (((nodeValue % scr) / cubert) > 0)
            neighbourList += nodeValue - cubert 
          //below neighbour in that level  
          if (((nodeValue % scr) / cubert) < (cubert - 1))
            neighbourList += nodeValue + cubert 
          //upper level neighbour  
          if (nodeValue>=scr)  
            neighbourList += nodeValue - scr 
          //down level neighbour  
          if (nodeValue<(totalNodes-scr))  
            neighbourList += nodeValue + scr 
  
            
            
        case "imp3d" =>
          // same as perfect 3d with random edge connection added       
          var cubert: Int = Math.cbrt(totalNodes).toInt
          var scr: Int = Math.pow(cubert,2).toInt
          if (nodeValue % cubert != 0)
            neighbourList += nodeValue - 1 
          if (nodeValue % cubert != cubert - 1)
            neighbourList += nodeValue + 1 
          if (((nodeValue % scr) / cubert) > 0)
            neighbourList += nodeValue - cubert
          if (((nodeValue % scr) / cubert) < (cubert - 1))
            neighbourList += nodeValue + cubert 
          if (nodeValue>=scr)  
            neighbourList += nodeValue - scr
          if (nodeValue<(totalNodes-scr))  
            neighbourList += nodeValue + scr 
          //add random neighbour 
          var r = Random.nextInt(totalNodes-1)
          while (r==nodeValue) 
          {
            r = Random.nextInt(totalNodes-1)
          }
          neighbourList += r 
      }
    }
  }

   //Method for passing message from this node to another random node in the neighbor array
   def PassMsg = {
     
     algorithm match {
     case "gossip" =>
       if(msg_received)
       {
           var randGossip:Int = 0
           if(topology.equals("line"))
           {
             randGossip = Random.nextInt(neighbourList.length)
           }
           else
             randGossip = Random.nextInt(neighbourList.length-1)
           var r_node = "node_"+neighbourList.apply(randGossip).toString
           val node = context.actorSelection("akka://GossipSimulator/user/"+r_node)
           //println("Passing msg from: "+nodeValue+" to "+r_node)
           node ! gossipMsgPass(node_msg)
       }
    
     case "pushsum" =>
       var randPS:Int = 0
       if(topology.equals("line"))
       {
          randPS = Random.nextInt(neighbourList.length)
       }
       else
         randPS = Random.nextInt(neighbourList.length-1) 
       var randNode= "node_"+neighbourList.apply(randPS).toString
       val node = context.actorSelection("akka://GossipSimulator/user/"+randNode)
       node ! PushSumMsgPass(s,w) 
     }
   } 
}

/* Main object */
object project2 { 
  def main(args: Array[String]) {
        var starttime:Long=System.currentTimeMillis 
        // To check if the user has inputed the correct arguments
        if (args.length != 3) 
        {
          println("Input arguements not entered correctly")
        }
        else
        // Correct Arguments entered.
        {
            println("Input Arguements Enetered. Gossip Simulator started ")
            var InputNodes = args(0).toInt
            var topology = args(1).toLowerCase()
            var algorithm = args(2).toLowerCase()
            var system = ActorSystem("GossipSimulator")

           // If topology contains 3-d then we have to make sure that the no of nodes is a cube.Else 
           // we have to round it off to nearest ceiling cube number.So that at any given moment of time
           // we have more number of nodes as compared to user input nodes.
            if (topology.contains("3d"))
            {
              InputNodes=getNextPerfectCube(InputNodes)
              println("3D topology required.The value will be rounded of to next perfect cube")
              println("The new number of nodes to implement 3D topology : "+InputNodes)
            }
            //Method to round off the number of nodes to the next perfect cube
            def getNextPerfectCube (numNodes:Int):Int=
            {
              //Taking the ceiling for cuberoot
              var newNodes:Int=numNodes
              var cuberoot=Math.cbrt(numNodes).ceil.toInt
              newNodes = Math.pow(cuberoot,3).toInt
              return newNodes
            }
            // Initiating the boss actor which will be used for termination
            val Master = system.actorOf(Props(new MasterActor(InputNodes.toInt, system)), name = "Master")
            Master ! "InitateMaster"
            
            //Instantiating the nodes 
            println("Instantiating the " +InputNodes +" for building " + topology + " topology")
            for (i<-0 to InputNodes.toInt-1 )
            {
              var workerNodes=system.actorOf(Props(new WorkerActor(InputNodes.toInt,topology, algorithm, i, Master)),name = "node_"+i.toString)
              workerNodes ! "BuildTopology"
            }
            
            //Setting up the start node
             var startNode = system.actorFor("/user/node_0")
            //println("first node :: " + startNode.path + '\n')
             println("Setting up the first node for Message Circulation")
             
             if (algorithm.equals("gossip"))
             {
               startNode ! gossipMsgPass("Rumor Started")
               println("Gossip Algorithm Selected for Communication")
             } 
            else if (algorithm.equals("pushsum"))
            {
              startNode ! PushSumMsgPass(0.0, 0.0)
              println("Push Sum Algorithm Selected for Communication")
            }
             
            if(System.currentTimeMillis-starttime>=5)
            {              
              failnode
            }
            
            //
            def failnode ={
               var maxfailnodes:Int=0 
               //Capping the max no of failure nodes to 3% of total nodes.
               if (InputNodes<100)
                 maxfailnodes=2
               else
               {
                 maxfailnodes =(InputNodes*0.03).toInt
               }
               println("Node Failure Sequence Started")
               var randtime=Random.nextInt(maxfailnodes)
               if(randtime==0)
                 randtime=1
               println("Maximum Possible No of failure nodes " + maxfailnodes)
               for (a<-0 to randtime-1)                 
               { 
                 //For each time a node fails we communicate to all other nodes that the 
                 //particular node has failed so that all other nodes can update their
                 //neighbor list accordingly so that they do not realy message to the 
                 //dead node.
                 var randnodetofail:Int=Random.nextInt(InputNodes)
                 for (b<-0 to InputNodes-1)
                 {  
                   println("Communicating to all nodes : Node" + randnodetofail +" has failed.")
                   var failedNode = system.actorFor("/user/node_"+b.toString)
                   failedNode!NodeFailure(randnodetofail.toInt)
                 }    
                 
               }
              }
        }
      }
}
