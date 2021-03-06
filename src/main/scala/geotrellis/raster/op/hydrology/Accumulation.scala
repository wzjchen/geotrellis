package geotrellis.raster.op.hydrology

import geotrellis._
import scala.collection.mutable._
import geotrellis.raster._


object Accumulation{
  //checks if the encoded value includes the dirrecrion
  def doesFlow(value:Int,dir:Int):Boolean= {
    if(value<dir){
      return false 
    }
    else{
      if((value >> (math.floor(math.log(dir)/math.log(2)).toInt))%2 == 1)
        return true
      else
        return false
    }
  }

  def calcAcc(column:Int,row:Int,data:IntArrayRasterData,flowDirrection:Raster)={
    var c = column
    var r = row
    var sum = data.get(c,r)
    val cols = data.cols
    val rows = data.rows
    var flag = 0

    //map the flow dirrection that each  neighboring cell must have to flow into testthe current cell 
    val map = Map[Int,(Int,Int)](
      16 -> (c+1,r),
      32 -> (c+1,r+1),
      64 -> (c,r+1),
      128 -> (c-1,r+1),
      1 -> (c-1,r),
      2 -> (c-1,r-1),
      4 -> (c,r-1),
      8 -> (c+1,r-1))

    if(sum == -1){

      sum =0
      var  stack= new ArrayStack[(Int,Int)]()
      var len = 0
      stack.push((c,r))
      stack.push((c,r))

      while(! stack.isEmpty || data.get(c,r) == -1){
        sum = 0
        flag = 0  
        //the neighboring cell
        var n = map(16)
        //right neighbour	
        if(c+1<cols && doesFlow(flowDirrection.get(n._1,n._2),16)){
          if(data.get(n._1,n._2) == -1){
          stack.push(n)
          flag = 1
        } else {
            sum= sum +data.get(n._1,n._2)
          }
        }
        //bottom right neighbor
        n = map(32)
        if(c+1<cols && r+1<rows && doesFlow(flowDirrection.get(n._1,n._2),32)){
          if(data.get(n._1,n._2) == -1){
            stack.push(n)
            flag =1
          } else {
              sum = sum + data.get(n._1,n._2) +1
            }
        }

        //bottom neighbor
        n = map(64)
        if(r+1<rows && doesFlow(flowDirrection.get(c,r+1),64)){
          if( data.get(n._1,n._2) == -1){
            stack.push(n)
           flag =1
          }else {
            sum = sum + data.get(n._1,n._2) +1
          }
        }

        //bottom left neighbor
        n = map (128)
        if(c-1 >= 0 && r+1<rows && doesFlow(flowDirrection.get(c-1,r+1),128)){
          if(data.get(c-1,r+1) == -1){
            stack.push((c-1,r+1))
            flag = 1 
          }else{
            sum = sum + data.get(c-1,r+1)  +1
          }
        }

        //left neighbor
        n = map(1) 
        if( c-1 >= 0 && doesFlow(flowDirrection.get(c-1,r),1)){
          if(data.get(c-1,r) == -1){
            stack.push((c-1,r))
            flag = 1
          }else{
             sum = sum + data.get(c-1,r) +1
            }
        }

        //top left neighbor
        n = map(2)
        if(c-1 >= 0 && r-1 >= 0 && doesFlow(flowDirrection.get(c-1,r-1),2)){
          if(data.get(c-1,r-1) == -1){
            stack.push((c-1,r-1))
            flag = 1
          }else{
           sum = sum + data.get(c-1,r-1) +1   
          }	
        }

        //top neighbor
        n = map(4) 
        if(r-1 >= 0 && doesFlow(flowDirrection.get(c,r-1),4)){
          if(data.get(c,r-1) == -1){
            stack.push((c,r-1))
            flag = 1
          }else{
            sum = sum + data.get(c,r-1) +1   
          }
        }

        //top right neighbor
        n = map(8)
        if(c+1<cols && r-1>=0 && doesFlow(flowDirrection.get(c+1,r-1),8)){
          if(data.get(c+1,r-1)== -1){
            stack.push((c+1,r-1))
            flag =1
          }else{
            sum = sum + data.get(c+1,r-1) +1   
          }	
        }

        //set the calculated sum as the accumulation 
        if (flag == 0){ 
         data.set(c,r,sum)
        }
        if(!stack.isEmpty){
         val t = stack.pop
         c= t._1
         r= t._2
        }
      }
    }
  }
}

case class Accumulation(flowDirrection:Op[Raster]) extends Op1(flowDirrection)({
  flowDirrection =>

    val cols = flowDirrection.cols
    val rows = flowDirrection.rows
    val data = IntArrayRasterData(Array.ofDim[Int](cols*rows),cols,rows)

    var c= 0
    var r= 0

    while(c < cols){
     r=0
      while(r < rows){		
        data.set(c,r,-1)
        r=r+1
      }
      c=c+1
    }

    c= 0
    while(c < cols){
      r=0
      while(r < rows){
        Accumulation.calcAcc(c,r,data,flowDirrection)
        r = r+1
      }
      c= c+1
    }
    r= 0
    while(r < rows){
      c=0
      while(c < cols){
        System.out.print(data.get(c,r) + ", ")
        c = c+1 
      }
      System.out.println()
      r= r+1 
    }

    //convert the IntArrayflowDirrection to a flowDirrection
    Result(Raster( data , flowDirrection.rasterExtent))
})