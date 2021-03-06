package geotrellis.process.actors

import geotrellis._
import geotrellis.process._
import akka.actor._


/*******************************************************
 * External messages sent from GeoTrellis land (non-actors)
 *******************************************************
 */

/**
 * External message to compute the given operation and return result to sender.
 * Run child operations using default local dispatcher.
 */
case class Run(op:Operation[_])

/********************
 * Internal messages 
 ********************
 */

/**
 * Internal message to run the provided op and send the result to the client.
 */
private[actors] case class RunOperation[T](op: Operation[T], 
                                           pos: Int, 
                                           client: ActorRef)

/**
 * Internal message to compute the provided args (if necessary), invoke the
 * provided callback with the computed args, and send the result to the client.
 */
private[actors] case class RunCallback[T](args:Args, 
                                           pos:Int, 
                                           cb:Callback[T], 
                                           client:ActorRef, 
                                           history:History)

/**
 * Message used to send result values. Used internally.
 */
private[process] case class PositionedResult[T](result:InternalOperationResult[T], pos: Int)
