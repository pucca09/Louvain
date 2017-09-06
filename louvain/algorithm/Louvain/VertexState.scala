package wtist.algorithm.Louvain

/**
  * Louvain vertex state
  * Contains all information needed for louvain community detection
  */
class VertexState extends Serializable{

  var community = -1L
  var communitySigmaTot = 0D
  var internalWeight = 0D  // self edges
  var nodeWeight = 0D;  //out degree
  var changed = false

  override def toString(): String = {
//    "{community:"+community+",communitySigmaTot:"+communitySigmaTot+
//      ",internalWeight:"+internalWeight+",nodeWeight:"+nodeWeight+"}"
    community.toString
  }
}
