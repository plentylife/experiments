import scala.util.Random

object Types {
  /** the int is userId, the index is coin id */
  type CoinTable = Array[Int]
  type AgentId = Int
  /** agent id to agent id */
  type Connections = Set[(Int, Int)]
  type TransactionId = Int
}

import Types._

/** from, to, coinId */
case class Transaction(id: TransactionId, from: Int, to: Int, coinId: Int)

object Main {
  val coinsPerAgent = 7
  val agentCount = 100
  val coinCount = coinsPerAgent * agentCount
  val agents = (0 until agentCount).toArray
  val coins = (0 until coinCount).toArray
  val baseTable: CoinTable = Functions.instantiateCoinTable(coinCount, coinsPerAgent)
  val coinTables: Array[CoinTable] = agents map { _ ⇒ baseTable.clone() }
  /** history of transactions that have already been recorded, by agent */
  val transactionHistories: Array[Set[TransactionId]] = (0 until agentCount).map{_ ⇒ Set[TransactionId]()}.toArray
  // how many links a transaction can cross
  val armsLength = 2

  val maxConnectionsMadeByUser = 3
//  val maxConnectionsAccepted = maxConnectionsMadeByUser * maxConnectionsMadeByUser
  val maxConnectionsAccepted = 15
  /** userId to userId */
  val globalConnections = Functions.instantiateConnectionMap(agents, 1, maxConnectionsMadeByUser)

  /** how many transactions to send in total */
  val numberOfTrials = 100


  /* INITIALIZATION */

  def main(args: Array[String]): Unit = {
    val directConnectionSizes = agents.map(id ⇒ Functions.findDirectConnections(id).size)
//        println(directConnectionSizes.mkString("\n"))
    println(s"Most connected with ${directConnectionSizes.max}")
    assert(directConnectionSizes.forall(_ <= maxConnectionsAccepted) )

    (0 until numberOfTrials) foreach { _ ⇒
      val randomAgentId = Random.nextInt(agentCount)
      Functions.transactAtRandom(randomAgentId, globalConnections, coinTables, armsLength, transactionHistories)
    }

    val changedTablesCount = agentCount - coinTables.map(_ sameElements baseTable).count(isTrue ⇒ isTrue)
    println(s"How many tables have shifted from the original: $changedTablesCount")
  }
}

/* FUNCTIONS*/

object Functions {
  var lastTransactionId: TransactionId = 0

  def transactAtRandom(agentFrom: Int, connections: Set[(Int, Int)], coinTables: Array[CoinTable], armsLength: Int,
                       transactionHistories: Array[Set[TransactionId]]): Unit
  = {
    val coinTableForAgent = coinTables(agentFrom)
    // index here is coin id
    val coins = coinTableForAgent.zipWithIndex collect { case (`agentFrom`, index) ⇒ index }
    // could be that the agent has no coins
    if (coins.isEmpty) return

    val coin = coins(Random.nextInt(coins.size))

    // finding users that this agent is connected to within arms length
    val possibleTargets = findAgentsWithinArmsLength(agentFrom, connections, armsLength).toSeq
    val to = possibleTargets(Random.nextInt(possibleTargets.size))

    val transaction = Transaction(lastTransactionId, from = agentFrom, to = to, coinId = coin)
    lastTransactionId += 1

    println(s"Agent ${agentFrom} transacting coin ${coin} to ${to}")

    recordTransaction(transaction, to, globalConnections = connections, coinTables = coinTables, armsLength =
      armsLength, transactionHistories)
  }

  /** does the actual accounting and verification */
  def recordTransaction(t: Transaction, recordingAgent: Int, globalConnections: Connections, coinTables:
  Array[CoinTable], armsLength: Int, transactionHistories: Array[Set[TransactionId]], linkCount: Int = 0): Unit = {
    val history = transactionHistories(recordingAgent)
    if (history contains t.id) {
//      println("\t skipping")
      return
    }

//    println(s"Agent $recordingAgent recording transaction ${t} at depth ${linkCount} / $armsLength")

    val receivingCoinTable = coinTables(recordingAgent)
    val coinOwner = receivingCoinTable(t.coinId)
    if (coinOwner == t.from) {
      // recording
      receivingCoinTable(t.coinId) = t.to
      transactionHistories(recordingAgent) = history + t.id

      // relaying the message
      if (linkCount < armsLength - 1) {
        val agentsWithinReach = findDirectConnections(recordingAgent)
        agentsWithinReach foreach { id ⇒
          recordTransaction(t, id, globalConnections, coinTables, armsLength, transactionHistories,
            linkCount = linkCount + 1)
        }
      }
    } else {
      println(s"Agent $recordingAgent : Conflict! Coin ${t.coinId} coming from ${t.from} belongs to ${coinOwner}")
    }
  }

  def findDirectConnections(agentFrom: Int): Set[Int] = {
    Main.globalConnections.collect {
      case (other, `agentFrom`) ⇒ other
      case (`agentFrom`, other) ⇒ other
    }
  }

  /** finds all agents that are within `armsLength` */
  def findAgentsWithinArmsLength(agentFrom: Int, allConnections: Set[(Int, Int)], armsLength: Int,
                                 linkCount: Int = 0): Set[Int] = {
    val agentsConnections = findDirectConnections(agentFrom)
    if (linkCount < armsLength - 1) {
      agentsConnections ++ agentsConnections.flatMap { id ⇒
        findAgentsWithinArmsLength(id, allConnections, armsLength,
          linkCount + 1)
      }
    } else {
      agentsConnections
    }
  }

  def instantiateCoinTable(coinCount: Int, coinsPerAgent: Int): Array[Int] = {
    var i = 0
    var agentCounter = 0
    val table = Stream.continually(-1).take(coinCount).toArray
    while (i + coinsPerAgent < coinCount) {
      (i until i + coinsPerAgent) foreach { coinId ⇒ table(coinId) = agentCounter }
      agentCounter += 1
      i += coinsPerAgent
    }
    table
  }


  def instantiateConnectionMap(agentIds: Array[Int], connectionsPerPersonMin: Int, connectionsPerPersonMax: Int):
  Set[(Int, Int)] = {
    val globalCons = collection.mutable.Set[(Int, Int)]()

    for (id ← agentIds) {
      val agentCons = instantiateConnectionMapForAgent(agentIds, connectionsPerPersonMin, connectionsPerPersonMax)
      globalCons ++= agentCons.map(c ⇒ id → c)
    }

    globalCons.toSet
  }

  def instantiateConnectionMapForAgent(agentIds: Array[Int], connectionsPerPersonMin: Int, connectionsPerPersonMax:
  Int): Array[Int] = {
    val (cMin, cMax) = (connectionsPerPersonMin, connectionsPerPersonMax)
    val connectionsCount: Int = Random.nextInt(cMax - cMin + 1) + cMin

    Random.shuffle(agentIds.toSeq).take(connectionsCount).toArray
  }
}