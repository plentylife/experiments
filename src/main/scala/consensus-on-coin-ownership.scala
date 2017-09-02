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
  /** for emulating how humans tend to make connections that are clustered. */
  val agentConnectionRange = 20
  val coinCount = coinsPerAgent * agentCount
  val agents = (0 until agentCount).toArray
  val coins = (0 until coinCount).toArray
  /** the one true ledger */
  val truthLedger: CoinTable = Functions.instantiateCoinTable(coinCount, coinsPerAgent)
  /** the original table (ledger) that the system starts out with */
  val originalLedger = truthLedger.clone()
  val coinTables: Array[CoinTable] = agents map { _ ⇒ truthLedger.clone() }
  /** history of transactions that have already been recorded, by agent */
  val transactionHistories: Array[Set[TransactionId]] = (0 until agentCount).map{_ ⇒ Set[TransactionId]()}.toArray
  // how many links a transaction can cross
  val armsLengthOfNotifying = 3 // fixme! there must be a bug! at this length there should not be any mismatches
  val armsLengthOfTrading = 3
  // how many links can an inquiry pass
  val armsLengthForVoting = 4

  val maxConnectionsMadeByUser = 3
  val minConnectionsMadeByUser = 2
//  val maxConnectionsAccepted = maxConnectionsMadeByUser * maxConnectionsMadeByUser
  val maxConnectionsAccepted = 15
  /** userId to userId */
  val globalConnections = Functions.instantiateConnectionMap(minConnectionsMadeByUser, maxConnectionsMadeByUser)

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
      Functions.transactAtRandom(randomAgentId, globalConnections, coinTables, armsLengthOfNotifying, transactionHistories)
    }

    val changedTablesCount = agentCount - coinTables.map(_ sameElements originalLedger).count(isTrue ⇒ isTrue)
    val synchedTables = coinTables.map(_ sameElements truthLedger).count(isTrue ⇒ isTrue)
    println(s"How many tables have shifted from the original: $changedTablesCount")
    println(s"How many tables are same as the truth ledger: $synchedTables")

  }
}

/* FUNCTIONS*/

object Functions {

  /* Transactions */

  var lastTransactionId: TransactionId = 0

  def transactAtRandom(agentFrom: Int, connections: Set[(Int, Int)], coinTables: Array[CoinTable], armsLength: Int,
                       transactionHistories: Array[Set[TransactionId]]): Unit
  = {
    val coinTableForAgent = coinTables(agentFrom)
    // index here is coin id
    val coins = coinTableForAgent.zipWithIndex collect { case (`agentFrom`, index) ⇒ index }
    // could be that the agent has no coins
    if (coins.isEmpty) return

    assert(coins map {c ⇒ Main.truthLedger(c) == agentFrom} forall identity, "all possible coins for sending must " +
      "belong to the sender")
    val coin = coins(Random.nextInt(coins.size))

    // finding users that this agent is connected to within arms length
    val possibleTargets = findAgentsWithinArmsLength(agentFrom, Main.armsLengthOfTrading).toSeq
    val to = possibleTargets(Random.nextInt(possibleTargets.size))

    val transaction = Transaction(lastTransactionId, from = agentFrom, to = to, coinId = coin)
    lastTransactionId += 1

//    println(s"Agent ${agentFrom} transacting coin ${coin} to ${to}")

    // both agents must record the transaction
    recordTransaction(transaction, to, globalConnections = connections, coinTables = coinTables,
      transactionHistories)
    recordTransaction(transaction, agentFrom, globalConnections = connections, coinTables = coinTables,
      transactionHistories)
    assert(coinTableForAgent(coin) == to)
    Main.truthLedger(coin) = to
  }

  /** does the actual accounting and verification */
  def recordTransaction(t: Transaction, recordingAgent: Int, globalConnections: Connections, coinTables:
  Array[CoinTable], transactionHistories: Array[Set[TransactionId]], linkCount: Int = 0): Unit = {
    val history = transactionHistories(recordingAgent)
    if (history contains t.id) {
//      println("\t skipping")
      return
    }

//    println(s"Agent $recordingAgent recording transaction ${t} at depth ${linkCount} / $armsLength")

    val receivingCoinTable = coinTables(recordingAgent)
    var coinOwner = receivingCoinTable(t.coinId)
    if (coinOwner == t.from) {
      // recording
      receivingCoinTable(t.coinId) = t.to
      transactionHistories(recordingAgent) = history + t.id

      // relaying the message
      if (linkCount < Main.armsLengthOfNotifying - 1) {
        val agentsWithinReach = findDirectConnections(recordingAgent)
        agentsWithinReach foreach { id ⇒
          recordTransaction(t, id, globalConnections, coinTables, transactionHistories,
            linkCount = linkCount + 1)
        }
      }
    } else {
      println(s"""Agent $recordingAgent : Conflict! Coin ${t.coinId} heading to ${t.to} coming from ${t.from}
        belongs to ${coinOwner}""")
      println(s"\t in truth coin belongs to ${Main.truthLedger(t.coinId)}")
      println(s"\t transaction id ${t.id}")

      coinOwner = whosCoin(t.coinId, recordingAgent, t)
      println(s"\t decided owner is $coinOwner")
      println(s"\t direct connections ${findDirectConnections(recordingAgent)}")

//      val beilvedOwnerBelieves = whosCoin(t.coinId, coinOwner)
      println(s"According to believed owner $coinOwner, coin belongs to ${Main.coinTables(coinOwner)(t.coinId)}")
      if (Main.truthLedger(t.coinId) == coinOwner) println("match")
      else println("MISMATCH")
    }
  }

  /* Coins */
  /** @return the id of the believed to be owner (agent) */
  def whosCoin(coinId: Int, inquirerId: Int, transaction: Transaction): Int = {
    val opinions = Main.coinTables(inquirerId)(coinId) +: collectOpinions(coinId, inquirerId, transaction)
    val opinionsWithVoteCount = opinions.groupBy(identity).mapValues(_.size)
    println(s"\tAgent $inquirerId: Opinions are ${opinionsWithVoteCount.toSeq.sortBy(_._2).reverse.mkString(" ")}")
    opinionsWithVoteCount.maxBy(_._2)._1
  }

  /** @return a list of opinions from persons directly connected about who is the owner of `coinId` */
  def collectOpinions(coinId: Int, inquirerId: Int, transaction: Transaction): List[Int] = {
    val otherAgents = findAgentsWithinArmsLength(inquirerId, Main.armsLengthForVoting)
    println(s"\t other connected agents ${otherAgents.size}")
    otherAgents.toList map {opinionOf ⇒
      // so first checking if the transaction is accepted. if it is then count it as a vote for the last owner
      // todo. fix. this is a hack
      val transactionHistory = Main.transactionHistories(opinionOf)
      if (transactionHistory contains transaction.id) {
        transaction.from
      } else Main.coinTables(opinionOf)(coinId)
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

  /* Connections */

  def findDirectConnections(agentFrom: Int): Set[Int] = {
    Main.globalConnections.collect {
      case (other, `agentFrom`) ⇒ other
      case (`agentFrom`, other) ⇒ other
    }
  }

  /** finds all agents that are within `armsLength` */
  def findAgentsWithinArmsLength(agentFrom: Int, armsLength: Int,
                                 linkCount: Int = 0): Set[Int] = {
    val agentsConnections = findDirectConnections(agentFrom)
    if (linkCount < armsLength - 1) {
      agentsConnections ++ agentsConnections.flatMap { id ⇒
        findAgentsWithinArmsLength(id, armsLength,
          linkCount + 1)
      }
    } else {
      agentsConnections
    }
  }

  def instantiateConnectionMap(connectionsPerPersonMin: Int, connectionsPerPersonMax: Int):
  Set[(Int, Int)] = {
    val globalCons = collection.mutable.Set[(Int, Int)]()

    for (id ← Main.agents) {
      val agentCons = instantiateConnectionMapForAgent(id, connectionsPerPersonMin, connectionsPerPersonMax)
      globalCons ++= agentCons.map(c ⇒ id → c)
    }

    globalCons.toSet
  }

  def instantiateConnectionMapForAgent(ofAgent: Int, connectionsPerPersonMin: Int,
                                       connectionsPerPersonMax:Int): Set[Int] = {
    val (cMin, cMax) = (connectionsPerPersonMin, connectionsPerPersonMax)
    val connectionsCount: Int = Random.nextInt(cMax - cMin + 1) + cMin
    var connections = Set[Int]()

    while(connections.size < connectionsCount) {
      // simulating human type of networks
      val offsets = (0 until 2).map(_ ⇒ Math.round(Math.abs(Main.agentConnectionRange * Random.nextGaussian())).toInt).sorted

      var idRangeFloor = ofAgent - offsets.head - 1
      if (idRangeFloor < 0) idRangeFloor = 0
      var idRangeCeil = ofAgent + offsets.tail.head + 1
      if (idRangeCeil >= Main.agentCount) idRangeCeil = Main.agentCount - 1
      val idRange = idRangeCeil - idRangeFloor

      val newCon = Random.nextInt(idRange) + idRangeFloor
      // no self connections
      if (newCon != ofAgent) connections += newCon
    }

    connections
  }
}