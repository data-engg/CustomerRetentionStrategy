package connection

import com.datastax.driver.core.{Session, Cluster}

class Cassandra {

  private var session : Session = null
  private var cluster : Cluster = null

  def this(node : String, port : Int, keyspace : String = "edureka_735821") {
    this()
    this.cluster = Cluster.builder()
      .addContactPoint(node)
      .withPort(port)
      .withCredentials("edureka_735821", "edureka_735821iyktd")
      .build()

    this.session = cluster.connect(keyspace)
  }

  def getSession() : Session = {
    this.session
  }

  def close() : Unit = {
    this.session.close()
  }

}
