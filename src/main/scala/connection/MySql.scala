package connection

import utils.Utilities
import java.sql.{DriverManager, Connection}

object MySql {

  def getConn() : Connection = {

    val conn = DriverManager.getConnection(Utilities.url, Utilities.getDbProps())

    conn
  }

}
