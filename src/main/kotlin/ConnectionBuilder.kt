import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException

class ConnectionBuilder {
    lateinit var connection: Connection
    //private val jdbcURL = "jdbc:h2:~/un9remake"
    //private val jdbcURL = "jdbc:h2:mem:default"
    private val jdbcURL = "jdbc:h2:~/un9pe"
    //private val jdbcURL = "jdbc:postgresql://localhost:5432/postgres"
    private val jdbcUsername = "user"
    //private val jdbcUsername = ""
    //private val jdbcUsername = "postgres"
    private val jdbcPassword = "user"
    //private val jdbcPassword = ""
    //private val jdbcPassword = "cvink1925"

    init {
        try {
            connection = DriverManager.getConnection(jdbcURL, jdbcUsername, jdbcPassword)
            connection.autoCommit = false
        } catch (e: SQLException) {
            e.printStackTrace()
        } catch (e: ClassNotFoundException) {
            e.printStackTrace()
        }
    }
}