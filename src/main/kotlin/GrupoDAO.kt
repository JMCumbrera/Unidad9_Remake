import java.sql.Connection
import java.sql.SQLException

class GrupoDAO (private val c: Connection) {
    companion object {
        private const val SCHEMA = "PUBLIC"
        //private const val SCHEMA = "default"
        private const val TABLE = "GRUPOS"
        private const val TRUNCATE_TABLE_GRUPOS_SQL = "TRUNCATE TABLE GRUPOS"
        private const val CREATE_TABLE_GRUPOS_SQL = "CREATE TABLE GRUPOS (" +
                "grupoid INT NOT NULL AUTO_INCREMENT, " +
                "grupodesc VARCHAR(100) NOT NULL, " +
                "mejorposCTFid INT, PRIMARY KEY (grupoid));"
        //private const val CREATE_TABLE_GRUPOS_SQL = "CREATE TABLE GRUPOS (grupoid INT NOT NULL AUTO_INCREMENT, grupodesc VARCHAR(100) NOT NULL, mejorposCTFid INT, PRIMARY KEY (grupoid));"
        private const val ALTER_TABLE_GRUPOS_SQL = "ALTER TABLE GRUPOS ADD FOREIGN KEY (mejorposCTFid, grupoid)" + "REFERENCES CTFS(CTFid,grupoid)"
        private const val INSERT_GRUPOS_SQL = "INSERT INTO GRUPOS (grupoid, grupodesc) VALUES " + " (?,?);"
        private const val SELECT_GRUPOS_BY_ID_SQL = "SELECT grupoid, grupodesc, mejorposCTFid FROM GRUPOS WHERE grupoid = ?"
        private const val SELECT_ALL_GRUPOS_SQL = "SELECT * FROM GRUPOS"
        private const val UPDATE_GRUPOS_SQL = "UPDATE GRUPOS SET grupoDesc = ?, mejorposCtfId = ? WHERE grupoid = ?"
        private const val DELETE_GRUPOS_SQL = "DELETE FROM GRUPOS WHERE grupoid = ?"
    }

    private fun doTableExists(): Boolean {
        var bool = false
        val metaData = c.metaData
        val rs = metaData.getTables(null, SCHEMA, TABLE, null)

        if (rs.next()) bool = true
        return bool
    }

    private fun truncateTable() {
        println(TRUNCATE_TABLE_GRUPOS_SQL)
        try {
            c.createStatement().use { st ->
                st.execute(TRUNCATE_TABLE_GRUPOS_SQL)
            }
            c.commit()
        } catch (e: SQLException) {
            printSQLException(e)
        }
    }

    private fun createTable() {
        println(CREATE_TABLE_GRUPOS_SQL)
        try {
            c.createStatement().use { st ->
                st.execute(CREATE_TABLE_GRUPOS_SQL)
            }
            c.commit()
        } catch (e: SQLException) {
            printSQLException(e)
        }
    }

    fun prepareTable() {
        if (!doTableExists()) createTable() else truncateTable()
    }

    fun alterTable() {
        try {
            c.createStatement().use { st ->
                st.execute(ALTER_TABLE_GRUPOS_SQL)
            }
            c.commit()
        } catch (e: SQLException) {
            printSQLException(e)
        }
    }

    fun selectGrupoByID(grupoid: Int): Grupo? {
        println(SELECT_GRUPOS_BY_ID_SQL)
        var grupo: Grupo? = null

        try {
            c.prepareStatement(SELECT_GRUPOS_BY_ID_SQL).use { st ->
                st.setInt(1, grupoid)
                println(st)
                val rs = st.executeQuery()

                while (rs.next()) {
                    val grupoid = rs.getInt("grupoid")
                    val grupoDesc = rs.getString("grupoDesc")
                    val mejorposCTFid = rs.getInt("mejorposCTFid")
                    grupo = Grupo(grupoid, grupoDesc, mejorposCTFid)
                }
            }

        } catch (e: SQLException) {
            printSQLException(e)
        }
        return grupo
    }

    fun selectAll(): List<Grupo> {
        println(SELECT_ALL_GRUPOS_SQL)
        val grupos: MutableList<Grupo> = ArrayList()

        try {
            c.prepareStatement(SELECT_ALL_GRUPOS_SQL).use { st ->
                println(st)
                val rs = st.executeQuery()

                while (rs.next()) {
                    val grupoid = rs.getInt("grupoid")
                    val grupoDesc = rs.getString("grupoDesc")
                    val mejorposCTFid = rs.getInt("mejorposCTFid")
                    grupos.add(Grupo(grupoid, grupoDesc, mejorposCTFid))
                }
            }

        } catch (e: SQLException) {
            printSQLException(e)
        }
        return grupos
    }

    fun insertGrupos(grupo: Grupo) {
        println(INSERT_GRUPOS_SQL)
        try {
            c.prepareStatement(INSERT_GRUPOS_SQL).use { st ->
                st.setInt(1, grupo.grupoid)
                st.setString(2, grupo.grupoDesc)
                println(st)
                st.executeUpdate()
            }
            c.commit()
        } catch (e: SQLException) {
            printSQLException(e)
        }
    }

    fun updateGrupos(grupo: Grupo): Boolean {
        println(UPDATE_GRUPOS_SQL)
        var rowUpdated = false

        try {
            c.prepareStatement(UPDATE_GRUPOS_SQL).use { st ->
                st.setString(1, grupo.grupoDesc)

                when {
                    grupo.mejorCtfId != null -> { st.setInt(2, grupo.mejorCtfId!!) }
                    else -> { st.setNull(2, java.sql.Types.INTEGER) }
                }

                st.setInt(3, grupo.grupoid)
                rowUpdated = st.executeUpdate() > 0
            }
            c.commit()
        } catch (e: SQLException) {
            printSQLException(e)
        }
        return rowUpdated
    }

    fun deleteGrupoID(grupoid: Int): Boolean {
        println(DELETE_GRUPOS_SQL)
        var rowDeleted = false

        try {
            c.prepareStatement(DELETE_GRUPOS_SQL).use { st ->
                st.setInt(1, grupoid)
                rowDeleted = st.executeUpdate() > 0
            }
            c.commit()
        } catch (e: SQLException) {
            printSQLException(e)
        }
        return rowDeleted
    }

    private fun printSQLException(ex: SQLException) {
        for (e in ex) {
            if (e is SQLException) {
                e.printStackTrace(System.err)
                System.err.println("SQLState: " + e.sqlState)
                System.err.println("Error Code: " + e.errorCode)
                System.err.println("Message: " + e.message)
                var t = ex.cause
                while (t != null) {
                    println("Cause: $t")
                    t = t.cause
                }
            }
        }
    }
}