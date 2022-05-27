import java.sql.Connection
import java.sql.SQLException

class CtfDAO (private val c: Connection) {
    companion object {
        private const val SCHEMA = "PUBLIC"
        //private const val SCHEMA = "default"
        private const val TABLE = "CTFS"
        private const val TRUNCATE_TABLE_CTFS_SQL = "TRUNCATE TABLE CTFS"
        private const val CREATE_TABLE_CTFS_SQL = "CREATE TABLE CTFS (" +
                "CTFid INT NOT NULL, " +
                "grupoid INT NOT NULL, " +
                "puntuacion INT NOT NULL, " +
                "PRIMARY KEY (CTFid,grupoid));"
        private const val ALTER_TABLE_CTFS_SQL = "ALTER TABLE CTFS ADD FOREIGN KEY (grupoid)" + "REFERENCES GRUPOS(grupoid);"
        private const val INSERT_CTFS_SQL = "INSERT INTO CTFS (CTFid, grupoid, puntuacion) VALUES " + " (?,?,?);"
        private const val SELECT_CTFS_BY_ID_SQL = "SELECT * FROM CTFS WHERE ctfid = ? AND grupoId = ?"
        private const val SELECT_ALL_CTFS_SQL = "SELECT * FROM CTFS"
        private const val DELETE_FROM_CTFS = "DELETE FROM CTFS WHERE CTFid = ? AND grupoid = ?"
        private const val UPDATE_CTFS_SQL = "UPDATE CTFS SET puntuacion = ? WHERE ctfid = ? AND grupoId = ?"
    }

    private fun doTableExists(): Boolean {
        var bool = false
        val metaData = c.metaData
        val rs = metaData.getTables(null, SCHEMA, TABLE, null)

        if (rs.next()) bool = true
        return bool
    }

    private fun truncateTable() {
        println(TRUNCATE_TABLE_CTFS_SQL)
        try {
            c.createStatement().use { st ->
                st.execute(TRUNCATE_TABLE_CTFS_SQL)
            }
            c.commit()
        } catch (e: SQLException) {
            printSQLException(e)
        }
    }

    private fun createTable() {
        println(CREATE_TABLE_CTFS_SQL)
        try {
            c.createStatement().use { st ->
                st.execute(CREATE_TABLE_CTFS_SQL)
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
                st.execute(ALTER_TABLE_CTFS_SQL)
            }
            c.commit()
        } catch (e: SQLException) {
            printSQLException(e)
        }
    }

    fun insertCTFS(ctf: CTF) {
        println(INSERT_CTFS_SQL)
        try {
            c.prepareStatement(INSERT_CTFS_SQL).use { st ->
                st.setInt(1, ctf.id)
                st.setInt(2, ctf.grupoId)
                st.setInt(3,ctf.puntuacion)
                println(st)
                st.executeUpdate()
            }
            c.commit()
        } catch (e: SQLException) {
            printSQLException(e)
        }
    }

    fun selectCTFByID(id: Int, grupoId: Int): CTF? {
        println(SELECT_CTFS_BY_ID_SQL)
        var ctf: CTF? = null

        try {
            c.prepareStatement(SELECT_CTFS_BY_ID_SQL).use { st ->
                st.setInt(1, id)
                st.setInt(2, grupoId)
                println(st)
                val rs = st.executeQuery()

                while (rs.next()) {
                    val id = rs.getInt("CTFid")
                    val grupoId = rs.getInt("grupoId")
                    val puntuacion = rs.getInt("puntuacion")
                    ctf = CTF(id, grupoId, puntuacion)
                }
            }

        } catch (e: SQLException) {
            printSQLException(e)
        }
        return ctf
    }

    fun selectAll(): MutableList<CTF> {
        println(SELECT_ALL_CTFS_SQL)
        val ctfs: MutableList<CTF> = ArrayList()

        try {
            c.prepareStatement(SELECT_ALL_CTFS_SQL).use { st ->
                println(st)
                val rs = st.executeQuery()

                while (rs.next()) {
                    val id = rs.getInt("ctfid")
                    val grupoId = rs.getInt("grupoId")
                    val puntuacion = rs.getInt("puntuacion")
                    ctfs.add(CTF(id, grupoId, puntuacion))
                }
            }

        } catch (e: SQLException) {
            printSQLException(e)
        }
        return ctfs
    }

    fun updateCTFS(ctf: CTF): Boolean {
        println(UPDATE_CTFS_SQL)
        var rowUpdated = false

        try {
            c.prepareStatement(UPDATE_CTFS_SQL).use { st ->
                st.setInt(1, ctf.puntuacion)
                st.setInt(2, ctf.id)
                st.setInt(3, ctf.grupoId)

                rowUpdated = st.executeUpdate() > 0
            }
            c.commit()
        } catch (e: SQLException) {
            printSQLException(e)
        }
        return rowUpdated
    }

    fun deleteCTFById(ctfID: Int, grupoId: Int): Boolean {
        println(DELETE_FROM_CTFS)
        var rowDeleted = false

        try {
            c.prepareStatement(DELETE_FROM_CTFS).use { st ->
                st.setInt(1, ctfID)
                st.setInt(2, grupoId)
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