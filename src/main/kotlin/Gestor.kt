class Gestor {
    lateinit var c: ConnectionBuilder
    lateinit var grupoDAO: GrupoDAO
    lateinit var ctfDAO: CtfDAO

    fun startConnection(): Boolean {
        var bool = false
        print("Comenzando conexión...\n")
        c = ConnectionBuilder()
        if (c.connection.isValid(10)) {
            bool = true
        }
        return bool
    }

    fun endConnection() {
        println("Terminando conexión...")
        c.connection.close()
    }

    fun run() {
        c.connection.use {
            grupoDAO = GrupoDAO(it)
            ctfDAO = CtfDAO(it)

            grupoDAO.prepareTable()
            ctfDAO.prepareTable()
            grupoDAO.alterTable()
            ctfDAO.alterTable()
            grupoDAO.insertGrupos(Grupo(1, "1DAM-G1"))
            grupoDAO.insertGrupos(Grupo(2, "1DAM-G2"))
            grupoDAO.insertGrupos(Grupo(3, "1DAM-G3"))
            grupoDAO.insertGrupos(Grupo(4, "1DAW-G1"))
            grupoDAO.insertGrupos(Grupo(5, "1DAW-G2"))
            grupoDAO.insertGrupos(Grupo(6, "1DAW-G3"))
            ctfDAO.insertCTFS(CTF(1, 1, 3))
            ctfDAO.insertCTFS(CTF(1, 2, 101))
            ctfDAO.insertCTFS(CTF(2,2,3))
            ctfDAO.insertCTFS(CTF(2,1,50))
            ctfDAO.insertCTFS(CTF(2,3,1))
            ctfDAO.insertCTFS(CTF(3,1,50))
            ctfDAO.insertCTFS(CTF(3,3,5))
        }
    }

    fun añadirParticipacion(idCtf: Int, idGrupo: Int, puntuacion: Int) {
        c.connection.use {
            grupoDAO = GrupoDAO(it)
            ctfDAO = CtfDAO(it)

            ctfDAO.insertCTFS(CTF(idCtf, idGrupo, puntuacion))

            val mejoresResultados = calculaMejoresResultados(ctfDAO.selectAll())

            mejoresResultados.forEach { t, u ->
                var grupoID = u.second.grupoId
                var ctfID = u.second.id
                var grupoActualizar = grupoDAO.selectGrupoByID(grupoID)
                if (grupoActualizar != null) {
                    grupoActualizar.mejorCtfId = ctfID
                }
                if (grupoActualizar != null) {
                    grupoDAO.updateGrupos(grupoActualizar)
                }
            }
        }
    }

    fun eliminarParticipacion(ctfid: Int, grupoid: Int) {
        c.connection.use {
            grupoDAO = GrupoDAO(it)
            ctfDAO = CtfDAO(it)
            var ctf = ctfDAO.selectCTFByID(ctfid,grupoid)
            var grupo = grupoDAO.selectGrupoByID(grupoid)

            if (ctf != null && grupo != null) {
                grupo.mejorCtfId = null // Si no fuese de tipo Int? (nullable), no dejaría ponerlo a null
                grupoDAO.updateGrupos(grupo)

                ctf.puntuacion = 0
                ctfDAO.updateCTFS(ctf)

                ctfDAO.deleteCTFById(ctfid,grupoid)

                val mejoresResultados = calculaMejoresResultados(ctfDAO.selectAll())

                mejoresResultados.forEach { t, u ->
                    var grupoID = u.second.grupoId
                    var ctfID = u.second.id
                    var grupoActualizar = grupoDAO.selectGrupoByID(grupoID)
                    if (grupoActualizar != null) {
                        grupoActualizar.mejorCtfId = ctfID
                    }
                    if (grupoActualizar != null) {
                        grupoDAO.updateGrupos(grupoActualizar)
                    }
                }
            }
        }
    }

    fun elegirGrupoPorID(grupoid: Int?): List<Grupo?> {
        var grupos: List<Grupo> = listOf()

        c.connection.use {
            grupoDAO = GrupoDAO(it)

            if (grupoid == null) {
                grupos = grupoDAO.selectAll()
                return grupos
            } else {
                val grupoElegido: List<Grupo?> = listOf(grupoDAO.selectGrupoByID(grupoid))
                return grupoElegido
            }
        }
    }

    /**
     * @param participaciones
     * @return devuelve un mutableMapOf<Int, Pair<Int, Ctf>> donde
     *
     *      Key: el grupoId del grupo
     *      Pair:
     *          first: Mejor posición
     *          second: Objeto CTF el que mejor ha quedado
     */

    private fun calculaMejoresResultados(participaciones: List<CTF>): MutableMap<Int, Pair<Int, CTF>> {
        val participacionesByCTFId = participaciones.groupBy { it.id }
        var participacionesByGrupoId = participaciones.groupBy { it.grupoId }
        val mejoresCtfByGroupId = mutableMapOf<Int, Pair<Int, CTF>>()
        participacionesByCTFId.values.forEach { ctfs ->
            val ctfsOrderByPuntuacion = ctfs.sortedBy { it.puntuacion }.reversed()
            participacionesByGrupoId.keys.forEach { grupoId ->
                val posicionNueva = ctfsOrderByPuntuacion.indexOfFirst { it.grupoId == grupoId }
                if (posicionNueva >= 0) {
                    val posicionMejor = mejoresCtfByGroupId.getOrDefault(grupoId, null)
                    if (posicionMejor != null) {
                        if (posicionNueva < posicionMejor.first)
                            mejoresCtfByGroupId.set(grupoId, Pair(posicionNueva, ctfsOrderByPuntuacion.get(posicionNueva)))
                    } else
                        mejoresCtfByGroupId.set(grupoId, Pair(posicionNueva, ctfsOrderByPuntuacion.get(posicionNueva)))
                }
            }
        }
        return mejoresCtfByGroupId
    }
}