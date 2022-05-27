import kotlinx.cli.*

@OptIn(ExperimentalCli::class)
fun main(args: Array<String>) {
    val parser = ArgParser("un9pe", strictSubcommandOptionsOrder = true)
    val run = Gestor()
    //run.startConnection()
    //run.run()

    //run.endConnection()

    class Añadir: Subcommand("-a", "Añade participación de un grupo en un CTF con cierta puntuación") {
        val ctfid by argument(ArgType.Int, description = "ID del CTF")
        val grupoid by argument(ArgType.Int, description = "ID del grupo")
        val puntuacion by argument(ArgType.Int, description = "Puntuación")

        override fun execute() {
            if (run.startConnection()) {
                run.añadirParticipacion(ctfid, grupoid, puntuacion)
            }
        }
    }

    class Eliminar: Subcommand("-d", "Elimina participación de un grupo en un ctf") {
        val ctfid by argument(ArgType.Int, description = "ID del CTF")
        val grupoid by argument(ArgType.Int, description = "ID del grupo")

        override fun execute() {
            if (run.startConnection()) {
                run.eliminarParticipacion(ctfid,grupoid)
            }
        }
    }

    class Mostrar: Subcommand("-l", "Muestra la información de un grupo, si no existe mostrará todos los grupos") {
        val grupoid by argument(ArgType.Int, description = "ID del grupo").optional()

        override fun execute() {
            println("Entrando a ejecutar: ")

            if (run.startConnection()) {
                if (grupoid == null) {
                    val todosLosGrupos = run.elegirGrupoPorID(grupoid)
                    todosLosGrupos.forEach {
                        if (it != null) {
                            println("ID del grupo: ${it.grupoid}")
                            println("Nombre del grupo: ${it.grupoDesc}")
                            println("ID CTF con mejor puntuación: ${it.mejorCtfId}")
                            println()
                        }
                    }
                } else {
                    val grupoElegido = grupoid?.let { run.elegirGrupoPorID(it) } // Versión nueva (generada automáticamente)

                    if (grupoElegido != null) {
                        grupoElegido.forEach {
                            if (it != null) {
                                println("ID del grupo: ${it.grupoid}")
                                println("Nombre del grupo: ${it.grupoDesc}")
                                println("ID CTF con mejor puntuación: ${it.mejorCtfId}")
                            }
                        }
                    }
                }
            }

        }
    }

    val añadir = Añadir()
    val eliminar = Eliminar()
    val mostrar = Mostrar()

    parser.subcommands(añadir, eliminar, mostrar)
    parser.parse(args)
}