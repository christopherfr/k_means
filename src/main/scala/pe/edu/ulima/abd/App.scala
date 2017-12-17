package pe.edu.ulima.abd
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import Array._

object App {

	// Constantes de configuración de Spark
	val CONF = new SparkConf().setAppName("parcial").setMaster("local")
	val SC = new SparkContext(CONF)
	val FILE_PATH = "data/HR_comma_sep.csv"

	// Constantes del algoritmo
	val K = readLine("Ingrese el número de centroides (ENTERO): ").toInt
	val PREGUNTA_1 = if (readLine("¿Pregunta 1? (S/N): ") == "S") {true} else {false}
	val INDICE_COL = if (PREGUNTA_1) {readLine("Ingrese la columna (1-based index) (ENTERO): ").toInt-1} else {-1} // SIN MANEJO DE ERRORES PORQUE TENGO SUEÑO :(

	// Constantes dataset
	val DATASET_STR_RDD = SC.textFile(FILE_PATH)
	val DATASET_ARRSTR_RDD = DATASET_STR_RDD.map(x => x.split(",")) 
											.map(x => x.map(x => x.replace("sales","0")))
											.map(x => x.map(x => x.replace("hr","1")))
											.map(x => x.map(x => x.replace("IT","2")))
											.map(x => x.map(x => x.replace("management","3")))
											.map(x => x.map(x => x.replace("marketing","4")))
											.map(x => x.map(x => x.replace("product_mng","5")))
											.map(x => x.map(x => x.replace("RandD","6")))
											.map(x => x.map(x => x.replace("accounting","7")))
											.map(x => x.map(x => x.replace("support","8")))
											.map(x => x.map(x => x.replace("technical","9")))
											.map(x => x.map(x => x.replace("high","0")))
											.map(x => x.map(x => x.replace("low","1")))
											.map(x => x.map(x => x.replace("medium","2"))) // Reemplazo de valores alfanuméricos por numéricos
	val DATASET_ARRDBL_RDD = DATASET_ARRSTR_RDD.map(x => x.map(_.toDouble))
	val NROW = DATASET_ARRDBL_RDD.count().toInt // 14999
	val NCOL = DATASET_ARRDBL_RDD.first().length // 10
	val DATASET_ARR = DATASET_ARRDBL_RDD.collect()

	val RND = new scala.util.Random

	def main(args : Array[String]) {
		var nuevosCentroides = true
		var centroidesAnteriores = Array.ofDim[Double](K,NCOL)
		var distancias = Array.ofDim[Double](NROW,K)
		var datasetConCentroides = Array.ofDim[Double](NROW,NCOL+1)

		var centroides = inicializarCentroides()	
		while (nuevosCentroides) {
			centroidesAnteriores = centroides
			distancias = distanciasDeCentroidesXFila(centroides)
			datasetConCentroides = asignarCentroideAFilas(distancias)
			centroides = actualizarCentroides(datasetConCentroides)
			nuevosCentroides = existenCambios(centroidesAnteriores,centroides)
		}
	
		// Mostrar RDD
		var RESPUESTA_RDD = SC.parallelize(asignarCentroideAFilas(distanciasDeCentroidesXFila(centroides)))
		RESPUESTA_RDD.collect().map(x => println(x(1)+","+x(2)+","+x(3)+","+x(4)+","+x(5)+","+x(6)+","+x(7)+","+x(8)+","+x(9)+" en centroide "+(x(0).toInt+1)))

		println("***********************************")
		println("************** F I N **************")
		println("***********************************")
	}

	def inicializarCentroides() : Array[Array[Double]] = {
		var centroides = Array.ofDim[Double](K,NCOL)
		var repetido = false
		var i = 0

		while (i < K) {
			repetido = false
			centroides(i) = DATASET_ARR(nuevoRandom())
			if (i > 0) {
				// Itera por los centroides ya escogidos para validar que el nuevo no se repita
				for (a <- 0 to (i-1)) {
					if (centroides(i) == centroides(a)) { repetido = true }
				}
			}
			// Si encontró un repetido, no incrementa el i, razón por la que vuelve
			// a asignarle otro valor al mismo centroide en la siguiente iteración
			if (repetido == false) { 
				i = i + 1 
			}
		}
		
		return centroides
	}

	def distanciasDeCentroidesXFila(centroides : Array[Array[Double]]) : Array[Array[Double]] = {
		var distancias = Array.ofDim[Double](NROW,K)
		
		for (i <- 0 to NROW-1) {
			for (j <- 0 to K-1) {
				if (PREGUNTA_1) {
					distancias(i)(j) = distanciaElementos(DATASET_ARR(i)(INDICE_COL), centroides(j)(INDICE_COL))
				} else {
					distancias(i)(j) = distanciaArrays(DATASET_ARR(i), centroides(j))
				}
			}
		}

		return distancias
	}

	def distanciaElementos(a : Double, b : Double) : Double = {
		return math.abs(a-b)
	}

	def distanciaArrays(a : Array[Double], b : Array[Double]) : Double = {
		var acum = 0.0
		for (j <- 0 to NCOL-1) {
			acum = acum + math.pow(a(j)-b(j),2)
		}
		return math.sqrt(acum)
	}

	def asignarCentroideAFilas(distancias : Array[Array[Double]]) : Array[Array[Double]] = {
		var min = 0.0
		var centroide = 0
		var centroideDeFila = new Array[Int](NROW) // Array a "fusionar" con el dataset original
		var datasetConCentroides = Array.ofDim[Double](NROW,NCOL+1)

		// Asignar a las filas del dataset el centroide más cercano
		for (i <- 0 to NROW-1) {
			min = distancias(i).reduceLeft(_ min _) // Halla la mínima distancia encontrada por fila
			centroide = distancias(i).indexOf(min) // Busca qué centroide tiene el mínimo
			centroideDeFila(i) = centroide
		}
		
		// "Fusión" de centroides + DATASET_ARR
		for (i <- 0 to NROW-1) {
			datasetConCentroides(i)(0) = centroideDeFila(i)
			for (j <- 0 to NCOL-1) {
				datasetConCentroides(i)(j+1) = DATASET_ARR(i)(j)
			}
		}

		return datasetConCentroides
	}

	def actualizarCentroides(datasetConCentroides : Array[Array[Double]]) : Array[Array[Double]] = {
		var centroides = Array.ofDim[Double](K,NCOL)	
		var cambios = true		

		// Inicialización rochoza con cualquier cosa para no lidiar con los tipos de dato
		var promedio_RDD = SC.parallelize(datasetConCentroides.map(x => (x(0),(x(0), 1))))
		var promedioReduced = promedio_RDD.reduceByKey((x,y) => (x._1+y._1, x._2 + y._2))
		var rddPromFin = promedioReduced.map( x => (x._1 , x._2._1/x._2._2 ))
		var tuplas : Array[(Double,Double)] = null

		// Sacar promedios y asignarlo a centroides
		if (PREGUNTA_1) {
			promedio_RDD = SC.parallelize(datasetConCentroides.map(x => (x(0), (x(INDICE_COL+1), 1) )))
			promedioReduced = promedio_RDD.reduceByKey( (x,y) => (x._1+y._1, x._2 + y._2 ))
			rddPromFin = promedioReduced.map( x => (x._1 , x._2._1/x._2._2 ))

			// Actualiza los centroides
			tuplas = rddPromFin.collect()
			for (k <- 0 to K-1) {
				centroides(tuplas(k)._1.toInt)(INDICE_COL) = tuplas(k)._2 
			}
		} else {
			for (j <- 1 to NCOL) {
				promedio_RDD = SC.parallelize(datasetConCentroides.map(x => (x(0), (x(j), 1) )))
				promedioReduced = promedio_RDD.reduceByKey( (x,y) => (x._1+y._1, x._2 + y._2 ))
				rddPromFin = promedioReduced.map( x => (x._1 , x._2._1/x._2._2 ))
			
				// Actualiza los centroides
				tuplas = rddPromFin.collect()
				for (k <- 0 to K-1) {
					centroides(tuplas(k)._1.toInt)(j-1) = tuplas(k)._2 
				}			
			}
		}

		return centroides
	}

	def existenCambios(c1 : Array[Array[Double]],c2 : Array[Array[Double]]) : Boolean = {
		var nuevosCentroides = true
		// Verifica si los centroides son distintos a los anteriores
		for (k <- 0 to K-1) {
			if (c1(k).sameElements(c2(k))) {
				nuevosCentroides = false
			}
		}
		return nuevosCentroides
	}

	def nuevoRandom() : Int = {
		return RND.nextInt(NROW)
	}

}
