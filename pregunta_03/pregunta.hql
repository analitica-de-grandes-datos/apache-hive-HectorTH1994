/* 
Pregunta
===========================================================================

Para resolver esta pregunta use el archivo `data.tsv`.

Escriba una consulta que devuelva los cinco valores diferentes más pequeños 
de la tercera columna.

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

        >>> Escriba su respuesta a partir de este punto <<<
*/
DROP TABLE IF EXISTS datos;
CREATE TABLE datos (letra STRING,
                    fecha STRING,
                    numero INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
SHOW CREATE TABLE datos;
LOAD DATA LOCAL INPATH "data.tsv" OVERWRITE INTO TABLE datos;
DESCRIBE datos;

DROP TABLE IF EXISTS word_counts;
CREATE TABLE word_counts
AS
select DISTINCT numero
from datos
order by numero limit 5;

INSERT OVERWRITE LOCAL DIRECTORY './output'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM word_counts;

