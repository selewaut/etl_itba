# Job Preliminar

El job preliminar `create_substance_schema.kjb` se encarga de generar las tablas necesarias para el ETL. Estas son: `Substance`, que es la dimensión principal que el ETL busca generar; `etl_run`, que guarda el estado de una corrida del ETL para tener registro de las corridas, cuánto tardaron y si fallaron; y por último `etl_rejects`, donde guarda errores y logs si alguna corrida no se completa.

Además, se genera un PSM `rec_user_type` que sirve para poder insertar correctamente los registros nuevos generados por el ETL.
La función principal se llama `inserta_substance`. Principalmente se utiliza para poder tener el ID autogenerado disponible cada vez que se genera un nuevo registro en la dimensión Substance.
Si hay un error al insertar, devuelve el mensaje de error y además loguea los valores que se quisieron insertar.

# Flujo Principal

En el flujo principal es donde se realiza el verdadero ETL para la dimensión Substance a partir de la tabla histórica de mediciones (`hourly_42101_2012_1`).

A continuación se describen brevemente los bloques del flujo principal.
1. `source substances`
Aquí se ingesta la data cruda de la tabla histórica mediante un distinct, sin filtrar valores nulos ni erróneos, ya que estos se registrarán en el mismo ETL.

2. `check source`
Valida que `name` y `unit` no sean nulos ni vacíos. Si alguno de los dos lo es, devuelve el motivo (`'null or empty name'` o `'null or empty unit'`); si ambos son válidos, devuelve `null`, lo que indica que la fila es utilizable.

3. `source ok?`
Toma el output de `check source` (`source_reason`) y lo evalúa: si es **nulo** (fila válida), manda por la rama correcta hacia `drop header`; si **no es nulo** (tiene motivo de rechazo), manda al flujo de registros erróneos.

4. `drop header`
En este bloque se elimina el header duplicado filtrando el nombre específico (`Parameter Name`).

5. `normalize unit`
Aquí mediante expresiones JAVA se normaliza la unidad de la sustancia contaminante. En particular, la expresión primero remueve caracteres separadores, ignora mayúsculas y luego se fija que esté en un mapping que sea o "parts per million" o "ppm". Finalmente devuelve la estandarizada "PPM". Si es una unidad nueva, simplemente la devuelve con las operaciones básicas y se requiere actualizar el mapping, capaz mejor aún con una tabla auxiliar.

```java
if (unit == null) {
    return unit; // si es nulo devuelvo nulo
} else {
    // elimino espacios y otros separadores
    String cleanUnit = unit.replaceAll("\\s+", " ").trim();
    // aplica regla especifica de PPM que es la unica unidad por ahora.
    if ("parts per million".equalsIgnoreCase(cleanUnit) || "ppm".equalsIgnoreCase(cleanUnit)) {
        return "PPM";
    } else {
        return cleanUnit;
    }
}
```

6. `normalize name`
De manera similar a las unidades, con los nombres se quitan espacios redundantes y se ignoran mayúsculas con una expresión JAVA y luego se evalúan posibles casos de typos.

```java

if (name == null) {
    return name; // devuelve nulo de manera segura
} else {
    String cleanName = name.replaceAll("\\s+", " ").trim();
    
    // evaluamos posibles casos de typos.
    if ("carbon monoxide".equalsIgnoreCase(cleanName)
            || "co".equalsIgnoreCase(cleanName)
            || "carbno monoxide".equalsIgnoreCase(cleanName) // Typo 1
            || "cabron monoxide".equalsIgnoreCase(cleanName)) { // Typo 2
        return "Carbon monoxide";
    } else {
        return cleanName;
    }
}
```
En caso de empezar a agregar más sustancias, se podría agregar un maestro de sustancias y luego matchear mediante distancia de levenshtein a la sustancia más cercana.

7. `strip source reason`
Paso extra (Select Values) que elimina el campo `source_reason` del flujo justo antes de llamar al PSM. La PSM vincula los `?` posicionalmente con **todos** los campos del renglón: con `source_reason` presente llegaban 3 campos para 2 `?` (error `column index is out of range: 3, number of columns: 2`). Tras este paso el renglón queda solo con `name` y `unit`.

8. `inserta substance`

Utilizando el PSM, se inserta en la tabla Substance de la dimensión. Nuevamente, si hay algún tipo de error como sustancia repetida, nulo, esquema incompatible o violación de alguna restricción de la tabla, se manda al flujo de errores.
```sql
SELECT id, message FROM inserta_substance(?,?);
```


# Flujo de Errores

1. `open run`
Se inicializa una corrida en la tabla `etl_run` para registrar su estado.
```sql
INSERT INTO etl_run (job_name) VALUES ('Substance02') RETURNING etl_run_id;
```

2. <a id="join-source-reject"></a>`join source reject`
Joinea o mergea la fila proveniente de `source ok?` con la data de la corrida del ETL actual.

3. `shape source reject`
Arma las 3 columnas que necesita la tabla `etl_rejects` para loggear errores:
- stage = bloque que rechazó
- reason = copia de `source_reason`
- raw_payload = json con los valores rechazados (`name`/`unit`).

4. `write source reject`
Inserta en la tabla `etl_rejects` los errores provenientes de `source ok?`
```sql
INSERT INTO etl_rejects (etl_run_id, stage, raw_payload, reason) VALUES (?,?,?::jsonb,?)
```

5. `join header reject`
Joinea errores en el bloque de header con la metadata de la corrida del ETL, igual que en [2.](#join-source-reject)

6. `shape header reject`
Prepara las columnas para guardar los errores en el bloque de header.

7. `write header reject`
Escribe los errores del bloque de filtrado del header.

8. `join psm reject`
De los rejects más importantes, joinea los errores del PSM con la metadata del ETL.


9. `shape psm reject`
Le da el formato correcto para guardar en `etl_rejects`

10. `write psm reject`

Finalmente guarda si hubiere algún error en el PSM.
