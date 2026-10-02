# Permisos requeridos

Especificación de los accesos que la automatización necesita para operar sobre un clúster.
La definición de la identidad y su implementación corresponden a quien administra el ambiente.

## Qué necesita acceder

| Recurso | Permisos | Para qué |
|---|---|---|
| `kafkas` | consultar | establecer si el clúster está operativo |
| `kafkamirrormaker2s` | consultar y modificar | leer el estado de la replicación e invertirla |
| `virtualkafkaclusters` | consultar y modificar | leer y conmutar el destino del tráfico |
| `kafkaservices` | consultar | validar que el destino sea alcanzable antes de conmutar |
| `pods` | consultar | ubicar un broker |
| `pods/exec` | ejecutar | medir el estado de la replicación contra Kafka |

## Condiciones

**Acotado a los namespaces de la solución.** La automatización no requiere visibilidad sobre el
resto del clúster, por lo que alcanza con permisos de namespace y no de clúster.

**Sin permiso de eliminación en ningún recurso.** Ninguna operación implementada destruye datos.
Las que sí lo harán —el failback elimina tópicos antes de resincronizar— corresponden a una
identidad distinta, para que la credencial del failover no pueda borrar.

**Credencial de larga duración.** Los tokens de sesión caducan y dejan la automatización
inoperante; el acceso debe sobrevivir entre ejecuciones.

## Manifiestos de referencia

`core-co.yaml` y `core-ca.yaml` expresan lo anterior como `ServiceAccount`, `Role` y
`RoleBinding`. Son una referencia: quien administre el ambiente puede usarlos, adaptarlos a sus
convenciones de nombres y namespaces, o implementar lo mismo por otro medio.

Lo que no cambia es la tabla de arriba.

## Verificación

Con la identidad ya definida, estas comprobaciones confirman que el alcance es el correcto:

```
puede modificar kafkamirrormaker2s   → debe dar sí
puede eliminar  kafkatopics          → debe dar no
```
