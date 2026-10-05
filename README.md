# Automatización de Failover — Kafka Activo/Pasivo

Automatiza en Ansible Automation Platform el procedimiento de **failover** entre los dos sitios
de un clúster Kafka activo/pasivo sobre OpenShift: invierte la replicación de MirrorMaker 2 y
conmuta el tráfico hacia el sitio que asume la operación.

La decisión de ejecutar un failover sigue siendo del operador. Lo que se automatiza es todo lo
demás: establecer el estado real, verificar que la operación sea posible, aplicarla en el orden
correcto y confirmar el resultado.

---

## Cómo opera

```mermaid
%%{init: {'theme':'base','themeVariables':{'primaryColor':'#eef2f7','primaryTextColor':'#111827','primaryBorderColor':'#5a6474','lineColor':'#5a6474','secondaryColor':'#e3eaf5','tertiaryColor':'#ffffff','background':'#ffffff','mainBkg':'#eef2f7','textColor':'#111827','fontSize':'14px'}}}%%
flowchart TD
    OP([El operador solicita la operación<br/>y declara la dirección]) --> DESC[Descubrir el estado real<br/>de ambos sitios]
    DESC --> COH{¿El estado es<br/>reconocible?}
    COH -->|no| S1([Se detiene])
    COH -->|sí| VAL{¿La transición<br/>está permitida?}
    VAL -->|no| S2([Se detiene])
    VAL -->|sí| PRE{Validaciones<br/>previas}
    PRE -->|alguna falla| S3([Se detiene])
    PRE -->|todas pasan| PROP[Propuesta:<br/>estado actual, cambios y resultado esperado]
    PROP --> APR{{Aprobación del operador}}
    APR --> REV{¿El estado sigue<br/>siendo el aprobado?}
    REV -->|cambió| S4([Se detiene])
    REV -->|coincide| EX1[Apagar la replicación en curso]
    EX1 --> EX2[Activar la replicación inversa]
    EX2 --> EX3[Conmutar el destino del tráfico]
    EX3 --> VER[Verificar el estado final]
    VER --> FIN([Operación completa])

    classDef paso     fill:#eef2f7,stroke:#5a6474,color:#111827,stroke-width:1px
    classDef decision fill:#e3eaf5,stroke:#2b46ae,color:#111827,stroke-width:1px
    classDef detiene  fill:#f7d9cf,stroke:#a13d12,color:#111827,stroke-width:1.5px
    classDef aprueba  fill:#fae8c8,stroke:#8a5a00,color:#111827,stroke-width:1.5px
    classDef fin      fill:#cdeadb,stroke:#136c46,color:#111827,stroke-width:1.5px

    class OP,DESC,PROP,EX1,EX2,EX3,VER paso
    class COH,VAL,PRE,REV decision
    class S1,S2,S3,S4 detiene
    class APR aprueba
    class FIN fin
```

### Las etapas

| | Etapa | Qué ocurre |
|---|---|---|
| 1 | **Descubrir** | Se consulta el estado real de los dos sitios: cuál está activo, en qué estado está cada MirrorMaker, hacia dónde apunta el tráfico y cuánto atraso acumula la replicación. Nada se da por supuesto. |
| 2 | **Reconocer** | El estado encontrado debe corresponder a una situación conocida. Señales contradictorias —dos replicaciones activas a la vez, un sitio que no responde, un conector caído— interrumpen la operación. |
| 3 | **Validar la transición** | La operación solicitada debe estar permitida desde el estado actual. El conjunto de transiciones válidas es explícito y acotado. |
| 4 | **Validaciones previas** | Se comprueban las condiciones necesarias para que la operación pueda completarse: que la dirección declarada por el operador sea la que el descubrimiento encontró, la salud del sitio destino, el estado de la replicación y la alcanzabilidad del destino del tráfico. |
| 5 | **Proponer** | Se presenta al operador qué se encontró, qué cambios se aplicarán y cómo quedará el ambiente. |
| 6 | **Aprobar** | El flujo se detiene. Hasta aquí no se modificó nada. |
| 7 | **Revalidar** | Antes de la primera escritura se vuelve a establecer el estado y se comprueba que siga coincidiendo con lo aprobado. |
| 8 | **Ejecutar** | Se apaga la replicación en curso, se activa la inversa y se conmuta el destino del tráfico. En ese orden. |
| 9 | **Verificar** | Se confirma que el estado alcanzado es el que la transición declaraba. |

Las etapas 1 a 6 no modifican el ambiente. La primera escritura ocurre en la 8.

---

## Principios

Dos propiedades sostienen el diseño:

**No se asume quién es el sitio activo.** Se descubre en cada ejecución, a partir de qué
MirrorMaker está replicando. Tras una rotación de roles los sitios quedan invertidos de forma
permanente, y la automatización lo refleja sin cambios de configuración.

**Ante un estado que no reconoce, se detiene.** No interpreta, no corrige, no improvisa. El
conjunto de transiciones válidas es explícito; cualquier combinación fuera de él interrumpe la
operación.

---

## Alcance

La unidad sobre la que opera es una **pareja**: dos clústeres Kafka en sitios opuestos, con sus
dos MirrorMaker y su proxy.

| | |
|---|---|
| **Implementado** | Failover de una pareja |
| **Previsto** | Failback, rotación programada de roles, y parejas adicionales |

Incorporar una pareja adicional es agregar un bloque de datos: la automatización no contiene
referencias a ninguna pareja en particular.

---

## Estructura

```
transitions.yml          contrato de transiciones permitidas
inventory/
  hosts.ini
playbooks/
  discover.yml           consulta el estado, sin modificar nada
  preflight.yml          valida la operación y arma la propuesta
  failover.yml           ejecuta y verifica
  group_vars/all/
    parejas.yml          definición de cada pareja
    umbrales.yml         tolerancias y tiempos de espera
roles/
  kafka_discover/        establece el estado real de ambos sitios
  kafka_decide/          valida contra el contrato; propone o verifica
  mm2_state/             modifica el estado de un MirrorMaker
  proxy_target/          conmuta el destino del tráfico
  app_scale/             ajusta las aplicaciones al rol de su sitio
```

De los cinco roles, **tres modifican el ambiente**. Los otros dos consultan y calculan.

---

## Los roles

Cada rol es una capacidad acotada que recibe una pareja y actúa sobre ella. Ninguno contiene
referencias a una pareja concreta: eso vive en los datos.

### `kafka_discover` — establece el estado real

Consulta los dos sitios y construye una representación única del ambiente: qué sitio está
activo, en qué estado se encuentra cada MirrorMaker, hacia dónde apunta el tráfico, si los
destinos configurados son alcanzables, y cuánto atraso acumula la replicación medido por
partición.

El sitio activo **se deduce**, no se configura: es aquel desde el cual se está replicando.

Es la única pieza que consulta ambos sitios a la vez, y la que alimenta a todas las demás. Si un
sitio no responde, lo reporta como tal en lugar de fallar — distinguir *"no existe"* de *"no
pude consultarlo"* es necesario para decidir correctamente.

**No modifica nada.**

### `kafka_decide` — contrasta contra el contrato

Busca en `transitions.yml` la transición que corresponde a la operación solicitada desde el
estado encontrado. Si no existe, interrumpe.

Opera en dos momentos:

| Momento | Qué hace |
|---|---|
| Antes de ejecutar | Corre las validaciones previas y arma el plan: qué recursos se tocarán, con qué valores y cuál es el resultado esperado |
| Después de ejecutar | Compara el estado alcanzado contra el que la transición declaraba, y contrasta los datos: que no falten particiones, que no se hayan perdido mensajes y que los consumidores conserven su posición |

**No modifica nada.** Decide si se puede avanzar y qué debe ocurrir; la ejecución es de otros.

### `mm2_state` — cambia el estado de la replicación

Lleva cada MirrorMaker al estado que el plan indica. El cambio es declarativo: se ajusta la
configuración existente, no se eliminan ni recrean recursos.

Respeta el orden del plan, que no es arbitrario: **primero se detiene la replicación en curso y
después se activa la inversa.** Tenerlas activas simultáneamente haría que los mensajes
circulen entre sitios indefinidamente.

Tras cada cambio espera a que el recurso quede efectivamente aplicado antes de continuar.

### `proxy_target` — conmuta el destino del tráfico

Cambia el destino al que el proxy dirige a los productores y consumidores.

Antes de tocar nada verifica que el destino sea alcanzable. Es la validación que evita el modo
de falla más costoso: cortar el tráfico del origen y descubrir después que el destino no estaba
disponible.

### `app_scale` — ajusta las aplicaciones al rol de su sitio

Lleva cada aplicación declarada al número de instancias que corresponde al rol que su sitio
pasa a tener. Respeta el orden del plan: **primero se detienen las del sitio que deja de ser
activo**, después se levantan las del que asume. Evita que ambas procesen a la vez.

Qué aplicaciones siguen el rol del sitio es una decisión del ambiente, declarada en los datos.
Los sistemas que conmutan por su cuenta quedan fuera.

---

## Configuración

Dos archivos concentran todo lo que cambia entre ambientes. Los roles no contienen nombres de
recursos ni rutas.

### `parejas.yml` — qué existe y dónde

Por cada pareja: los dos sitios con su API, namespace y nombre de clúster; los dos MirrorMaker
con el sitio donde reside cada uno; el proxy y sus destinos posibles; los tópicos de aplicación
y los grupos de consumo que se vigilan; y las aplicaciones que siguen el rol de su sitio.

El MirrorMaker reside siempre en el sitio **destino** de la replicación, no en el origen.

No se declaran nombres de pods. El broker al que se consultan los offsets se elige por etiqueta
entre los que están corriendo: un nombre fijo se desactualiza en silencio cuando el clúster se
recrea, y la medición quedaría vacía sin que nada lo señale.

### `transitions.yml` — qué está permitido

Por cada transición: el estado de partida, las validaciones exigidas, las acciones a aplicar y
el estado esperado al terminar. Una transición no declarada aquí no puede ejecutarse.

Es el documento que se revisa con arquitectura y con auditoría: describe el comportamiento
completo de la automatización sin leer código.

### `umbrales.yml` — tolerancias

Atraso de replicación admitido, tiempos de espera y condiciones de salud exigidas. Ajustables
por ambiente sin tocar la lógica.

---

## Validaciones previas

La transición declara cuáles exige. Si alguna falla, no se modifica nada.

| Validación | Qué exige |
|---|---|
| Sitio destino alcanzable | su API responde |
| Kafka destino operativo | el clúster reporta estado correcto |
| Sin particiones sub-replicadas | el destino está íntegro |
| Replicación al día | el atraso no supera el umbral configurado |
| Posición de los consumidores replicada | el sitio que asume no reprocesa desde el principio |
| Destino de tráfico resoluble | la configuración del proxy apunta a un destino válido |

Dos merecen explicación. La de **posición de los consumidores** existe porque los mensajes y la
posición de lectura son dos flujos distintos: pueden estar los datos y no estar el marcador, y
entonces el sitio que asume reprocesa. La de **destino de tráfico** evita el modo de falla más
costoso: cortar el tráfico y descubrir después que el destino no era alcanzable.

Las validaciones que comparan ambos sitios exigen además haber podido medirlos. Una validación
sin datos no se da por superada.

---

## Operación en AAP

El flujo se ejecuta como un workflow de tres pasos: **validación → aprobación → ejecución**.

El operador declara su intención — qué pareja, qué operación y **qué sitio quiere dejar activo**.
La dirección no describe una maniobra, describe el estado deseado. De ahí salen tres respuestas
posibles:

| Lo que el operador pide | Lo que encuentra la automatización | Respuesta |
|---|---|---|
| Dejar activo al otro sitio | el estado de partida de una transición válida | propone la maniobra y pide aprobación |
| Dejar activo al sitio que ya lo es | un estado coherente | **nada que hacer**: lo informa y no modifica nada |
| Dejar activo al sitio que ya lo es | un estado incoherente | se detiene: el sitio correcto puede estar activo con la replicación o el tráfico a medio camino, y eso necesita una persona |

Que pedir un destino al que ya se llegó no sea un error hace la operación repetible: relanzarla
tras una ejecución completa informa que no hay nada que hacer, en lugar de proponer la maniobra
inversa. Sin la dirección declarada, la misma plantilla ejecutaba hacia un lado o hacia el otro
según un estado que el operador no veía hasta tenerlo ya propuesto, y una aprobación dada por
costumbre bastaba para conmutar al revés.

Antes de aprobar, el operador recibe el estado encontrado, los cambios que se aplicarán y el
resultado esperado.

El plan aprobado acompaña a la ejecución. Antes de modificar nada, la automatización vuelve a
establecer el estado y **comprueba que siga coincidiendo con lo aprobado**: si algo cambió entre
la aprobación y la ejecución, se detiene.

### Objetos requeridos

| Objeto | Función |
|---|---|
| Proyecto | Apunta a este repositorio. Conviene sincronizarlo al lanzar, para que cada ejecución use la versión vigente |
| Inventario | Un único `localhost`. La automatización corre desde el entorno de ejecución contra las APIs de los clústeres; no hay hosts remotos |
| Tipo de credencial | Entrega el acceso a cada sitio. **Su creación requiere privilegios de superusuario** en la plataforma; no alcanza con administrar la organización |
| Credencial | Una por conjunto de sitios, construida sobre el tipo anterior |
| Plantillas de trabajo | Tres: consulta de estado, validación y ejecución |
| Flujo de trabajo | Uno por operación: validación → aprobación → ejecución |

Las tres plantillas son genéricas: no contienen la operación ni la pareja. Esos valores llegan
desde el flujo.

### Configuración que la solución requiere

Más allá de crear los objetos, estas condiciones sostienen las garantías de la sección final.
Sin ellas la automatización sigue funcionando, pero deja de ser confiable.

**El flujo de trabajo es la única puerta de entrada.** El formulario que responde el operador
vive ahí, no en las plantillas. Replicarlo en ambos lugares abre la posibilidad de que queden
desalineados y que el comportamiento dependa de por dónde se entre.

**Cada valor tiene un solo origen.** Lo que el operador elige llega por el formulario; lo que
define el contexto de ejecución vive en la plantilla correspondiente; lo que configura el
comportamiento de la solución vive en este repositorio. Un mismo valor definido en dos lugares
vuelve indeterminado cuál gana.

**La inyección libre de variables debe estar deshabilitada** en el flujo y en todo lo que
ejecute. De lo contrario, quien lanza puede sobrescribir los umbrales que gobiernan las
validaciones previas y atravesarlas sin que quede constancia de que se saltearon.

La consecuencia es deliberada: **los parámetros que gobiernan el comportamiento solo se cambian
en el repositorio**, con revisión y trazabilidad. La pregunta *"¿con qué parámetros se ejecutó
esta operación?"* tiene una única respuesta posible, y está versionada.

**La ejecución declara que opera bajo aprobación.** La plantilla que modifica el ambiente lleva
esa condición fijada: si se la invoca sin el plan aprobado, se detiene en lugar de continuar
sin esa validación.

---

## Nota sobre la versión de la API de MirrorMaker

Los recursos `KafkaMirrorMaker2` están definidos con el formato anterior del esquema de Strimzi:
declaran `spec.clusters`, `connectCluster` y `sourceCluster`/`targetCluster`.

La automatización **apunta explícitamente a `v1beta2`** en todas sus consultas y modificaciones,
porque es la versión que esos recursos entienden. No es una omisión: leídos por `v1` no exponen
su topología, y escritos por `v1` son rechazados, ya que ese esquema exige un `spec.target` que
no tienen.

### Cuando se actualice el operador

La versión `v1` reemplaza esos campos por `spec.target` y `spec.mirrors[].source`. Al migrar:

1. Reescribir los manifiestos de `KafkaMirrorMaker2` con los campos nuevos.
2. Cambiar `api_versions.mirrormaker2` en `umbrales.yml` a la versión nueva.

El segundo paso es un único valor, y es el motivo por el que la versión está declarada en los
datos y no repetida en cada tarea.

---

## Garantías

| | |
|---|---|
| Ante un estado desconocido | se detiene sin modificar nada |
| Si ya se está en el destino solicitado | lo informa y no modifica nada |
| Ante una validación fallida | se detiene antes de la primera escritura |
| Si el estado cambió tras la aprobación | se detiene |
| Al invertir la replicación | apaga antes de activar, para no duplicar mensajes |
| Al conmutar el tráfico | verifica el destino antes de cortar el origen |
| Al mover las aplicaciones | detiene antes de levantar, para que no procesen en paralelo |
| Tras el cambio | los consumidores retoman en el mensaje donde quedaron |
| Al terminar | confirma que el estado alcanzado es el declarado |
| Sobre los datos | comprueba que ninguna partición falte ni haya perdido mensajes |
| Sobre los consumidores | comprueba que conserven su posición y no reprocesen desde el principio |

Toda modificación es un cambio de configuración declarativo y reversible. La automatización no
elimina ni recrea recursos.
