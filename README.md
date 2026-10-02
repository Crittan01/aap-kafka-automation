# Automatización Kafka Activo/Pasivo — AAP

Automatización en Ansible Automation Platform de los procedimientos de **Failover**,
**Failback** y **Rotación** sobre Kafka con MirrorMaker 2 en OpenShift.

> **Fase actual: 1 — Failover CORE.** Certificada de punta a punta en AWX: workflow completo
> con nodo de aprobación, ejecución real contra el laboratorio en ambas direcciones, sin pérdida
> ni duplicación de mensajes. Lo demás está proyectado. Ver *Hoja de ruta*.

---

## Principio de diseño

AAP no ejecuta una receta a ciegas. **Descubre** el estado real, lo **valida** contra un contrato
de transiciones permitidas, le **propone** al operador qué va a hacer, ejecuta y **verifica**.
Si encuentra un estado que no reconoce, se detiene.

Nada asume quién es el sitio activo: después de una rotación los roles quedan invertidos de
forma permanente. El activo se descubre en cada corrida.

### Tres capas

| Capa | Qué es | Dónde |
|---|---|---|
| **Operaciones** | secuencia y aprobaciones | `playbooks/` + Workflows de AAP |
| **Capacidades** | verbos atómicos y reutilizables | `roles/` |
| **Datos** | quién es quién, qué se permite | `playbooks/group_vars/`, `transitions.yml` |

Un rol nunca sabe que existe CORE. Recibe una pareja y actúa sobre ella.

### La abstracción: *pareja*

Una pareja son dos Kafka en sitios opuestos con sus dos MirrorMaker y su proxy. CORE e
Integrations son estructuralmente idénticas.

**Agregar Integrations (fase 4) es agregar un bloque a `parejas.yml`, sin código nuevo.**

---

## Estructura

```
transitions.yml      ★ el contrato — qué transiciones se permiten
inventory/
  hosts.ini            localhost — todo corre desde el EE
playbooks/
  discover.yml         solo lectura
  preflight.yml        simulacro — ¿funcionaría hoy?
  failover.yml         la operación
  group_vars/all/
    parejas.yml      ★ quién es quién — namespaces, recursos, tópicos
    umbrales.yml       timeouts, lag tolerado, versiones de API
roles/
  kafka_discover/      lee el estado de una pareja, incluido el lag
  kafka_decide/        valida contra el contrato; propone o verifica
  mm2_state/           ESCRIBE — MirrorMaker activo/pasivo
  proxy_target/        ESCRIBE — destino del proxy
rbac/                  ServiceAccount con privilegio mínimo, por sitio
```

> Los `group_vars` van junto a los **playbooks**, no al inventario. AWX usa su propio
> inventario y nunca leería `inventory/group_vars/`; adyacentes al playbook se cargan igual
> en ejecución local y en AWX.

**Solo dos roles escriben.** Los otros dos leen o calculan.

---

## Contratos de datos

Los roles se comunican por dos estructuras. Son el contrato interno del proyecto.

### `kafka_estado` — lo produce `kafka_discover`

```yaml
kafka_estado:
  pareja: core
  descubierto: "2026-09-30T18:42:11Z"
  activo: co                      # derivado del MirrorMaker encendido
  pasivo: ca
  coherente: true                 # bool — ningún sitio caído, un solo MM activo y sano
  sitios_caidos: []
  proxy_destinos_rotos: []        # KafkaService con ResolvedRefs != True
  sitios:
    co:
      alcanzable: true
      kafka_ready: true
      kafka_version: "4.1.0"
      proxy_destino: kafka-co-srv
      proxy_destino_sitio: co
      proxy_redirigido: false     # apunta fuera de su propio sitio
      proxy_destinos_rotos: []
    ca: { ... }
  mirrormakers:
    co_ca:
      vive_en: ca                 # el MM vive en el sitio DESTINO
      estado: ACTIVO              # ACTIVO | PASIVO | OFF | DESCONOCIDO
      replicas: "1"
      conectores_ok: true
      conectores: 2
    ca_co: { ... }
  replicacion:
    medida: true
    offsets:                      # por sitio, clave "topico:particion"
      co: { "bpdtransactstreaming-event-topic:0": 26 }
      ca: { "bpdtransactstreaming-event-topic:0": 26 }
    under_replicated: { co: false, ca: false }
```

**Estados del MirrorMaker.** `OFF` es "el recurso no existe"; `DESCONOCIDO` es "no pude
consultarlo porque el sitio no responde". No son lo mismo y confundirlos haría decidir sobre
información que no se tiene.

### `kafka_plan` — lo produce `kafka_decide` en `modo: proponer`

```yaml
kafka_plan:
  transicion: FAILOVER
  operacion: failover
  pareja: core
  sitio_a: co                     # activo al momento de decidir
  sitio_b: ca                     # destino
  mm_hacia_b: co_ca               # el MM resuelto a nombre concreto
  mm_hacia_a: ca_co
  acciones: [ ... ]               # copiadas de transitions.yml
  esperado: { ... }               # el bloque 'hasta' de la transición
```

Los roles que escriben consumen `kafka_plan`: no deciden nada, ejecutan un plan resuelto.

**`modo: verificar` lee los sitios del plan, no del estado nuevo.** Después del failover el
activo cambió; re-derivarlos invertiría la expectativa y daría un falso negativo.

---

## Uso

```bash
ansible-galaxy collection install -r collections/requirements.yml

# Solo lectura — el estado real en pantalla
ansible-playbook playbooks/discover.yml -e pareja=core

# Simulacro — valida y propone, sin tocar nada
ansible-playbook playbooks/preflight.yml -e pareja=core -e operacion=failover

# La operación (en AAP va con nodo de aprobación)
ansible-playbook playbooks/failover.yml -e pareja=core
```

Los tokens se pasan como `kafka_tokens: { co: ..., ca: ... }`. En AAP los inyecta un credential
type propio; en local, por extra_vars o vault.

---

## En AAP

| | |
|---|---|
| **Credenciales** | una por sitio (`ocp-co`, `ocp-ca`). Ningún Job Template que escriba recibe acceso a los dos |
| **Survey** | solo la intención (`operacion`, `pareja`). El sitio destino y el escenario los determina el discovery |
| **Workflow** | `preflight` → **aprobación** → `failover` |
| **Artifacts** | `preflight` publica `wf_kafka_estado` y `wf_kafka_plan` por `set_stats` |

### Objetos configurados

| Objeto | Detalle |
|---|---|
| Proyecto | Git, rama `develop`, sync al lanzar |
| Credential type | `Kafka DR - Tokens de sitio` — inyecta `kafka_tokens` con el token de cada sitio |
| Inventario | un único `localhost`; todo corre desde el EE contra las APIs |
| Job Templates | `Discover`, `Preflight` (con survey), `Failover` (`exigir_plan_aprobado: true`) |
| Workflow | los tres nodos encadenados, con aprobación de por medio |

El *credential type* es un objeto de sistema: **requiere superusuario de AWX**, no alcanza con
ser administrador de la organización.

El operador no le dice a AAP qué hacer: AAP le dice qué encontró y qué propone, y el operador
confirma. Así se elimina el error de ejecutar el escenario equivocado.

### Por qué los artifacts llevan prefijo `wf_`

AAP inyecta los `set_stats` del nodo anterior como **extra_vars**, que tienen mayor precedencia
que `set_fact`. Un artifact llamado `kafka_plan` pisaría en silencio el fact interno que
`kafka_decide` acaba de calcular, y se ejecutaría el plan viejo. Por eso los artifacts se
publican como `wf_kafka_estado` y `wf_kafka_plan`.

### Doble validación antes de escribir

`failover.yml` no confía en el plan aprobado: vuelve a descubrir y a decidir por su cuenta, y
después **compara** su plan recién calculado contra `wf_kafka_plan`. Si el estado cambió entre
la aprobación y la ejecución, se detiene.

Esa diferencia de comportamiento es **declarada, no inferida**, con la variable
`exigir_plan_aprobado`:

| | `false` — local | `true` — Job Template de AWX |
|---|---|---|
| El plan aprobado llega | se ignora | se compara |
| El plan aprobado **no** llega | sigue, avisando que corre sin aprobación | **falla**: revisar el cableado del workflow |

El caso que esto rescata es el de abajo a la derecha: un workflow mal cableado donde el
artifact no llega, la comparación se saltearía en silencio y todos creerían que la validación
corrió.

### Lo que ve el operador antes de aprobar

```
═══ PROPUESTA · FAILOVER ═══

ESTADO ENCONTRADO
  activo: co    pasivo: ca
  co_ca: ACTIVO    ca_co: PASIVO
  proxy co -> kafka-co-srv
  replicacion: al dia

VOY A HACER
  co_ca -> PASIVO
  ca_co -> ACTIVO
  proxy co -> Kafka de ca

VA A QUEDAR
  activo: ca    pasivo: co

PRECHECKS  todos superados
```

---

## Prechecks

La transición los **nombra** en `transitions.yml`; la implementación vive en
`kafka_decide/tasks/prechecks.yml`.

| Precheck | Qué exige |
|---|---|
| `sitio_b_alcanzable` | el API del sitio destino responde |
| `sitio_b_kafka_ready` | el Kafka destino está `Ready` |
| `sitio_b_sin_under_replicated` | sin particiones sub-replicadas en el destino |
| `replicacion_al_dia` | el atraso por partición no supera `lag_maximo_mensajes` |
| `proxy_destino_b_resuelve` | el `KafkaService` destino tiene `ResolvedRefs: True` |

> El último existe por un defecto real: el 29/09 los `KafkaService` que cruzan de sitio no
> resolvían. Un failover habría cortado el tráfico y después fallado.

---

## Reglas de construcción

1. Nada de `CO` ni `CA` escrito en los roles. Son datos.
2. La matriz vive en `transitions.yml`, no en condicionales.
3. Variables internas prefijadas por rol (`_kd_`, `_dc_`, `_ms_`, `_pt_`). `set_fact` tiene mayor
   precedencia que las vars de tarea: sin prefijo, un rol contamina al siguiente.
4. Idempotente: `replicas: 0` antes que eliminar; `state: patched`, nunca `oc edit`.
5. Todo soporta `check_mode` — es lo que hace posible el simulacro.
6. **Anclar `kafka.strimzi.io/v1beta2`** en toda consulta y patch de MirrorMaker. Leído por `v1`
   la topología **no existe** y el discovery devuelve vacío *sin dar error*.
7. Los enteros van en un dict templado, no en un escalar entre comillas: `"{{ x | int }}"` se
   envía como cadena y el CRD lo rechaza con un 422.
8. Tópicos por lista explícita, nunca patrón abierto.
9. Los `group_vars` van junto al playbook, no al inventario: AWX usa el suyo.
10. `ansible-lint` perfil `production` limpio antes de commitear.

---

## Alcance de la fase 1

**Cubre:** descubrir el estado, validar la transición, correr los prechecks, proponer, invertir
los MirrorMaker, conmutar el proxy y verificar el resultado.

**No cubre:** volver. Correr `failover.yml` dos veces **no es un failback** — la transición
FAILOVER solo declara el proxy del sitio que deja de ser activo, así que el otro queda
redirigido. Devolverlo es parte del failback, que es otra transición con acciones propias.

Tampoco cubre el borrado de tópicos, la espera de sincronización, Integrations, ni el escalado
de aplicaciones.

---

## Hoja de ruta

| Fase | Alcance | Roles nuevos |
|---|---|---|
| **1** | **Failover CORE** | 4 |
| 2 | Failback CORE | 1 — `topic_reset` |
| 3 | Rotación MM CORE | 0 |
| 4 | Integrations | 0 — un bloque en `parejas.yml` |
| 5 | Proxy | 0 — incluido desde la fase 1 |
| 6 | CORE ↔ Integrations | 1 — `mm2_repoint_source` |

---

## Identidad en los clusters

`rbac/` trae un `ServiceAccount` por sitio con el privilegio mínimo que la fase 1 necesita, para
reemplazar el uso de credenciales de administrador. Es un `Role` acotado al namespace y **sin
`delete` en ningún recurso**: el borrado de tópicos llega en la fase 2 y llevará una identidad
separada. Ver `rbac/README.md`.

## Nota sobre `_contexto/`

Contiene documentación del cliente, análisis, evidencia de discovery e imágenes. **Está en
`.gitignore` y no se publica**: es material de trabajo con documentos confidenciales. Este
repositorio solo lleva la automatización.
