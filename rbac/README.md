# Identidad de la automatizacion

Un `ServiceAccount` por sitio con el privilegio minimo que la fase 1 necesita.
Reemplaza el uso de credenciales de administrador del cluster.

## Que incluye

| Objeto | Para que |
|---|---|
| `ServiceAccount kafka-dr` | la identidad con la que AWX se conecta |
| `Secret kafka-dr-token` | token de larga duracion; sin esto caduca en horas |
| `Role kafka-dr` | los permisos, acotados al namespace |
| `RoleBinding kafka-dr` | une los dos |

## Permisos

| Recurso | Verbos | Por que |
|---|---|---|
| `kafkas` | get, list | salud del cluster en el discovery |
| `kafkamirrormaker2s` | get, list, **patch** | leer el estado e invertir la replicacion |
| `virtualkafkaclusters` | get, list, **patch** | leer y conmutar el destino del proxy |
| `kafkaservices` | get, list | precheck de que el destino resuelve |
| `pods` | get, list | ubicar un broker |
| `pods/exec` | create | medir offsets con el Admin API de Kafka |

**Es un `Role`, no un `ClusterRole`.** La automatizacion no puede ver nada fuera de su
namespace.

**No hay `delete` en ningun recurso.** El borrado de topicos es fase 2 y va a llevar una
identidad separada, para que la credencial del failover no pueda borrar datos.

## Aplicar

Una vez por sitio, en su cluster correspondiente:

```bash
# cluster del sitio CO
oc apply -f rbac/core-co.yaml

# cluster del sitio CA
oc apply -f rbac/core-ca.yaml
```

Requiere permisos de administrador **del namespace**, no del cluster.

## Obtener el token

```bash
oc get secret kafka-dr-token -n core-co -o jsonpath='{.data.token}' | base64 -d
```

Ese valor se carga en la credencial de AWX **Kafka DR - Tokens de sitio**, campo
`Token sitio CO` (y el equivalente de CA). Una vez cargado, AWX lo guarda cifrado y no
vuelve a mostrarlo.

## Verificar que alcanza

```bash
oc auth can-i patch kafkamirrormaker2s -n core-co --as=system:serviceaccount:core-co:kafka-dr
oc auth can-i delete kafkatopics      -n core-co --as=system:serviceaccount:core-co:kafka-dr   # debe dar no
```

## Nota para el ambiente del cliente

Los namespaces `core-co` y `core-ca` son los del laboratorio. En el cliente hay que
ajustarlos, pero los verbos y recursos son los mismos: **esta es la respuesta a "que permisos
necesita la automatizacion"**.
