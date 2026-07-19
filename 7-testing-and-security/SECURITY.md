# Securing Kafka

Every cluster in this workshop runs `PLAINTEXT` with no authentication — fine for a local
learning environment, never fine for anything reachable by anyone else. Production Kafka layers
three independent controls on top: encryption in transit, authentication, and authorization.

## Encryption in transit: TLS

By default, everything — including messages, not just the connection handshake — travels
unencrypted between clients and brokers, and between brokers themselves. `SSL` listeners
(Kafka's config calls it `SSL` even though it means TLS) encrypt that traffic. This costs some
CPU and a bit of latency, and is table stakes for anything crossing a network you don't fully
control.

## Authentication: who is this client?

Kafka supports several ways for a client to prove its identity to the broker:

- **SASL/PLAIN** — a username/password, sent over the wire (so this *requires* TLS to not be
  trivially sniffable). Simplest to set up; fine when you also control credential
  provisioning/rotation.
- **SASL/SCRAM** — challenge-response, so the password itself is never sent over the wire even
  under TLS. A meaningfully stronger default than PLAIN for the same operational shape.
- **mTLS (SSL client auth)** — both sides present a certificate; the broker authenticates the
  client's cert instead of a username/password. Common in service-mesh-style environments where
  every workload already has a cert.
- **SASL/GSSAPI (Kerberos)** — the enterprise-SSO answer, common where an organization already
  runs a Kerberos realm (often alongside Hadoop).
- **OAUTHBEARER** — token-based, plugs into an existing OAuth2/OIDC identity provider — the
  common choice for cloud-native deployments issuing short-lived tokens instead of long-lived
  credentials.

Whatever mechanism authenticates the connection, the resulting identity is what
[ACLs](#authorization-what-can-this-identity-do) are written against.

## Authorization: what can this identity do?

Authentication answers "who are you"; **authorization** answers "what are you allowed to do."
Kafka's built-in authorizer evaluates ACLs of the shape `(principal, operation, resource)`:

```bash
kafka-acls --bootstrap-server localhost:9093 --add \
  --allow-principal User:inventory-service \
  --operation Read --operation Write \
  --topic orders

kafka-acls --bootstrap-server localhost:9093 --add \
  --allow-principal User:reporting-service \
  --operation Read \
  --group reporting-consumers \
  --topic orders
```

Default-deny is the right posture: with an authorizer configured, any principal without an
explicit ALLOW is rejected. Grant the narrowest operation set a service actually needs — a
consumer usually needs `Read` on its topics and its own consumer group, not `Write`; a producer
usually needs `Write` and (if the topic doesn't already exist) `Create`.

## Putting it together

A production listener config combines all three:

```properties
listeners=SASL_SSL://0.0.0.0:9093
security.protocol=SASL_SSL
sasl.enabled.mechanisms=SCRAM-SHA-512
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
```

`SASL_SSL` in the listener name is Kafka's shorthand for "this listener requires both an
encrypted connection (SSL) and a SASL authentication handshake" — encryption and authentication
are configured together per listener, while authorization (the authorizer + ACLs) applies
cluster-wide once configured.

## Where this fits in what you've already built

None of the client code in this workshop changes shape when security is turned on — you add
`sasl.mechanism`, `security.protocol`, and credentials to the client config (in `Go`, the
relevant `Dialer`/`kgo.Opt` fields), and everything else (producers, consumers, the admin client
from [stage 4](../4-cluster-administration/), Connect/MirrorMaker from
[stage 5](../5-connect-and-mirroring/)) works exactly as already demonstrated. Security is
additive configuration on top of a client that's already correct, not a different way of
writing one.

Continue to [stage 8: Capstone — Order Processing Pipeline](../8-capstone-order-pipeline/).
