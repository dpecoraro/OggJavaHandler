# GoldenGate KCOP Handler

Handler Java para o **Oracle GoldenGate (Java Adapter / javawriter)** que transforma operações CDC (INSERT/UPDATE/DELETE) em eventos **Avro** e publica em **Kafka**.

> Repositório: `golden-gate-kcop-handler` (Java 11 / Maven)

## Visão geral do fluxo

1. O **Replicat** do GoldenGate executa o **Java Adapter** (`libggjava.so` + `ggjava.jar`).
2. O Adapter instancia a classe `com.santander.goldengate.handler.KcopHandler` (este projeto).
3. Para cada operação CDC, o handler:
   - lê metadados de tabela/colunas via API do GoldenGate
   - cria (ou reutiliza) um **Schema Avro** compatível
   - monta um **envelope** com `beforeImage` / `afterImage` + campos de auditoria
   - calcula uma **chave** (`String`) a partir das colunas-chave
   - publica em um **tópico Kafka** usando `KafkaProducer` + `KafkaAvroSerializer`.

## Principais bibliotecas (com foco no GoldenGate)

### Oracle GoldenGate Java API (`ggjava`)
Dependência definida no [pom.xml](pom.xml) como `systemPath`:
- `com.oracle.goldengate:ggjava:${ogg.version}` (ex.: 21.9.0)

É a API que fornece:
- `oracle.goldengate.datasource.AbstractHandler`: classe base do handler.
- `oracle.goldengate.datasource.DsEvent`, `DsTransaction`, `DsOperation`: eventos, transações e operações CDC.
- `oracle.goldengate.datasource.meta.DsMetaData`, `TableMetaData`, `ColumnMetaData`: metadados das tabelas/colunas (inclui flags como `isKeyCol()`).

**Pontos importantes do ciclo de vida:**
- `init(DsConfiguration config, DsMetaData metaData)`: chamado pelo GoldenGate ao inicializar o handler.
- `operationAdded(DsEvent event, DsTransaction tx, DsOperation operation)`: chamado para cada operação CDC.
- `transactionCommit(...)`: chamado em commit de transação.
- `destroy()`: chamado no encerramento.

### Oracle GoldenGate DB Util (`ggdbutil`)
Também referenciado via `systemPath` no [pom.xml](pom.xml):
- `com.oracle.goldengate:ggdbutil:23.9.2.25.10.001`

> Observação: no código lido, o uso direto de `ggdbutil` não é evidente nas classes principais; ele pode existir por compatibilidade/ambiente de runtime.

### Avro
- `org.apache.avro:avro`

Usado para:
- criar esquemas dinamicamente
- produzir `GenericRecord` para `beforeImage`/`afterImage` e envelope

### Kafka + Confluent
- `org.apache.kafka:kafka-clients`
- `io.confluent:kafka-avro-serializer`

O handler cria um `KafkaProducer<String, GenericRecord>` e configura:
- `key.serializer=org.apache.kafka.common.serialization.StringSerializer`
- `value.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer`

## Estrutura do código (classes relevantes)

### `KcopHandler`
Arquivo: [src/main/java/com/santander/goldengate/handler/KcopHandler.java](src/main/java/com/santander/goldengate/handler/KcopHandler.java)

Responsabilidades principais:
- **Integração GoldenGate:** herda `AbstractHandler` e implementa `init(...)` e `operationAdded(...)`.
- **Leitura de dados CDC:** percorre `DsRecord`/`DsColumn` e monta mapas `beforeImage` e `afterImage`.
- **Schema Avro:** pede ao `AvroSchemaManager` um schema por tabela e usa `SchemaTypeConverter` para ajustes.
- **Envelope CDC:** cria `GenericRecord` com:
  - `beforeImage` (record ou `null`)
  - `afterImage` (record ou `null`)
  - `A_ENTTYP` (tipo de operação)
  - `A_CCID` (transaction id)
  - `A_TIMSTAMP` (timestamp formatado)
- **Chave do Kafka (String):**
  - cria um **schema de chave** (record) baseado em:
    1) override via propriedades (`gg.handler.kafkahandler.keyColumns.<TABELA>`)
    2) default interno (`defaultKeyColumnSpecs`)
    3) fallback para metadado GG (`ColumnMetaData.isKeyCol()`)
  - monta a string da chave concatenando campos com padding quando há `length`.
- **Publicação Kafka:** envia `ProducerRecord<String, GenericRecord>`.

### `AvroSchemaManager`
Arquivo: [src/main/java/com/santander/goldengate/handler/AvroSchemaManager.java](src/main/java/com/santander/goldengate/handler/AvroSchemaManager.java)

Cria e faz cache de schema por tabela:
- define o **record da tabela** com base em `TableMetaData`/`ColumnMetaData`
- cria o **envelope** `AuditRecord` com unions anuláveis:
  - `beforeImage`: `null | <TableRecord>`
  - `afterImage`: `null | <TableRecord>`
  - `A_ENTTYP`, `A_CCID`, `A_TIMSTAMP`, `A_JOBUSER`, `A_USER`: `null | string`

Cada coluna recebe propriedades Avro úteis:
- `logicalType` (ex.: `DECIMAL`, `DATE`, `TIMESTAMP`, `CHARACTER`, `BINARY`)
- `precision`/`scale` quando decimal
- `length` para strings
- `dbColumnName`

### `SchemaRegistryClient`
Arquivo: [src/main/java/com/santander/goldengate/handler/SchemaRegistryClient.java](src/main/java/com/santander/goldengate/handler/SchemaRegistryClient.java)

Cliente simples para registrar schemas no Schema Registry via REST.

No `KcopHandler`, a inicialização ocorre via `schemaRegistryClient.init(kafkaProps)`.
O handler registra cada versão distinta de schema por subject e propaga falhas de
registro; o `KafkaAvroSerializer` também utiliza `schema.registry.url`.

### Helpers (`helpers/*`)
Pasta: [src/main/java/com/santander/goldengate/helpers/](src/main/java/com/santander/goldengate/helpers/)

Utilitários para:
- conversão de tipos e defaults Avro (`SchemaTypeConverter`)
- formatação de datas (`DateFormatHandler`)
- pad/length e tratamento de char (`CharFormatHandler`)
- mapear tipo de operação GG (`EntityTypeFormatHandler`)

## Configuração (GoldenGate)

### Replicat (`replicat.prm`)
Exemplo no arquivo [replicat.prm](replicat.prm):
- carrega o Java Adapter:
  - `TARGETDB LIBFILE libggjava.so SET property=.../custom.properties`
- define `javawriter.bootoptions` com classpath incluindo:
  - `ggjava.jar`
  - o JAR gerado por este projeto (`KcopHandler01.jar` ou `...-jar-with-dependencies.jar`)
  - dependências do Kafka (no exemplo, via pasta `.../dependencies/kafka_3.7.2/*`)

### custom.properties
Template atual em [src/main/resources/custom.properties.template](src/main/resources/custom.properties.template).

**Importante:** esse template parece estar mais orientado a um exemplo “parquet”. Para o fluxo Kafka/Avro deste handler, o que realmente é lido hoje pelo `KcopHandler` é:
- caminho do arquivo de propriedades do producer Kafka (você seta via `setKafkaProducerConfigFile(...)` no handler)
- parâmetros como `schema.registry.url` (diretamente ou via `value.converter.schema.registry.url` / `key.converter.schema.registry.url`)
- `bootstrap.servers`
- opcionalmente, overrides de chave:
  - `gg.handler.kafkahandler.keyColumns.<TABELA>=COL1,COL2,...`


## Build

- Requer Java 11.
- Requer que `ggjava.jar` e `ggdbutil-*.jar` existam no caminho configurado por `ogg.home` do [pom.xml](pom.xml).

Comandos:

```bash
mvn -q clean package
```

O build gera um JAR “fat” (com dependências) via `maven-assembly-plugin`.

## Testes

```bash
mvn -q test
```

Testes existentes:
- [src/test/java/com/santander/goldengate/handler/AvroSchemaManagerTest.java](src/test/java/com/santander/goldengate/handler/AvroSchemaManagerTest.java)
- [src/test/java/com/santander/goldengate/handler/KcopHandlerTest.java](src/test/java/com/santander/goldengate/handler/KcopHandlerTest.java)

## Observações operacionais

- **Schema Registry:** o handler tenta garantir `schema.registry.url` (usando fallbacks `value.converter.schema.registry.url` / `key.converter.schema.registry.url`).
- **Key do Kafka:** hoje o producer usa **key String** (não Avro). Existe um “schema de key” construído, mas ele é usado para definir regras de padding/tipos e não é enviado como Avro.
- **Tópico:** é resolvido por template (`topicMappingTemplate`) via `resolveTopic(...)`.

Propriedades operacionais adicionais no arquivo do producer Kafka:

```properties
# Aceita INFO, DEBUG/FINE, WARN/WARNING e ERROR/SEVERE.
gg.handler.kcoph.logLevel=INFO

# Publica o reportStatus a cada N operações; 0 desabilita.
gg.handler.kcoph.statusLogInterval=10000
```

O `reportStatus()` informa a quantidade de operações processadas. Logs em `INFO`
não incluem payload, chave ou detalhes por coluna.

### Contrato de tipos compatível com CDC DB2

- `DECIMAL(p,0)`: `INT` até 9 dígitos, `LONG` até 18 e `STRING` a partir de 19.
- `DECIMAL(p,s>0)`: `STRING`, preservando `precision` e `scale`.
- `SMALLINT`: `INT`, logical type `SMALLINT`, default numérico `0`.
- `DATE`: `STRING` com logical type `DATE` e length 10.
- `TIMESTAMP`: `STRING` com logical type `TIMESTAMP` e length 32.
- `CHAR/CHARACTER`: usa o comprimento lógico declarado, sem divisão heurística por três.

Precisão decimal e comprimento de texto ausentes interrompem a criação do schema,
evitando publicar contratos aproximados. O contrato completo usado nos testes está
em `src/test/resources/contracts/db2-reference-schema.avsc`.

Os schemas validados no relatório `index-prd 2 (2).html` foram extraídos para
`src/main/resources/contracts/db2-schema-contracts.json`. O HTML não é necessário
no build nem em runtime e pode ser removido do repositório. Para as 166 tabelas do
contrato, o handler usa diretamente o schema DB2 de valor e, quando disponível, o
schema DB2 de chave. Isso preserva inclusive contratos diretos sem envelope, `TIME`,
`VARCHAR`, SMALLINT/INTEGER sem precision/scale e exceções específicas por coluna.
Tabelas ausentes no contrato continuam usando o mapeamento dinâmico por metadata.

---

Se você me disser qual é o padrão real do `custom.properties` em produção (quais chaves vocês usam para apontar `kafkaProducerConfigFile`, `topicMappingTemplate` e `namespacePrefix`), eu ajusto o README para refletir exatamente a configuração usada no ambiente.
