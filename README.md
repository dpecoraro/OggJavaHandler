# GoldenGate KCOP Handler

Implementação de um handler Java com ciclo de vida compatível com o Oracle GoldenGate Java Adapter (javawriter):

- `init(Properties props)`
- `processRecord(...)` (N vezes)
- `destroy()`

Esta base não depende do `ggjava.jar` para compilar, permitindo desenvolvimento local. Em execução no GoldenGate, ele instanciará a classe `com.santander.goldengate.handler.KcopHandler` informada em `custom.properties`.

## Estrutura

- `com.santander.goldengate.handler.KcopHandler`: implementa o ciclo de vida, escreve registros em Parquet usando Avro.
- `com.santander.goldengate.Program`: pequeno simulador local do fluxo `init -> processRecord* -> destroy`.
- `src/main/resources/custom.properties.template`: exemplo de configuração para o Adapter.

## Como rodar localmente

1. Build do projeto:

```bash
mvn -q -DskipTests package
```

2. Executar o simulador com propriedades default:

```bash
java -jar target/golden-gate-kcop-handler-1.0-SNAPSHOT-jar-with-dependencies.jar
```

Saída esperada: `target/demo-output.parquet` com alguns registros exemplo.

3. Executar com um arquivo de propriedades:

```bash
java -Dprops=/caminho/custom.properties -jar target/golden-gate-kcop-handler-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## Integrando com GoldenGate

1. Copie o JAR com dependências (`*-jar-with-dependencies.jar`) para o diretório do GoldenGate ou para um local acessível.
2. Configure o `custom.properties` usando o template em `src/main/resources/custom.properties.template`.
3. No `custom.properties`, aponte `kcoph.handlertype=com.santander.goldengate.handler.KcopHandler`.
4. No parâmetro do writer, referencia o `custom.properties`. Exemplo de trecho do `extract/replicat`:

```
map ... , target ..., COLMAP (...)
-- writer parameters (exemplo genérico) 
-- gg.handlerlist=kcoph
-- kcoph.type=java
-- kcoph.mode=op
-- kcoph.handlertype=com.santander.goldengate.handler.KcopHandler
-- set properties file via adapter startup or parameter dependendo do ambiente
```

> Observação: para usar os tipos/SDKs do GoldenGate (ex: `oracle.goldengate.datasource.*`), adicione o `ggjava.jar` ao Maven local (comentário no `pom.xml`) ou use `systemPath` apontando para o jar no filesystem.

## Propriedades suportadas

- `kcop.output.file`: caminho do arquivo Parquet de saída (ex.: `/ogg/out/kcop-output.parquet`)
- `kcop.avro.schemaFile`: caminho para um `.avsc`
- `kcop.avro.schemaJson`: conteúdo JSON inline do schema
- `kcop.parquet.compression`: `SNAPPY|GZIP|UNCOMPRESSED|ZSTD|LZ4`

## Testes rápidos

Você pode alterar o `Program` para processar também um arquivo JSON linha-a-linha e validar o esquema. Atualmente `processRecord` aceita `String` (JSON) ou `Map<String,Object>`.
