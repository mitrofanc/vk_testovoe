# gRPC сервис на Java + Tarantool

Maven-приложение: Key-Value сервис на Java 17 с gRPC API и Tarantool в качестве хранилища. 

## Что требовалось сделать

По заданию нужно было реализовать gRPC API с операциями:

- `put(key, value)` - сохраняет в БД значение для новых ключей и перезаписывает значение для существующих
- `get(key)` - возвращает значение для указанного ключа
- `delete(key)` - удаляет значение для указанного ключа
- `range(key_since, key_to)` - отдает gRPC stream пар ключ-значение из запрошенного диапазона
- `count()` — возвращает количество записей в БД

Дополнительные требования:

- Tarantool `3.2.x`
- Java SDK `1.5.0`
- хранение в space `kv`
- схема space:
  - `key: string`
  - `value: varbinary, nullable = true`
- корректная работа с `null` в `put/get`
- корректная работа при большом количестве записей, вплоть до `5_000_000`

## В проекте реализовано:

- gRPC-сервер на Java 17
- protobuf-контракт в `src/main/proto/kv.proto`
- отдельный application layer
- отдельный storage layer для Tarantool
- инициализация Tarantool через YAML-конфиг и `init.lua`
- Docker Compose сценарии dev и prod
- unit tests без Docker
- integration tests с реальным Tarantool через Testcontainers
- load tests, вынесенные в отдельные Maven profiles
- отдельный full-load сценарий с preseeded dataset для проверки на больших объёмах

То есть проект можно:

- запускать локально, когда Java-сервис работает на хосте, а Tarantool в Docker (dev)
- запускать целиком в Docker (prod)
- отдельно прогонять быстрые тесты, интеграционные тесты и длинные нагрузочные сценарии

## Выбранный стек

- `Java 17`
- `Maven` - сборка, генерация protobuf/gRPC кода, разделение профилей тестирования
- `gRPC + protobuf` - транспортный контракт
- `Tarantool 3.2.1` - хранение данных
- `tarantool-client:1.5.0` - Java SDK для работы с Tarantool
- `Docker / docker-compose` - локальная инфраструктура и полноценный запуск
- `JUnit 5` - unit и integration/load тесты
- `Testcontainers` - запуск реального Tarantool в integration и load тестах
- `Lombok` - отказ от boilerplate кода
- `SLF4J` - логирование

## Архитектура проекта

Структура кода разделена по слоям:

```text
src/main/java/vk/kvstore/
  app/        entry point и lifecycle сервера
  config/     загрузка конфигурации
  grpc/       gRPC endpoints и protobuf mapping
  model/      доменная модель
  service/    бизнес-логика и валидация
  storage/    Tarantool repository и client factory
  util/       helpers

src/main/proto/
  kv.proto

src/test/java/
  unit tests

src/integrationTest/java/
  integration tests с Tarantool через Testcontainers

src/loadTest/java/
  два сценария нагрузочного тестирования

infra/
  docker-compose*.yml
  tarantool/
    init.lua
    kv_config.yml
    prepare_full_dataset.lua
```

Runtime flow:

1. `GrpcServerApplication` - точка входа.
2. `ServerLifecycleManager` создаёт и запускает `GrpcServer`.
3. `GrpcServer` поднимает Netty gRPC server, создаёт Tarantool client и регистрирует shutdown logic.
4. `KvGrpcService` отвечает только за gRPC слой: принимает protobuf-сообщения, вызывает сервисный слой и возвращает ответы.
5. `DefaultKeyValueService` содержит бизнес-логику и валидацию входных данных.
6. `TarantoolKeyValueRepository` инкапсулирует доступ к Tarantool.


## Учет требований по ТЗ

### `put`

`put` реализован через `replace` в Tarantool:

- новый ключ вставляется
- существующий ключ перезаписывается
- `count()` не увеличивается при overwrite
- `value = null` поддерживается

### `get`

`get` явно различает три состояния:

- ключ отсутствует
- ключ существует, `value = null`
- ключ существует, `value = empty bytes`

Это реализовано с помощью:

- `GetResponse.found`
- `NullableBinaryValue` с `oneof { data | null_value }`

### `delete`

`delete` возвращает `deleted = true`, если ключ реально существовал, и `deleted = false`, если удалять было нечего. Повторное удаление не ломает `count()`.

### `range`

`range(key_since, key_to)` реализован как **server-streaming RPC**.

Семантика диапазона:

- нижняя граница включена
- верхняя граница исключена
- если `key_since >= key_to`, поток пустой

Диапазон не загружается целиком в память. Storage layer читает данные батчами через primary `TREE` index и сразу стримит их наружу.

### `count`

Используется Lua-функция `kv_count`, которая вызывает `box.space.kv:len()`. Для `memtx` это даёт точное количество записей без полного обхода на каждый запрос.

### `null` values

На уровне protobuf `null` не смешивается с empty bytes:

```proto
message NullableBinaryValue {
  oneof kind {
    bytes data = 1;
    google.protobuf.Empty null_value = 2;
  }
}
```

### Space `kv`

Space `kv` создаётся в `infra/tarantool/init.lua` со схемой:

```lua
{
    {name = 'key', type = 'string'},
    {name = 'value', type = 'varbinary', is_nullable = true}
}
```

- `engine = 'memtx'`
- `format`
- primary `TREE` index по полю `key`

### Корректная работа при наличии 5_000_000 записей в спейсе

Для данного требования было сделано два режима:

- `-Pload` — representative load tests, которые подходят для регулярного тестирования
- `-Pload-full` — отдельный full scenario на preseeded dataset

Полный сценарий не пытается каждый раз заливать `5_000_000` записей последовательно через Java.
Вместо этого база готовится один раз внутри Tarantool, сохраняется как preseeded dataset, а дальше переиспользуется.

### Персистентность Tarantool

В Docker Compose используется named volume `tarantool_data`, смонтированный в `/var/lib/tarantool`. Там лежат:

- snapshot
- WAL
- рабочие файлы Tarantool

### Безопасность доступа

В `init.lua` при старте создаётся и настраивается отдельный пользователь `kv_app`:

- пароль передаётся через `TARANTOOL_PASSWORD`
- выдаются только `read, write` на space `kv`
- `execute` выдаётся только на функцию `kv_count`

Права на `universe` для `guest` не выдаются.

## Особенности реализации

### 1. Tarantool на YAML-конфиге

Tarantool конфигурируется через `kv_config.yml`, а `init.lua` уже поверх применённого конфига создаёт schema objects и пользователя приложения.

Это удобнее, чем держать всё в одном большом `box.cfg{...}`.

### 2. Разделение режимов Docker

Есть три compose-файла:

- `infra/docker-compose.yml` — базовый Tarantool, без публикации порта наружу
- `infra/docker-compose.dev.yml` — локальная разработка, публикует `127.0.0.1:3301`
- `infra/docker-compose.app.yml` — полный стек, добавляет контейнер с gRPC сервером

### 3. Range сделан батчевым и потоковым

`range` сделан так, что данные читаются батчами по index и сразу уходят в gRPC stream. Для больших диапазонов это быстрее.

### 4. Отдельный full-load сценарий тестирования на большом объеме данных

Самый тяжёлый сценарий вынесен в отдельный профиль `load-full`. На первом запуске он:

- готовит большой dataset внутри Tarantool
- делает snapshot
- сохраняет dataset на диск

На повторных прогонах база не собирается заново, а переиспользуется. 
Это значительно уменьшает время повторного тестирования.

## Запуск проекта

### Dev-сценарий: Java на хосте, Tarantool в Docker

Поднять Tarantool:

```bash
cd infra
vim .env 
# задать TARANTOOL_PASSWORD в .env
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d
```

Запустить Java-сервис:

```bash
cd ..
vim src/main/resources/application.properties
# задать tarantool.password
mvn exec:java
```

Остановить Tarantool:

```bash
cd infra
docker compose -f docker-compose.yml -f docker-compose.dev.yml down
```

###  Prod-сценарий в Docker

В этом режиме Tarantool и gRPC сервер работают в одном compose-стеке:

```bash
cd infra
vim .env 
# задать TARANTOOL_PASSWORD в .env
docker compose -f docker-compose.yml -f docker-compose.app.yml up -d --build
```

Остановить:

```bash
cd infra
docker compose -f docker-compose.yml -f docker-compose.app.yml down
```

Логи:

```bash
cd infra
docker compose -f docker-compose.yml -f docker-compose.app.yml logs -f
```

В данном режиме наружу публикуется только gRPC-порт `127.0.0.1:9090`. Tarantool остаётся внутренним сервисом Docker-сети.

## Запуск тестов

### Unit tests

```bash
mvn clean test
```

### Integration tests

Тесты с реальным Tarantool через Testcontainers:

```bash
mvn verify -Pintegration
```

Только скомпилировать integration tests без выполнения:

```bash
mvn verify -Pintegration -DskipITs=true
```

### Representative load tests

Обычный нагрузочный профиль:

```bash
mvn verify -Pload
```

Параметры:

```bash
-Dload.dataset.size=20000
-Dload.smoke.seed.size=5000
-Dload.smoke.iterations=1000
```

### Full load test на preseeded dataset

Первый запуск на `5_000_000` записей:

```bash
mkdir -p ${HOME}/.kvstore-load-cache

mvn clean verify -Pload-full \
  -Dload.full.dataset.size=5000000 \
  -Dload.full.dataset.root=${HOME}/.kvstore-load-cache \
  -Dload.full.prepare.progress.step=250000 \
  -Dload.full.prepare.timeout.minutes=180 \
  -Dload.full.runtime.timeout.minutes=120
```

Повторный запуск с подготовленным dataset:

```bash
mvn clean verify -Pload-full \
  -Dload.full.dataset.size=5000000 \
  -Dload.full.dataset.root=${HOME}/.kvstore-load-cache
```

Base dataset хранится вне `target`, чтобы его не удалял `mvn clean`

## Использование сервиса

В проекте доступны RPC:

- `Put(PutRequest) -> PutResponse`
- `Get(GetRequest) -> GetResponse`
- `Delete(DeleteRequest) -> DeleteResponse`
- `Range(RangeRequest) -> stream RangeEntry`
- `Count(CountRequest) -> CountResponse`

Семантика ответов:

- отсутствие ключа: `found = false`, `value` не установлен
- `null` значение: `found = true`, `value.null_value`
- empty bytes: `found = true`, `value.data = ""`

Примеры удобно проверять через `grpcurl`. 

`put` с бинарным значением:

```bash
grpcurl -plaintext \
  -import-path src/main/proto \
  -proto kv.proto \
  -d '{"key":"alpha","value":{"data":"VKTEST"}}' \
  localhost:9090 kv.KvStore/Put
```

`put` с `null`:

```bash
grpcurl -plaintext \
  -import-path src/main/proto \
  -proto kv.proto \
  -d '{"key":"beta","value":{"nullValue":{}}}' \
  localhost:9090 kv.KvStore/Put
```

`get`:

```bash
grpcurl -plaintext \
  -import-path src/main/proto \
  -proto kv.proto \
  -d '{"key":"alpha"}' \
  localhost:9090 kv.KvStore/Get
```

`range`:

```bash
grpcurl -plaintext \
  -import-path src/main/proto \
  -proto kv.proto \
  -d '{"keySince":"a","keyTo":"z"}' \
  localhost:9090 kv.KvStore/Range
```

`count`:

```bash
grpcurl -plaintext \
  -import-path src/main/proto \
  -proto kv.proto \
  -d '{}' \
  localhost:9090 kv.KvStore/Count
```

## Инфраструктура

### Dockerfile

`Dockerfile` использует multi-stage build:

- на первом этапе Maven собирает проект и runtime dependencies
- на втором этапе запускается JRE-образ
- entrypoint - `vk.kvstore.app.GrpcServerApplication`

### Compose-файлы

- `infra/docker-compose.yml` - Tarantool, volume, healthcheck, без `ports`
- `infra/docker-compose.dev.yml` - только локальная публикация `127.0.0.1:3301:3301`
- `infra/docker-compose.app.yml` - контейнер с Java/gRPC сервисом

### Где лежат данные Tarantool

Для Docker-сценария данные находятся в `/var/lib/tarantool`:

- snapshot
- WAL
- рабочая директория инстанса

Для full-load preseeded dataset используется отдельный каталог на хосте, который задаётся через `load.full.dataset.root`.
