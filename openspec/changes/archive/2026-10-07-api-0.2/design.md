# Design: Публичный API 0.2: ручки, трейты источника, модули

> **Change**: [change.md](change.md)
>
> **Статус**: Готово к реализации
>
> **Автор**: Андрей Серохвостов
>
> **Дата**: 2026-10-07

---

## Context

Что есть в 0.1.1:

- `Queue::builder(name, Arc<S>, codec, service)` при `S: Source`.
- `Queue::handle()` даёт `QueueHandle<S, C, Args>`, причём взять ручку нужно до `Monitor::register(queue) -> Result<Monitor, ConfigError>`.
- Наблюдатели монитора записываются в ячейку очереди (`ObserverCell`) при `register`. Наблюдатель, добавленный после, очередь не видит.
- `Source` — один трейт с 11 методами, семь из них по умолчанию.

Какие источники что реализуют:

| Источник | Своё, кроме опроса и ack |
|----------|--------------------------|
| `InMemorySource` | `subscribe`, `push`, `remove`, `defer` |
| `FaultySource` (тестовый) | `subscribe`, `push` |
| `KafkaSource` | `subscribe`, `notices` (ошибки фиксации) |
| `PgSource` | всё, `ack` пустой: исход пишет `complete` |

- Воркер обобщён по `S: Source` и вызывает `complete`, `progress`, `defer`, `notices` у любого источника.
- `PgSource` принимает любой `Codec<Args, Vec<u8>>`. Метаданные кодек и обработчики разбирают через **разные** реестры: реестр кодека и реестр очереди.
- В корне пакета 74 имени.

Две поправки к спайку, найденные в коде:

- `notices` нужен Kafka, а не только хранилищу (заявка `kafka-commit-errors`).
- `defer` умеет in-memory источник, а не только хранилище.

---

## Goals / Non-Goals

**Goals:** Q-1…Q-6 из заявки; критерии №1–5; руководство по миграции.

**Non-Goals:**
- изменения поведения очередей;
- публикация 0.2.0 — только по команде владельца;
- `cargo semver-checks` в CI — это заявка `ci-quality-gates`.

---

## Decisions

### D-1. Три трейта источника (Q-3)

| Трейт | Методы | Кто реализует |
|-------|--------|---------------|
| `Source` | `capabilities`, `poll`, `ack`; по умолчанию `subscribe`, `notices` | Kafka, любой внешний поток |
| `PushSource: Source` | `push`, `remove`; по умолчанию `defer` | in-memory, тестовый, свои |
| `TaskStore` (сам по себе, **не** `Source`) | `poll`, `push`, `remove`, `defer`, `complete`, `progress`, `status`; по умолчанию `subscribe`, `notices` | PostgreSQL |

- **`notices` остаётся в `Source`.** Поправка к спайку: им пользуется Kafka.
- **`defer` — в `PushSource`.** Отложить можно только там, куда можно положить. Метод по умолчанию отвечает «не поддерживается»; объявление — прежний флаг `Capabilities::with_defer`.
- **`TaskStore` не наследует `Source`.** Иначе хранилище можно передать как обычный источник: `ack` пустой, исход не пишется, задача вернётся после аренды. Теперь такая сборка не компилируется.
- У `TaskStore` нет `ack` и `capabilities`: момент ack зафиксирован (`AckPointSupport::Fixed`), постановка и отложенный повтор есть всегда.
- **Убираются:**
  - `Capabilities::with_push` и `accepts_push`;
  - `PushError::Unsupported`;
  - `PushTaskError::Unsupported`.

  Постановку объявляет трейт. Раздел 2.11.2 «Источник не принимает постановку» удаляется.

**Как воркер вызывает разные трейты.** Без специализации обобщённый воркер не может вызвать `complete` у `S`, если не знает, хранилище ли это. Решение — внутренний запечатанный трейт `Backend` со всеми операциями и три обёртки, публичные в модуле `source`:

| Обёртка | Над | Операции хранилища |
|---------|-----|---------------------|
| `Consumed<S: Source>` | поток | по умолчанию: `complete` = `ack`, `progress` и `status` пусты |
| `Pushed<S: PushSource>` | источник с постановкой | то же, плюс `push`, `remove`, `defer` |
| `Stored<S: TaskStore>` | хранилище | все свои, `ack` пустой, как в `PgSource` сейчас |

Воркер и ручка обобщены по `B: Backend`. В воркере меняются только границы: `S: Source` на `B: Backend`. Имена методов те же.

### D-2. Три способа собрать очередь

| Конструктор | Источник | Кодек | Ручка |
|-------------|----------|-------|-------|
| `Queue::builder(name, Arc<S: PushSource>, codec, handler)` | in-memory, свой | параметр | `QueueHandle<Args>` |
| `Queue::consumer(name, Arc<S: Source>, codec, handler)` | Kafka, свой поток | параметр | `ConsumerHandle<Args>` |
| `Queue::on_store(name, Arc<S: TaskStore>, handler)` | PostgreSQL | встроенный JSON (Q-4) | `QueueHandle<Args>` |

- Имя `builder` остаётся за самым частым случаем.
- `consumer` — новое имя: очередь читает внешний поток, класть в неё из кода нельзя.
- Тип очереди — `Queue<B, C, Svc, Args>`, где `B` — обёртка (`Pushed<InMemorySource<u32>>` и т. п.). Пользователь его почти не пишет.

### D-3. Ручки (Q-1, Q-2)

- **`register` возвращает ручку:** `Monitor::register(queue) -> Result<(Monitor, Handle), ConfigError>`. `Queue::handle` убирается (критерий №1).
- **Две ручки вместо одной с маркером:**
  - `QueueHandle<Args>` — `name`, `live_tasks`, `status`, `fetch_status`, `cancel`, `push`;
  - `ConsumerHandle<Args>` — то же без `push`.

  Постановка в очередь Kafka — ошибка компиляции: у её ручки нет `push` (критерий №3).
- **Стирание типов:** источник и кодек стираются за `Arc<dyn …>` с упакованными future. Цена — одно выделение на постановку, статус и отмену. В цикле воркера стирания нет.
- **Выбор ручки по типу очереди** — запечатанный трейт `HandleKind` на обёртках: `Consumed` → `ConsumerHandle`, `Pushed` и `Stored` → `QueueHandle`.

### D-4. Хранилище и кодек (Q-4)

- **Тип сообщения хранилища:** в ядре `source::StoreMessage` — байты JSON-конверта, с `from_bytes`, `into_bytes`, `as_bytes`. `TaskStore::Message = StoreMessage` задаётся трейтом.
- **Встроенный кодек:** `JsonCodec` реализует `Codec<Args, StoreMessage>` поверх нынешнего `Codec<Args, Vec<u8>>`.
- **`Queue::on_store` кодек не принимает.** `JsonCodec` строится в `build()` из реестра очереди (`metadata_registry`). Кодек и обработчики разбирают метаданные одним реестром — сейчас их два, и рассинхрон ловится только в базе.
- **Как это устроено:** в построителе необязательная фабрика кодека `fn(&MetadataRegistry) -> C`. `build()` применяет её, если она задана. Специализации не нужно.
- **Критерий №4:** другой кодек в очередь хранилища передать негде. `Queue::builder` и `Queue::consumer` хранилище не принимают: оно не `PushSource` и не `Source`.
- **`taskcraft-postgres`:** `PgSource` реализует `TaskStore`, сообщение — `StoreMessage`. Схема базы и формат конверта не меняются.

### D-5. Наблюдатели при `run` (Q-6)

- `register` больше не заполняет ячейку наблюдателей очереди. Ячейка хранится в зарегистрированной очереди, и `run` заполняет её перед стартом воркеров (критерий №2).
- События ручки до `run` — `Pushed` от постановок — наблюдатели не получают, как и сейчас до регистрации. Это записывается в 2.3.22.

### D-6. Модули (Q-5)

| Путь | Что |
|------|-----|
| корень | `Queue`, `QueueBuilder`, `Monitor`, `QueueHandle`, `ConsumerHandle`, `CancelOutcome`, `PushOutcome`, `RejectReason`, `TaskStatus`, `TaskState`, `Lifecycle`, `FinishReason`, `Task`, `TaskId`, `TaskParts`, `AckPoint`, `Metadata`, `MetadataRegistry`, `TraceParent`, `TRACE_PARENT`, `Outcome`, `TaskError`, `ResultExt`, `ErrorKind`, `BoxError`, `task_fn`, `Attempt`, `Meta`, `Data`, `Cancel`, `SharedData`, `InMemorySource`, `RetryPolicy`, `PollStrategy`, `TimeoutOutcome`, `DeadLetter`, `ShutdownReport`, `QueueReport`, `StopReason`, `CancellationToken` |
| `source` | `Source`, `PushSource`, `TaskStore`, `StoreMessage`, `Consumed`, `Pushed`, `Stored`, `Polled`, `CloseReason`, `Capabilities`, `AckPointSupport`, `AckOverrideUnsupported`, `PushResult`, `Withdrawal`, `Completion`, `Progress`, `Notice`, `Notices`, `PushError`, `DeferError`, `WakeHandle`, `WakeSignal`, `Delivery`, `OffsetTracker`, `Poller`, `Wakeup` |
| `codec` | `Codec`, `CodecError`, `IdentityCodec`, `JsonCodec` |
| `handler` | `Handler`, `TaskFn`, `TaskRequest`, `FromTask`, `Rejection`, `BoxFuture`, `HandlerOutput`, `IntoOutcome`, `CatchPanic`, `catch_panic`, `run_attempt`, `outcome_of` |
| `observe` | `Observer`, `Event`, `AttemptEnd`, `MetricsObserver` (возможность `metrics`) |
| `runnable` | `Runnable`, `Run`, `SpawnedMachine`, `OutcomeSlot`, `MachineEnd` |
| `error` | `ConfigError`, `InvalidTransition`, `MetadataError`, `RecoveryError`, `PushTaskError` |
| `testing` | как сейчас (возможность `test-util`) |
| `prelude` | `Queue`, `Monitor`, `QueueHandle`, `Task`, `TaskId`, `task_fn`, `Outcome`, `TaskError`, `ResultExt`, `Attempt`, `Meta`, `Data`, `Cancel`, `RetryPolicy`, `InMemorySource`, `IdentityCodec`, `JsonCodec`, `CancellationToken` |

- Модуль `handler` добавлен к списку спайка: обёртки tower и извлечение значений иначе остались бы в корне.
- В корне 41 имя вместо 74.
- Внутренние файлы не переезжают: публичные модули — это `pub mod` с реэкспортами там, где раскладка совпадает (`codec`, `runnable`, `source`), и отдельные модули-фасады там, где не совпадает (`handler`, `error`, `observe`).

### D-7. Версия и миграция

- **Версия.** Рабочая область поднимается до 0.2.0, вместе с зависимостями пакетов друг от друга. Шаг «Publish (dry run)» пропускает пакеты, пока ядро 0.2.0 не опубликовано. Публикация — по команде владельца.
- **Руководство по миграции** «0.1 → 0.2» — `MIGRATION.md` в корне репозитория. На него ссылаются README и документация пакета (docs.rs). Внутри таблица «было → стало» с кодом по каждому пункту Q-1…Q-6.
- **Критерий №6 (`cargo semver-checks`).** Инструмента на машине нет. Он ставится локально (`cargo install cargo-semver-checks`) и запускается один раз против опубликованной 0.1.1. Результат идёт в отчёт. В CI его вводит заявка `ci-quality-gates`.

### Спецификация

- **2.5 «API»:** регистрация с ручкой, три конструктора очереди, три трейта, две ручки.
- **2.3.22 п. 2:** наблюдатели получают события очередей независимо от порядка добавления.
- **2.11.2:** удаляется «Источник не принимает постановку».
- **Таблица статусов; версия спецификации 0.28.**

---

## Risks / Trade-offs

| Риск | Последствия | Mitigation |
|------|------------|------------|
| Обёртки `Pushed<…>` видны в типе `Queue` и в документации | Шум в сигнатурах | Документированы в модуле `source`; пользователь пишет `Queue<…>` редко — `register` возвращает ручку без обёрток |
| Объём: 46 файлов | Долгое ревью | Одна ветка; задачи по слоям: ядро → пакеты → тесты → примеры → README; каждый слой собирается отдельно |
| Стирание в ручке: выделение на вызов | Постановка медленнее на сотни наносекунд | Сверка с бенчмарком `throughput` и `latency`; цифры в отчёте |
| Свой источник 0.1 перестаёт компилироваться | Пользователям — правка | Руководство: какой трейт реализовать по методам, которые реализованы сейчас |

---

## Open Questions

Нет: всё, что не решено в заявке и спайке, — в D-1…D-7 на согласование.

---

## Затронутые примеры

Все 15 из каталога E-1…E-15 и `examples/axum-service`: замена `queue.handle()` на ручку из `register`; Kafka — `Queue::consumer`, PostgreSQL — `Queue::on_store`; свой источник (`custom-source`) реализует `PushSource`.

---

## Тестирование

| Критерий заявки | Проверка |
|-----------------|----------|
| №1 | Тесты используют ручку из `register`; `Queue::handle` отсутствует (компиляция) |
| №2 | Новый тест: наблюдатель добавлен после `register` получает события |
| №3 | doc-тест `compile_fail`: `push` у ручки Kafka-очереди (`ConsumerHandle`) |
| №4 | doc-тесты `compile_fail`: `Queue::builder` и `Queue::consumer` с хранилищем |
| №5 | `cargo test --workspace --all-features`, тесты Kafka и PostgreSQL, `readme-check`, clippy примеров |
| №6 | `cargo semver-checks` против 0.1.1 локально; версия 0.2.0 |
