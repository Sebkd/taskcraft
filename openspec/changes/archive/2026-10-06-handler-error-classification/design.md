# Design: Исход обработчика как данные

> **Change**: [change.md](change.md)
>
> **Статус**: Готово к реализации
>
> **Автор**: Андрей Серохвостов
>
> **Дата**: 2026-10-06

---

## Context

В ядре есть модель задачи (`Task<Args>`, `Metadata`, `MetadataRegistry`, `FinishReason`) и контракт источника (`Source`, `Codec`, `InMemorySource`). Воркера пока нет. Эта заявка определяет, **что** воркер будет вызывать и **как** понимать результат: форму обработчика, извлекаемые значения и классификацию исхода.

Требования (спецификация):

- **2.3.2.** Исход — «успех», «повторить(причина, задержка?)», «прервать(причина)», «отложить(задержка, причина)». Ошибка без классификации — «повторить». Классификация — само значение исхода, упаковка ошибки её не меняет. Паника перехватывается и становится «паникой».
- **2.5 «Обработчик».** Асинхронная функция: аргументы задачи плюс извлекаемые значения. Неверная сигнатура — ошибка компиляции. Обработчик — сервис tower, слои применяются без адаптеров.
- **2.2.11.** Нет обязательных метаданных или они не разбираются — обработчик не вызывается, исход «прервать» с причиной, значение по умолчанию не подставляется.
- **Критерии:** спецификация №5, №6, №27 — в части классификации (запуск и счёт попыток — в `worker-loop-and-supervision` и `retry-policy`); заявка №1 (сигнатура), №2 (слой tower).

Дефект apalis, который закрывает заявка, — B-1: «не повторять» определялось приведением типа `Box<dyn Error>` к `AbortError`, и после упаковки механизм не срабатывал никогда, в том числе для паники.

---

## Goals / Non-Goals

**Goals:**

- `Outcome` — исход попытки как данные, включая «паника».
- `TaskError` — ошибка обработчика, несущая классификацию рядом с источником ошибки. `?` на любой ошибке даёт «повторить», явная классификация — «прервать» или «отложить».
- Обработчик из обычной `async fn(Args, X1, …, Xn)` с извлекаемыми значениями: `Meta<T>`, `Option<Meta<T>>`, `Attempt`, `TaskId`, `Data<T>`.
- Обработчик реализует `tower::Service`; любой слой tower применяется без адаптеров; ошибка сервиса после слоёв — «повторить».
- Перехват паники в границах попытки: `catch_panic`.

**Non-Goals:**

- Извлекаемое значение «признак отмены» — появляется вместе с отменой в `task-registry` (правило 2.3.15). Механизм извлечения допускает его добавление без изменений.
- Запускаемое как результат обработчика — `statecraft-integration`.
- Решение о повторе, счёт попыток, переходы состояний, журнал паники ERROR и метрика — воркер и `retry-policy`.

---

## Decisions

### `Outcome` — перечисление с причинами из `FinishReason`

**Решение**: `Outcome` (`#[non_exhaustive]`):

| Вариант | Поля |
|---------|------|
| `Success` | — |
| `Retry` | `reason: FinishReason`, `delay: Option<Duration>` |
| `Abort` | `reason: FinishReason` |
| `Defer` | `delay: Duration`, `reason: FinishReason` |
| `Panic` | `message: String` |

- Методы: `is_final()` — `Success`, `Abort`, `Panic`.
- Конструкторы `Outcome::retry(reason)`, `abort(reason)`, `defer(delay, reason)` принимают `impl Display` и кладут текст в `FinishReason::Handler`.

**Обоснование**: 2.3.2 п. 1 и 2.11.4 — причины уже определены в `FinishReason`, отсутствие метаданных выражается `FinishReason::MissingMetadata` и `UnparsableMetadata`. Паника — отдельный вариант, а не ошибка: её нельзя спутать с «повторить» (п. 3).

**Альтернативы**:
- Отдельный тип причин для исхода — отклонено: одно и то же значение потом попадает в статус задачи (2.7.4).

### `TaskError`: классификация хранится рядом с ошибкой

**Решение**:
- `TaskError { kind: ErrorKind, source: Box<dyn Error + Send + Sync> }`, где `ErrorKind` — `Retry { delay }`, `Abort`, `Defer { delay }`.
- `impl<E: Into<Box<dyn Error + Send + Sync>>> From<E> for TaskError` с `kind = Retry { delay: None }`: `?` на любой ошибке — «повторить» (2.3.2 п. 2).
- Явная классификация:
  - `TaskError::abort(e)`, `TaskError::retry_after(e, delay)`, `TaskError::defer(e, delay)`;
  - расширение `ResultExt` для `Result<T, E>`: `.or_abort()`, `.or_retry_after(delay)`, `.or_defer(delay)`.
- `TaskError` **не** реализует `std::error::Error` (как `anyhow::Error`): иначе общий `From<E>` конфликтует с `From<T> for T`. Источник ошибки доступен через `source()`, текст — через `Display`.
- Перевод в исход: `From<TaskError> for Outcome` — `FinishReason::Handler(текст источника)` в соответствующем варианте.

**Обоснование**: классификация — поле, а не тип. Упаковка источника в `Box<dyn Error>` её не трогает, поэтому B-1 невозможен по построению. `?` остаётся удобным, а «не повторять» — явное и видимое в коде слово.

**Альтернативы**:
- Downcast к маркерному типу ошибки, как в apalis — отклонено: ровно B-1.
- Обработчик возвращает только `Outcome` — отклонено: теряется `?`, каждый обработчик превращается в ручной `match`.

### Что может вернуть обработчик — трейт `IntoOutcome`

**Решение**: `IntoOutcome` реализован для:
- `()` и `Outcome`;
- `Result<(), TaskError>` и `Result<Outcome, TaskError>`.

Другой тип возврата — ошибка компиляции.

**Обоснование**: 2.5 — неверная сигнатура ловится компилятором. Короткий закрытый список проще объяснять, чем обобщённые реализации.

### Обработчик — функция с извлекаемыми значениями, как в axum

**Решение**:
- Трейт `FromTask<Args>`: `fn from_task(request: &TaskRequest<Args>) -> Result<Self, Rejection>`.
- `Rejection` переводится в `Outcome::Abort` с причиной.
- Реализации:

| Извлекаемое | Поведение |
|-------------|-----------|
| `Meta<T>` | `Metadata::resolve::<T>`. Нет значения → `MissingMetadata { type_name }`; не разобралось → `UnparsableMetadata { name, type_name }` |
| `Option<Meta<T>>` | Нет значения → `None`; не разобралось → по-прежнему отказ (2.3.17, необязательные метаданные) |
| `Attempt` | Номер попытки |
| `TaskId` | Идентификатор задачи |
| `Data<T>` | `Arc<T>` из общих данных очереди. Нет — отказ «общие данные T не зарегистрированы»: ошибка программиста, без значения по умолчанию |

- Трейт `Handler<Args, X>` реализован макросом для `F: Fn(Args, X1, …, Xn) -> Fut + Clone + Send + Sync + 'static`, где `Fut: Future<Output: IntoOutcome> + Send`, при n = 0…8.
- Порядок в вызове:
  1. Все извлекаемые значения достаются из `&TaskRequest`. Первая неудача → `Abort`, обработчик **не** вызывается (2.2.11 п. 2).
  2. Аргументы забираются из задачи по значению.
  3. Вызывается функция.
- `TaskRequest<Args>` — задача плюс `Arc<MetadataRegistry>` для разбора метаданных и `Arc<SharedData>` (общие данные по типу). Конструктор публичный: воркер и тесты собирают запрос сами.
- `task_fn(f) -> TaskFn<F, Args, X>` — обёртка, превращающая функцию в сервис.

**Обоснование**: «Экстракторы в хендлерах» — пункт «Что забрать» разбора: axum-подобный DI без макросов-атрибутов. Аргументы — первый параметр, потому что они есть у каждой задачи; извлекаемые — по необходимости.

**Альтернативы**:
- Атрибутный макрос, как в statecraft-fsm — отклонено: отдельный пакет процедурных макросов ради того, что делает обычная обобщённая функция.
- Аргументы последним параметром, как тело в axum — отклонено: у задачи аргументы всегда есть, их место — первое.

### Обработчик — `tower::Service`, ошибки сервиса — «повторить»

**Решение**:
- `TaskFn` реализует `Service<TaskRequest<Args>>` с `Response = Outcome`, `Error = Infallible`, `Future = BoxFuture<'static, Result<Outcome, Infallible>>`.
- Воркер будет принимать любой `S: Service<TaskRequest<Args>, Response = Outcome>` с `S::Error: Into<BoxError>`.
- Перевод результата сервиса — функция `outcome_of(Result<Outcome, E>) -> Outcome`: `Err` → `Retry` с текстом ошибки. Так слой `Timeout` или `ConcurrencyLimit`, меняющий тип ошибки, работает без адаптеров (критерий заявки №2), а классификация из `Outcome` не теряется.

**Обоснование**: 2.5 и D-3 spike («задача — `Service<Task>`, всё остальное — слои»). Ошибка сервиса — это ошибка без явной классификации, значит «повторить» по 2.3.2 п. 2.

**Альтернативы**:
- Свой трейт сервиса — отклонено: теряется весь экосистемный middleware.

### Перехват паники — свой `CatchPanic` без `unsafe`

**Решение**:
- `catch_panic(future) -> CatchPanic<F>` — future, который вызывает `poll` внутрённего future под `std::panic::catch_unwind(AssertUnwindSafe(..))` и превращает панику в `Err(message)`.
- Сообщение — строка из `&str`/`String` полезной нагрузки, иначе `"panic with a non-string payload"`.
- Функция `run_attempt(service, request) -> Outcome` для воркера:
  1. дождаться готовности сервиса;
  2. вызвать его под перехватом;
  3. выполнить future под перехватом;
  4. перевести результат через `outcome_of`, панику — в `Outcome::Panic`.

**Обоснование**: 2.3.2 п. 3. Своя обёртка в 20 строк вместо зависимости `futures`. `forbid(unsafe_code)` соблюдается: `Pin<&mut F>` для `F: Unpin` не нужен, потому что внутренний future хранится в `Pin<Box<F>>`.

**Альтернативы**:
- `futures::FutureExt::catch_unwind` — отклонено: целый пакет ради одного комбинатора.
- Перехват паники в отдельной задаче tokio (`JoinHandle` с `is_panic`) — отклонено: прячет панику до `join` и заставляет спавнить задачу на каждую попытку.

---

## Risks / Trade-offs

| Риск | Последствия | Mitigation |
|------|------------|------------|
| `TaskError` не реализует `std::error::Error` | Нельзя вернуть его туда, где ждут `dyn Error` | Как у `anyhow`: доступ к источнику через `source()`; задокументировано |
| Боксинг future обработчика | Одна аллокация на попытку | Пренебрежимо рядом со стоимостью задачи; позволяет `Service::Future` без именования типов |
| Паника внутри `Drop` во время раскрутки | Процесс аварийно завершается (двойная паника) | Поведение Rust, вне контроля библиотеки |
| `AssertUnwindSafe` | Состояние, разделённое с паникнувшей задачей, может остаться несогласованным | Задача после паники не повторяется (2.3.2 п. 4); разделяемое состояние потребителя — его ответственность, задокументировано |
| Макрос на 8 извлекаемых значений | Девятое — ошибка компиляции | Больше восьми — признак, что обработчику нужна структура общих данных |

---

## Open Questions

| Вопрос | Статус | Решение/Ответ |
|--------|--------|--------------|
| Где перехватывать панику — в `TaskFn` или в воркере | Закрыт | В `run_attempt`, то есть в границах попытки: так перехват работает для любого сервиса, включая обёрнутый слоями, а не только для `task_fn` |
| Нужен ли извлекаемый признак отмены сейчас | Закрыт | Нет: появится с отменой в `task-registry` |

---

## Архитектура

### Компонентная диаграмма

```
async fn(Args, Meta<T>, Attempt, Data<D>) -> Result<(), TaskError>
        │ task_fn
        ▼
TaskFn ── impl Service<TaskRequest<Args>, Response = Outcome>
        │ любые слои tower (Timeout, ConcurrencyLimit, …)
        ▼
run_attempt(service, request) ──► CatchPanic ──► outcome_of ──► Outcome
                                                               (Success/Retry/Abort/Defer/Panic)
TaskRequest<Args> = Task<Args> + Arc<MetadataRegistry> + Arc<SharedData>
FromTask: Meta<T> | Option<Meta<T>> | Attempt | TaskId | Data<T> ── отказ → Abort(причина)
```

### Новые пакеты/модули

| Пакет/Модуль | Назначение |
|--------------|------------|
| `taskcraft::outcome` (приватный) | `Outcome`, `TaskError`, `ErrorKind`, `ResultExt`, `IntoOutcome` |
| `taskcraft::handler` (приватный) | `TaskRequest`, `SharedData`, `FromTask`, `Rejection`, извлекаемые `Meta`, `Attempt`, `Data`, трейт `Handler`, `task_fn`, `TaskFn` |
| `taskcraft::attempt` (приватный) | `catch_panic`, `CatchPanic`, `outcome_of`, `run_attempt`, `BoxError` |

---

## Изменения в коде

### Новые модули / классы / функции

| Имя | Расположение | Назначение | Ключевые операции |
|-----|--------------|------------|-------------------|
| `Outcome` | `outcome.rs` | Исход попытки | варианты, `retry`, `abort`, `defer`, `is_final` |
| `TaskError`, `ErrorKind` | `outcome.rs` | Ошибка с классификацией | `abort`, `retry_after`, `defer`, `kind`, `source`; `From<E>` |
| `ResultExt` | `outcome.rs` | Классификация на `Result` | `or_abort`, `or_retry_after`, `or_defer` |
| `IntoOutcome` | `outcome.rs` | Допустимые типы возврата | `into_outcome` |
| `TaskRequest<Args>`, `SharedData` | `handler.rs` | Вход обработчика | `new`, `task`, `registry`, `shared`; `SharedData::insert`, `get` |
| `FromTask<Args>`, `Rejection` | `handler.rs` | Извлечение | `from_task`; `Rejection → Outcome` |
| `Meta<T>`, `Attempt`, `Data<T>` | `handler.rs` | Извлекаемые значения | `Deref`, `into_inner` |
| `Handler<Args, X>`, `task_fn`, `TaskFn` | `handler.rs` | Функция → сервис | `call`; `Service` |
| `catch_panic`, `CatchPanic`, `outcome_of`, `run_attempt`, `BoxError` | `attempt.rs` | Перехват паники и перевод в исход | — |

### Изменения в существующих сущностях

| Имя | Расположение | Изменение |
|-----|--------------|----------|
| Корень пакета | `lib.rs` | Модули, реэкспорты, термины «обработчик», «исход», «извлекаемое значение» |

### Интерфейсы / Контракты

| Контракт | Входные параметры | Результат | Ошибки |
|----------|-------------------|-----------|--------|
| `FromTask::from_task` | `&TaskRequest<Args>` | значение | `Rejection` → `Outcome::Abort` |
| `Handler::call` | `TaskRequest<Args>` | `Outcome` | — (отказ извлечения и ошибка — уже в `Outcome`) |
| `run_attempt` | сервис, `TaskRequest<Args>` | `Outcome` | — (паника → `Outcome::Panic`, ошибка сервиса → `Outcome::Retry`) |

---

## Конфигурация

### Изменения в yml/properties

Конфигурационных параметров нет.

---

## Детали реализации

### Алгоритмы

```
Handler::call(request):
  для каждого извлекаемого Xi:
    xi = Xi::from_task(&request); отказ → вернуть Abort(причина отказа)   # обработчик не вызван
  args = request.into_task().into_args()
  вернуть f(args, x1..xn).await.into_outcome()

run_attempt(service, request):
  готовность = poll_ready(service); ошибка → Retry(текст)
  future = catch_panic_sync(|| service.call(request)); паника → Panic(сообщение)
  результат = catch_panic(future).await; паника → Panic(сообщение)
  вернуть outcome_of(результат)
```

### Обработка edge cases

| Ситуация | Поведение |
|----------|----------|
| `?` на ошибке ввода-вывода | `Outcome::Retry { delay: None }` |
| `TaskError::abort` упакован слоем tower в `BoxError` | Невозможно: классификация в `Response`, слой меняет только `Error` |
| Слой `Timeout` сработал | `Err(Elapsed)` → `Outcome::Retry` с текстом ошибки |
| Паника в синхронной части `call` | `Outcome::Panic` |
| Паника с нестроковой полезной нагрузкой | `Outcome::Panic { message: "panic with a non-string payload" }` |
| Два обязательных извлекаемых, первое отказало | `Abort` по первому, второе не вычисляется |
| `Data<T>` без зарегистрированного `T` | `Abort` с причиной `Handler("shared data T is not registered")` |

---

## Миграция

Не требуется.

### Шаги миграции

Нет.

### Совместимость

- **Прямая совместимость**: да
- **Обратная совместимость**: да; интерфейс только расширяется

---

## Зависимости

### Новые зависимости

| Библиотека | Версия | Зачем нужна |
|------------|--------|------------|
| `tower` | 0.5, без возможностей по умолчанию | Трейт `Service` (спецификация 2.6) |
| `tower` (dev) | 0.5, `util`, `timeout`, `limit` | Тест слоёв без адаптеров |

### Изменения в build-файлах

| Файл | Изменение |
|------|----------|
| `Cargo.toml` (корень) | `tower` в `[workspace.dependencies]` |
| `crates/taskcraft/Cargo.toml` | `tower` без возможностей; dev — `tower` с `util`, `timeout`, `limit` |
| `supply-chain/` | Исключения для новых пакетов |

---

## Тестирование

### Unit-тесты

| Что тестируем | Сценарий |
|---------------|----------|
| `TaskError` | `?` на ошибке → `Retry`; `or_abort` → `Abort`; `or_retry_after` и `or_defer` — с задержкой; источник и текст сохраняются |
| Классификация после упаковки | Ошибка, упакованная в `Box<dyn Error>` до и после классификации, даёт тот же `Outcome` (критерий №5, B-1) |
| `IntoOutcome` | `()`, `Outcome`, оба `Result` |
| Извлечение | `Meta<T>` нет → `Abort(MissingMetadata)`; неразборное → `Abort(UnparsableMetadata)`, обработчик не вызван (№27); `Option<Meta<T>>` нет → `None`, неразборное → `Abort`; `Attempt`, `TaskId`, `Data<T>` |
| `catch_panic` | Паника в `poll` → `Err(message)`; `&str` и `String` полезной нагрузки |
| `run_attempt` | Паника в обработчике → `Outcome::Panic` (№6, в части классификации); паника в синхронной части `call`; ошибка сервиса → `Retry` |

### Integration-тесты

| Что тестируем | Сценарий |
|---------------|----------|
| Слои tower | `ServiceBuilder` с `TimeoutLayer` и `ConcurrencyLimitLayer` поверх `task_fn`: успешный вызов → `Success`, медленный → `Retry` по таймауту (критерий заявки №2) |
| Неверная сигнатура | Doc-тесты `compile_fail`: возврат `String`, параметр, не реализующий `FromTask` (критерий заявки №1) |
