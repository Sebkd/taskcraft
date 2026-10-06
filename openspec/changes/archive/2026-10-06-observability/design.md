# Design: Наблюдаемость — наблюдатель, метрики, журнал, трассировка

> **Change**: [change.md](change.md)
>
> **Статус**: Готово к реализации
>
> **Автор**: Андрей Серохвостов
>
> **Дата**: 2026-10-06

---

## Context

Журнал в формате 2.9 (`event`, `action`, `message` с `ключ=значение`) уже ведётся в каждой заявке, спан попытки `taskcraft.attempt` уровня DEBUG есть с `worker-loop-and-supervision`. Наблюдателя событий, метрик и возможностей сборки `metrics` и `log` нет. Имя `trace_parent` зарезервировано (`TRACE_PARENT`), но типа значения нет, и спан с ним не связан.

Требования:
- **2.3.22** — события библиотеки идут наблюдателям; адаптер `metrics` — один из них; паника наблюдателя не влияет на задачу (WARN `observer/failed`).
- **4.4.2** — стандартные ряды; **4.4.1** — спаны DEBUG, связь с `trace_parent`.
- **2.9** «Как приложение видит журнал» — цель `taskcraft::*`, подписчика библиотека не ставит, возможность сборки `log`.
- **Критерии:** спецификация №32, №34, №48; критерии заявки №1–№6.

---

## Goals / Non-Goals

**Goals:**

- `Observer`, `Event`, `AttemptEnd`; `Monitor::observer`.
- `MetricsObserver` за возможностью `metrics`.
- Возможность `log` (`tracing/log`).
- Тип `TraceParent`, известный каждому реестру; поле спана попытки.

**Non-Goals:**

- События аренды — с `durable-task-store`, когда появится аренда.
- Связь с родительским спаном OpenTelemetry средствами библиотеки — без зависимости от OpenTelemetry (см. решение ниже).

---

## Decisions

### События — заимствующее перечисление

**Решение**: `Event<'a>` (`#[non_exhaustive]`, `Copy`) с `&str` очереди и `&TaskId`. Варианты (2.3.22 п. 1):

| Группа | Варианты |
|--------|----------|
| Задача | `Pushed`, `Accepted`, `Rejected`, `Duplicate`, `Retry`, `Finished { state, reason }` (успех, ошибка, паника, отмена, «отложена») |
| Попытка | `AttemptStarted`, `AttemptFinished { outcome: AttemptEnd, duration }` |
| Источник и воркер | `DecodeFailed`, `SourceFailed`, `WorkerRestarted`, `SourceClosed`, `WorkerStopped` |
| Занятость | `Occupancy { running, waiting_slot, waiting_retry }`, `PoolUsage { pool, in_use, total }` |

Аргументов и метаданных в событиях нет (инвариант 1.3.18).

**Обоснование**: событий много на каждую задачу; заимствование не выделяет память.

### Наблюдатели — у монитора, до регистрации очередей

**Решение**:
- `Monitor::observer(impl Observer)` добавляет наблюдателя.
- `register` кладёт текущий список в `ObserverCell` очереди (`Arc<OnceLock<Observers>>`). Ячейку разделяют воркер и `QueueHandle` (событие `Pushed`).
- Наблюдатели, добавленные после регистрации очереди, её событий не получают — так же, как пулы.
- Для `Arc<T: Observer>` есть реализация: потребитель держит одну ссылку, чтобы читать собранное, другую отдаёт монитору.

`Observers::emit` вызывает каждого наблюдателя под `catch_unwind`. Паника → WARN `observer/failed` с именем наблюдателя (`Observer::name`, по умолчанию имя типа), остальные получают событие.

### Занятость — счётчики-стражи

**Решение**:
- `Occupancy` очереди: атомарные счётчики «в работе», «ждёт слот или пул», «ждёт повтора».
- `Occupancy::enter` возвращает стража: он уменьшает счётчик и сообщает `Occupancy` при сбросе — на любом пути, включая прерывание попытки и отмену.
- `PoolHeld` оборачивает разрешения пула и сообщает `PoolUsage` при взятии и при возврате; занятость — `size - available_permits`.

**Обоснование**: инвариант 1.3.6 — ресурсы освобождаются на каждом исходе; стражи дают ту же гарантию калибрам.

### Адаптер `metrics`

**Решение**: `MetricsObserver` (возможность `metrics`, зависимость `metrics` 0.24) переводит события в ряды 4.4.2 с теми же именами и тегами:

| Ряд | Событие |
|-----|---------|
| `taskcraft_tasks_finished_total{outcome}` | `Finished` |
| `taskcraft_task_panics_total` | `Finished` с паникой |
| `taskcraft_attempt_duration_seconds{outcome}` | `AttemptFinished`; `outcome` — состояние или `retry` |
| `taskcraft_tasks_running`, `taskcraft_tasks_waiting{state}` | `Occupancy` |
| `taskcraft_pool_permits_in_use`, `taskcraft_pool_permits_total` | `PoolUsage` |
| Остальные счётчики | Одноимённые события |

### Возможность `log`

**Решение**: `log = ["tracing/log"]`. События tracing дублируются записями `log`, пока в процессе нет подписчика tracing (2.9 «Как приложение видит журнал» п. 4).

### `trace_parent` — тип и поле спана

**Решение**:
- `TraceParent(String)` (`serde(transparent)`). `MetadataRegistry::new` регистрирует его под `trace_parent`; `register` по-прежнему не даёт занять это имя.
- Спан попытки получает поле `trace_parent`, если у задачи есть это значение. Поле заполняется только для включённого спана, то есть при `taskcraft=debug`.

**Обоснование**: связать спан с внешним родителем OpenTelemetry без зависимости от OpenTelemetry библиотека не может: идентификатор спана tracing — локальный. Поле даёт слою трассировки потребителя всё нужное для связи. Ядро остаётся без тяжёлых зависимостей (инвариант 1.3.14). Уточнение 4.4.1 — при применении.

**Альтернативы**:
- Возможность `opentelemetry` с `tracing-opentelemetry` и установкой родителя — отложено: не требуется первым потребителем, заметно увеличивает дерево зависимостей.

---

## Risks / Trade-offs

| Риск | Последствия | Mitigation |
|------|------------|------------|
| Медленный наблюдатель | Тормозит воркер и задачи | Документация: быстро и без блокировок |
| Наблюдатель, добавленный после регистрации очереди | Не получает её событий | Документировано в `Monitor::observer` |
| Занятость пула по `available_permits` | Неатомарный снимок при конкуренции | Калибр отражает состояние на момент события, следующее событие поправит |

---

## Open Questions

Нет.

---

## Изменения в коде

| Имя | Расположение | Изменение |
|-----|--------------|----------|
| `Observer`, `Event`, `AttemptEnd`, `Observers`, `Occupancy` | `src/observe.rs` (новый) | Наблюдатель и занятость |
| `MetricsObserver` | `src/observe/metrics.rs` (новый, `metrics`) | Адаптер |
| `Monitor` | `monitor.rs` | `observer`, наблюдатели в очередь при `register`, имя и размер пула в `PoolClaim` |
| `Queue`, `QueueHandle` | `queue.rs`, `handle.rs` | `ObserverCell`; `Pushed` |
| `run_worker`, `execute`, `finish`, `cancel_waiting`, `decide`, `attempt`, `restart` | `worker.rs` | События, стражи занятости, `PoolHeld`, поле `trace_parent` |
| `TraceParent`, `MetadataRegistry::new` | `metadata.rs` | Тип и предрегистрация |
| `Cargo.toml` | | Возможности `metrics`, `log`; dev: `log`, `metrics-util`, `tracing-subscriber/json` |

---

## Тестирование

| Критерий | Тест |
|----------|------|
| Заявки №3 | unit `observe.rs`: паникующий наблюдатель не мешает другим |
| — | unit: стражи занятости; метки `AttemptEnd`; `TraceParent` в реестре |
| №32, заявки №1 | `tests/metrics.rs`: 10 задач всеми исходами — ряды адаптера совпадают со счётчиками своего наблюдателя; имена и теги по 4.4.2 |
| №34 | `tests/metrics.rs`: маркер в аргументах и метаданных отсутствует в журнале и метках при всех исходах |
| №48 | `tests/log_format.rs`: подписчик JSON на INFO — ровно `event`, `action`, `message`, домен из словаря, нет полей спанов, цель `taskcraft*` |
| Заявки №4 | `tests/log_format.rs`: `taskcraft=debug` — `accepted`, `started`, `finished` |
| Заявки №2 | `tests/log_format.rs`: спан попытки несёт `trace_parent` |
| Заявки №5 | `tests/log_bridge.rs` (возможность `log`): события как записи `log` с целью `taskcraft*` |
| Заявки №6 | `tests/no_subscriber.rs`: подписчик не установлен; в исходниках нет макросов печати |
