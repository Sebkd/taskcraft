# Design: Библиотека не паникует в рабочем коде

> **Change**: [change.md](change.md)
>
> **Статус**: Готово к реализации
>
> **Автор**: Андрей Серохвостов
>
> **Дата**: 2026-10-06

---

## Context

В рабочем коде (`crates/*/src` вне `#[cfg(test)]`) нет ни одного `.unwrap()`. Паниковать могут 7 мест:

| Место | Конструкция | Инвариант |
|-------|-------------|-----------|
| `worker.rs`, цикл приёма | `permit.expect("slots are never closed")`, `place.expect("the waiting room is never closed")` | Семафоры слотов и зала ожидания не закрываются |
| `worker.rs`, цикл приёма | `reject.as_ref().expect("Free gate means reject policy")` | Шлюз `Free` выдаётся только при политике «отказать» |
| `worker.rs`, `execute` | `permit.expect("slots are never closed")`, `permits.expect("pools are never closed")` | Семафоры слотов и пулов не закрываются |
| `metadata.rs`, `encode_as` | `unreachable!` | Функция кодирования ищется по `TypeId` значения |
| `attempt.rs`, `CatchPanic::poll` | `panic!` | Future не опрашивают после завершения |

Тесты содержат около 390 `unwrap`; там это норма.

---

## Goals / Non-Goals

**Goals:**

- Линты clippy на уровне workspace: `unwrap_used`, `expect_used`, `panic`, `unreachable`, `todo` (уже есть как `warn`), разрешение в тестах.
- Убрать 6 мест из 7 без изменения поведения при соблюдённых инвариантах.

**Non-Goals:**

- `CatchPanic` после завершения — остаётся паникой с явным разрешением.

---

## Decisions

### Линты и тесты

**Решение**:
- `[workspace.lints.clippy]`: `unwrap_used`, `expect_used`, `panic`, `unreachable` — `warn`; CI собирает с `-D warnings`.
- `.clippy.toml`: `allow-unwrap-in-tests`, `allow-expect-in-tests`, `allow-panic-in-tests = true`.
- Интеграционным тестам, где clippy не распознаёт тестовый контекст для вспомогательных функций, — `#![allow(...)]` на уровне файла.

**Обоснование**: правило проверяется автоматически (критерий №49); тесты остаются читаемыми.

### Закрытый семафор — штатная ветка вместо паники

**Решение**:
- **Цикл приёма**, ожидание слота или места в зале ожидания: ошибка `acquire_owned` → `break 'intake StopReason::Failed("…semaphore closed")`. Воркер останавливается штатно: дренаж, журнал `worker/stopped` с причиной, отчёт монитору.
- **`execute`**, ожидание слота или пула: ошибка → `cancel_waiting`: «Отменена» без ack, как при остановке. Источник доставит задачу снова.

**Обоснование**: `StopReason::Failed` и есть «сбой самого воркера, ошибка библиотеки». Процесс потребителя не падает, задача не теряется.

### Шлюз отказа несёт хук

**Решение**: `Gate::Free(RejectFn<Args>)` — шлюз политики «отказать» хранит свой хук. `expect` на `reject.as_ref()` уходит: невозможное состояние невыразимо в типе.

### Кодирование метаданных — ошибка вместо `unreachable!`

**Решение**: несовпадение типа в `encode_as` → `serde_json::Error::custom("metadata value is not a <T>")`. Ошибка всплывает как `MetadataError::Encode` при кодировании задачи.

### `CatchPanic` — явное разрешение

**Решение**: `#[allow(clippy::panic)]` на `poll` с комментарием: опрос завершённого future — ошибка вызывающего кода, паника стандартна (`std::future::Ready`, `futures`).

---

## Risks / Trade-offs

| Риск | Последствия | Mitigation |
|------|------------|------------|
| Ветки «семафор закрыт» не покрыты тестами | Код-путь без проверки | Семафоры приватны и не закрываются; ветки — защита от будущих правок, их поведение — уже проверенные пути (`StopReason::Failed`, `cancel_waiting`) |

---

## Open Questions

Нет.

---

## Изменения в коде

| Файл | Изменение |
|------|----------|
| `Cargo.toml` | Линты `unwrap_used`, `expect_used`, `panic`, `unreachable` |
| `.clippy.toml` | Разрешения в тестах |
| `worker.rs` | Семафоры без `expect`; `Gate::Free(hook)` |
| `metadata.rs` | `encode_as` без `unreachable!` |
| `attempt.rs` | `#[allow(clippy::panic)]` с комментарием |
| Тесты | Файловые `#![allow]` там, где нужно |

---

## Тестирование

| Критерий | Проверка |
|----------|----------|
| №49 | `cargo clippy --workspace --all-targets --all-features -- -D warnings` в CI; локально — проба: временный `unwrap` в рабочем коде даёт ошибку |
| Заявки №1 | Все существующие тесты проходят |
