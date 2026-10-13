# Design: Свои типы ошибок вместо типов драйверов

> **Change**: [change.md](change.md)
>
> **Статус**: Реализовано
>
> **Автор**: Андрей Серохвостов
>
> **Дата**: 2026-10-13

---

## Context

Типы драйверов попадают в публичный API пакетов в двух местах.

**Ошибки** — их и меняет заявка:
- `PgStoreError::Database(#[from] sqlx::Error)` (`crates/taskcraft-postgres/src/store.rs:41`). `#[from]` порождает публичный `impl From<sqlx::Error> for PgStoreError`.
- `KafkaSourceError::Client(#[from] KafkaError)` (`crates/taskcraft-kafka/src/source.rs:35`): то же для `rdkafka::error::KafkaError`.

Мажорное обновление `sqlx` (0.9 → 0.10) или `rdkafka` меняет тип в варианте и в `impl From` — несовместимость наших пакетов, даже если наш код не изменился.

**Пул приложения** — остаётся: `PgStoreBuilder::with_pool(sqlx::PgPool)` и `PgStore::pool() -> &PgPool`. Это осознанная связь: хранилище делит пул с приложением (в том числе пул sea-orm). Её не убрать, не отказавшись от возможности. После этой заявки мажорное обновление `sqlx` всё ещё будет несовместимым изменением `taskcraft-postgres`. Но только из-за пула, а не из-за каждой ошибки.

Внутри пакетов `?` на результатах драйвера опирается на этот `From`: около 50 мест в `taskcraft-postgres` и 8 в `taskcraft-kafka`.

## Goals / Non-Goals

**Goals**
- В сигнатурах публичных ошибок нет типов драйверов. Исходная ошибка драйвера доступна через `std::error::Error::source()`, то есть понижающим приведением, без обещания типа.
- Текст ошибок (`Display`) не меняется.

**Non-Goals**
- Отвязка от `sqlx::PgPool` (см. Context).
- Изменение поведения при ошибках и их классификации.

## Decisions

### D-1. Непрозрачные обёртки

```rust
/// A failure of the database driver. Its cause is the driver's error,
/// reachable through `source()`; its type is not part of this API.
pub struct DatabaseError(Box<dyn std::error::Error + Send + Sync>);
// Display — the driver's message; Debug; Error::source — the driver's error.

pub enum PgStoreError { …, Database(DatabaseError), … }
```

То же в Kafka: `KafkaClientError` и `KafkaSourceError::Client(KafkaClientError)`. Внутри ящика лежит ошибка драйвера целиком, так что приложение, которому она нужна, получает её через `source()` и `downcast_ref::<sqlx::Error>()`. В `MIGRATION.md` будет пример.

Хранить `Box<dyn Error>`, а не `sqlx::Error`, нужно для того, чтобы тип драйвера не попал даже в приватное поле публичной структуры. Тогда обновление драйвера — внутреннее дело пакета.

### D-2. Без публичного `From<тип драйвера>`

`#[from]` у вариантов убирается. Преобразование внутри пакета — приватный трейт-расширение:

```rust
pub(crate) trait OrDb<T> { fn db(self) -> Result<T, PgStoreError>; }
impl<T> OrDb<T> for Result<T, sqlx::Error> { … }
```

Места с `?` на результатах `sqlx` становятся `.db()?`. Компилятор показывает каждое. В Kafka — `.client()?`.

### D-3. Версия — 0.4.0, подъём в этой ветке

Изменение несовместимое: меняется тип поля варианта и исчезает `impl From`. Задание CI «API compatibility» (`cargo semver-checks` против опубликованной версии) без подъёма версии сделает master красным. Поэтому версия рабочего пространства поднимается до 0.4.0 здесь же, вместе с зависимостями пакетов на ядро и lock-файлами. Публикации нет: 0.4.0 выйдет, когда будет готова и BL-49 (она ляжет в ту же неопубликованную версию без нового подъёма).

С этого мержа master — это 0.4 в работе, и выпусков 0.3.x из master нет. Срочное исправление 0.3.x — веткой от тега `v0.3.4`.

### D-4. `MIGRATION.md`, раздел «0.3 → 0.4»

- `PgStoreError::Database(e)`: было `sqlx::Error`, стало `DatabaseError`; причина — `e.source()` и `downcast_ref::<sqlx::Error>()`.
- То же для `KafkaSourceError::Client`.
- Код, который делал `PgStoreError::from(sqlx_error)` или `?` из `sqlx::Error` в `PgStoreError`, оборачивает ошибку сам (такого кода у потребителя быть не должно).

### D-5. Тесты

- Модульные: `Display` и `source()` обёрток. Ошибка драйвера достаётся `downcast_ref` с исходным текстом.
- Существующие тесты, сопоставляющие `PgStoreError::Database(_)` и `KafkaSourceError::Client(_)`, остаются: сопоставление по варианту не меняется.
- Тест публичного API (`crates/taskcraft/tests/public_api.rs` — проверить, нет ли аналога в пакетах) — при необходимости.

### Спецификация

- 2.11: «Ошибки пакетов не раскрывают типы драйверов: ошибка драйвера доступна как причина (`source`). Связь с драйвером в API остаётся только у общего пула приложения (`sqlx::PgPool`)».
- Критерии из раздела 16 заявки — №101, №102.
- Таблица статусов: «реализовано (выпуск 0.4)», версия спецификации.

## Risks / Trade-offs

- Пользователь, который сопоставлял поле `Database(sqlx::Error::RowNotFound)`, переходит на `downcast_ref`. Это и есть цель: такая зависимость теперь явная.

## Найдено при реализации

- `cargo semver-checks` (0.51) не замечает ни смены типа поля варианта, ни удаления `impl From<тип драйвера>`: с подъёмом и без он отвечает «no semver update required». Несовместимость обеспечивает подъём до 0.4.0 вручную (D-3), а не инструмент.
- Первая версия обёртки Kafka (`Box<dyn Error + Send + Sync>`) лишала `KafkaSourceError` автотрейтов `UnwindSafe` и `RefUnwindSafe` — это инструмент заметил (`auto_trait_impl_removed`). Обёртка хранит `Box<dyn Error + Send + Sync + UnwindSafe + RefUnwindSafe>`, `Display` и `Error::source` написаны вручную (`thiserror` такой тип в `#[source]` не принимает). `sqlx::Error` этих автотрейтов не имеет, поэтому у PostgreSQL ничего не теряется.
- `#[error(transparent)]` у обёртки перенаправлял бы `source()` на причину ошибки драйвера, а не на неё саму. Обёртка — `#[error("{0}")]` с `#[source]`; вариант `Database` — `transparent`, поэтому `source()` самого `PgStoreError` даёт ошибку драйвера.

## Open Questions

Нет.

## Затронутые примеры

Нет: примеры не разбирают поле ошибок драйвера (проверю поиском по `Database(` и `Client(`).

## Тестирование

- D-5; весь `cargo test --workspace` с сервисами; clippy, fmt, doc, `semver-checks` (ожидается несовместимость), `readme-check`, примеры.
