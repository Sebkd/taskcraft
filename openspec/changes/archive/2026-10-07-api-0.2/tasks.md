# Tasks: Публичный API 0.2: ручки, трейты источника, модули

> **Change**: [change.md](change.md)
>
> **Design**: [design.md](design.md)
>
> **Статус**: В реализации
>
> **Автор**: Андрей Серохвостов
>
> **Дата**: 2026-10-07

---

## 1. Контракт источника (D-1, D-4)

- [x] 1.1 `Source`, `PushSource`, `TaskStore`, `StoreMessage`; убрать `with_push`, `accepts_push`, `PushError::Unsupported`
- [x] 1.2 Внутренний `Backend` и обёртки `Consumed`, `Pushed`, `Stored`; воркер на `B: Backend`
- [x] 1.3 `InMemorySource` и `FaultySource` — `PushSource`; `JsonCodec` для `StoreMessage`

## 2. Очередь, ручки, монитор (D-2, D-3, D-5)

- [x] 2.1 Конструкторы `builder`, `consumer`, `on_store`; фабрика кодека в `build()`
- [x] 2.2 `QueueHandle<Args>`, `ConsumerHandle<Args>` со стёртыми источником и кодеком; `HandleKind`
- [x] 2.3 `register` возвращает ручку; `Queue::handle` убран; убрать `PushTaskError::Unsupported`
- [x] 2.4 Наблюдатели раздаются очередям в `run`

## 3. Модули (D-6)

- [x] 3.1 Публичные модули `source`, `codec`, `handler`, `observe`, `runnable`, `error`, `prelude`; корень по таблице D-6
- [x] 3.2 Документация пакета (lib.rs) и `compile_fail` doc-тесты критериев №3, №4

## 4. Пакеты

- [x] 4.1 `taskcraft-kafka`: `Queue::consumer`, документация, README пакета
- [x] 4.2 `taskcraft-postgres`: `TaskStore`, `StoreMessage`, `Queue::on_store`, документация, README пакета

## 5. Тесты, примеры, README

- [x] 5.1 Тесты ядра и пакетов на 0.2; новый тест критерия №2
- [x] 5.2 Примеры E-1…E-15, `examples/axum-service`, бенчмарки
- [x] 5.3 README: все разделы на 0.2, ссылка на руководство
- [x] 5.4 `MIGRATION.md`: 0.1 → 0.2 по Q-1…Q-6

## 6. Версия, спецификация, финализация

- [x] 6.1 Версия рабочей области 0.2.0
- [x] 6.2 Спецификация: 2.5, 2.3.22, 2.11.2, таблица статусов, версия 0.28
- [x] 6.3 `cargo semver-checks` против 0.1.1 локально; бенчмарки до и после
- [x] 6.4 Локально: fmt, clippy, test (Kafka, PostgreSQL), doc, MSRV, deny, vet, примеры; CI зелёный

---

## Лог изменений

| Дата | Автор | Изменение |
|------|-------|----------|
| 2026-10-07 | Андрей Серохвостов | Design и план задач |
