# Tasks: Задачи с ошибкой: хук и повторный запуск

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

## 1. Хук

- [x] 1.1 `FailedTask<Args>`, `QueueBuilder::failed_task`
- [x] 1.2 Воркер: вызов хука до `finish` при «Ошибке» и «Панике», `catch_unwind`, журнал; `Delivered<S>` в `execute`

## 2. Повторный запуск

- [x] 2.1 `TaskStore::requeue`, `Requeue`, `Backend::requeue`, `Port::requeue`
- [x] 2.2 `QueueHandle::requeue`, `RequeueError`
- [x] 2.3 `PgSource::requeue`

## 3. Тесты

- [x] 3.1 Ядро: критерии №1, №2; in-memory → `Unknown`
- [x] 3.2 Хранилище: критерии №3, №4

## 4. Документация и спецификация

- [x] 4.1 README «Failed tasks»; примеры E-2, E-13; документация методов
- [x] 4.2 Спецификация: 2.3.26, 2.4.1.14, глоссарий, 2.5, 2.9, 2.11, критерии, таблица статусов, версия 0.30

## 5. Финализация

- [x] 5.1 Локально: fmt, clippy, test (Kafka, PostgreSQL), doc, readme-check, примеры; `cargo semver-checks` против 0.2.0; CI зелёный

---

## Лог изменений

| Дата | Автор | Изменение |
|------|-------|----------|
| 2026-10-07 | Андрей Серохвостов | Design и план задач |
