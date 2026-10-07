# Tasks: Отложенная постановка: выполнить задачу в заданный момент

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

## 1. Ядро

- [x] 1.1 `Task::with_deliver_at`, `with_delay`, `deliver_at`
- [x] 1.2 `PushSource::push_at`, `TaskStore::push_at`, `PushError::DelayUnsupported`, `PushTaskError::DelayUnsupported`
- [x] 1.3 `Backend::push_at`, `Port::push` с моментом, проверка возможности в ручке
- [x] 1.4 In-memory источник: `push_at`

## 2. Хранилище

- [x] 2.1 `PgSource::push_at`

## 3. Тесты

- [x] 3.1 Ядро: критерии №1, №3; отмена и ёмкость
- [x] 3.2 Хранилище: критерии №2, №4

## 4. Документация и спецификация

- [x] 4.1 README «Delayed push»; пример E-13; документация методов
- [x] 4.2 Спецификация: 2.7.1, 2.1.2.1, 2.11.2, 2.6, критерии, таблица статусов, версия 0.29

## 5. Финализация

- [x] 5.1 Локально: fmt, clippy, test (Kafka, PostgreSQL), doc, readme-check, примеры; `cargo semver-checks` (минорное изменение); CI зелёный

---

## Лог изменений

| Дата | Автор | Изменение |
|------|-------|----------|
| 2026-10-07 | Андрей Серохвостов | Design и план задач |
