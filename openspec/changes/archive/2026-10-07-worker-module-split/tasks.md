# Tasks: Разделение модуля воркера

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

## 1. Перенос

- [x] 1.1 `worker.rs` → `worker/mod.rs`; вынести `pools.rs`, `notices.rs`, `drain.rs` без правки тел
- [x] 1.2 Вынести `execute.rs` без правки тел

## 2. Разрезание длинных функций

- [x] 2.1 `intake.rs`: структура `Intake` с методами `accept_recovered`, `run`, `gate`, `on_empty`, `on_message`, `on_poison`
- [x] 2.2 `execute`: шаги `acquire`, `run_once`, `wait_retry`
- [x] 2.3 Проверка: ни одной функции в `worker/` длиннее 100 строк

## 3. Спецификация и финализация

- [x] 3.1 Таблица статусов, версия 0.27
- [x] 3.2 Локально: fmt, clippy, test (и с Kafka, PostgreSQL), doc, MSRV, примеры; CI зелёный

---

## Лог изменений

| Дата | Автор | Изменение |
|------|-------|----------|
| 2026-10-07 | Андрей Серохвостов | Design и план задач |
