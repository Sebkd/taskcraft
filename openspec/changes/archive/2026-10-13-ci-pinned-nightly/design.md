# Design: CI: закреплённый nightly для фаззинга

> **Change**: [change.md](change.md)
>
> **Статус**: Реализовано
>
> **Автор**: Андрей Серохвостов
>
> **Дата**: 2026-10-13

---

## Context

Задание «Fuzz (short)» (`.github/workflows/ci.yml`) ставит текущий nightly (`dtolnay/rust-toolchain@nightly`). Пакет `fuzz/` вне рабочего пространства; в комментарии `fuzz/Cargo.toml` — ручной запуск `cargo +nightly fuzz run …`. Ни CI, ни ручной запуск версию не фиксируют: сломанный nightly роняет CI без изменений в коде, а ручной прогон перед выпуском идёт на другом компиляторе, чем CI.

Локальный nightly — `rustc 1.101.0-nightly (8d1a76430 2026-10-06)`, то есть `nightly-2026-10-07`. Цели фаззинга на нём собираются (`cargo +nightly-2026-10-07 fuzz build`).

## Goals / Non-Goals

**Goals**
- Одна запись версии nightly для фаззинга, общая для CI и ручного запуска.

**Non-Goals**
- Закрепление stable (задание «Check», «Coverage» и др.).
- Автоматическое обновление даты.

## Decisions

### D-1. `fuzz/rust-toolchain.toml` — единственный источник версии

```toml
[toolchain]
channel = "nightly-2026-10-07"
profile = "minimal"
```

rustup берёт этот файл для любой команды `cargo`, запущенной из `fuzz/`: ручной запуск — `cargo fuzz run …` без `+nightly`.

### D-2. CI ставит ту же версию из файла

Шаг «Install Rust (nightly)» заменяется на шаг, который читает файл:

```yaml
- name: Install Rust (pinned nightly)
  working-directory: fuzz
  run: rustup toolchain install   # reads fuzz/rust-toolchain.toml
```

`rustup toolchain install` без аргумента ставит канал из `rust-toolchain.toml` текущего каталога (rustup ≥ 1.28). Действие `dtolnay/rust-toolchain` для этого задания больше не нужно. Ключ кэша `rust-cache` зависит от версии компилятора, поэтому смена даты кэш инвалидирует сама.

### D-3. Обновление — вручную

Дату меняет отдельная правка `fuzz/rust-toolchain.toml`; CI этой правки и есть её проверка. Порядок записан в комментарии файла.

### Спецификация

- 4.3 п. 8, пункт о фаззинге: «на nightly, закреплённом датой в `fuzz/rust-toolchain.toml`; дата обновляется вручную».
- Критерий из раздела 16 заявки — №90.
- Таблица статусов, версия спецификации.

## Risks / Trade-offs

- Старый nightly со временем отстаёт от MSRV-кода, которому нужны новые возможности компилятора. Это видно сразу — сборка падает — и лечится обновлением даты.

## Open Questions

Нет.

## Затронутые примеры

Нет.

## Тестирование

- Локально: `cargo fuzz build` и минутный прогон одной цели из `fuzz/` без `+nightly`.
- CI: «Fuzz (short)» зелёный на закреплённой версии (аннотация с версией компилятора).
