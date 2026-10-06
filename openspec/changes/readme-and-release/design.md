# Design: README, документация пакетов и выпуск 0.1.0

> **Change**: [change.md](change.md)
>
> **Статус**: Готово к реализации
>
> **Автор**: Андрей Серохвостов
>
> **Дата**: 2026-10-06

---

## Context

README репозитория — страница `taskcraft` на crates.io (`readme = "../../README.md"`); у пакетов Kafka и хранилища свои README. Вводная `lib.rs` — страница docs.rs. Всё это описывает версию «в работе». Пакеты на версии 0.0.0; пакеты Kafka и хранилища помечены `publish = false`.

Решения, согласованные с владельцем проекта: код в README показывает возможности; каждый фрагмент компилируется в CI (вариант А — doc-тесты в существующем `cargo test`).

---

## Goals / Non-Goals

**Goals:** README трёх пакетов с проверяемым кодом, благодарности apalis, вводные docs.rs, `CHANGELOG.md`, переход на 0.1.0, готовность к публикации.

**Non-Goals:** публикация до команды владельца; изменения поведения.

---

## Decisions

### Проверка кода README — отдельный внутренний пакет `crates/readme-check`

**Решение**:
- Пакет `readme-check` (`publish = false`), зависимости:
  - `taskcraft` со всеми возможностями;
  - `taskcraft-kafka`, `taskcraft-postgres`;
  - statecraft-fsm, tokio, serde.
- Его `lib.rs` подключает три README как документацию скрытых модулей (`#[doc = include_str!(...)]`). `cargo test --workspace` в CI выполняет их фрагменты как doc-тесты.
- Фрагменты, которым нужны брокер или база, — ` ```rust,no_run `: компилируются, но не запускаются.

**Обоснование**: в README репозитория есть фрагменты всех трёх пакетов. Doc-тест пакета `taskcraft` не видит пакеты Kafka и хранилища, а dev-зависимость на них создала бы цикл и две копии типов `taskcraft`. Отдельный пакет видит всё и ничего не публикует.

**Альтернативы**:
- Подключить README в `lib.rs` самого `taskcraft` — отклонено: фрагменты Kafka и хранилища там не компилируются.
- Фрагменты Kafka и хранилища без проверки (`ignore`) — отклонено: противоречит критерию заявки №1.

### Страницы docs.rs — свои вводные, не копия README

**Решение**: вводная `lib.rs` каждого пакета:
- что это и для чего;
- одна минимальная программа как doc-тест;
- карта модулей и терминов (существующий список «Terms»);
- ссылки на README, каталог примеров, спецификацию.

«Work in progress» убирается.

**Обоснование**: README репозитория содержит код других пакетов и не может быть doc-тестом `taskcraft`; на docs.rs нужна навигация по API, а не витрина.

### Структура README репозитория

**Решение**:
1. Название, одна фраза, значки: CI, crates.io, docs.rs, лицензия, MSRV 1.99.
2. «Why taskcraft» — 4–5 пунктов: что решает и чем отличается (опрос с тремя ответами, исходы как данные, прерываемые паузы, без паник).
3. «Quick start»: `[dependencies]` и минимальная очередь (E-1).
4. «A tour» — по фрагменту в 10–25 строк и ссылке на полный пример:
   1. Outcomes as data, retries (E-2).
   2. Long tasks: reject, interruptible pause (E-3).
   3. Resource pools, status and cancel by id (E-4).
   4. Idempotent push (E-5).
   5. Graceful shutdown and its report (E-6).
   6. Crash recovery with a recovery hook (E-15, часть).
   7. A state machine as a task — statecraft-fsm (E-15).
   8. Kafka source (E-12, `no_run`).
   9. PostgreSQL task store with leases (E-14, `no_run`).
   10. Observability: log format, metrics, own observer (E-10).
   11. Testing with the harness (`testing`).
5. «Packages and features» — таблица пакетов и возможностей сборки.
6. «Guarantees»: at-least-once, идемпотентный приём, без паник, журнал без аргументов.
7. «Environment variables»:
   - библиотека сама окружение не читает — приложение передаёт настройки в построители;
   - `RUST_LOG` (фильтр `tracing-subscriber` приложения): `taskcraft=debug`, `taskcraft=off`;
   - таблица переменных тестов и примеров: `TASKCRAFT_KAFKA_BROKERS`, `TASKCRAFT_POSTGRES_URL`, `TASKCRAFT_REQUIRE_SERVICES`.
8. «Examples» → каталог; «Specification» → спецификация (на русском).
9. «Requirements», «Acknowledgements», «License».

### Благодарности

**Решение** — раздел «Acknowledgements», по образцу statecraft-fsm:

> taskcraft was inspired by [apalis](https://github.com/geofmureithi/apalis) (MIT-licensed): the small polling source interface, tasks as tower services, and features as layers. taskcraft is an independent implementation with a different design — a three-answer poll, outcomes as data, interruptible waits, idempotent intake, a task store with leases — and does not use apalis code; credit is due to that project for the ideas. Its handler pattern also borrows from axum's extractors.

Также — statecraft-fsm как родственный проект.

### Выпуск 0.1.0

**Решение**:
- `workspace.package.version = "0.1.0"`.
- Пакеты Kafka и хранилища зависят от `taskcraft = { path, version = "0.1.0" }`; `publish = false` снимается.
- `CHANGELOG.md` в формате Keep a Changelog: раздел 0.1.0 по пакетам.
- `cargo publish --dry-run` пакетов Kafka и хранилища проверяет сборку против `taskcraft` 0.1.0 с crates.io и до его публикации не пройдёт. Поэтому локально проверяются `cargo package --list` (README входит в архив) и `--dry-run` для `taskcraft`. В CI шаг «Publish (dry run)» временно пропускает пакеты, чья версия `taskcraft` ещё не опубликована; после публикации `taskcraft` 0.1.0 шаг проверяет все три.
- Публикация — по команде владельца: `taskcraft`, затем `taskcraft-kafka` и `taskcraft-postgres`.

### Затронутые примеры

Примеры не меняются. README ссылается на них и повторяет их суть.

---

## Risks / Trade-offs

| Риск | Последствия | Mitigation |
|------|------------|------------|
| Фрагменты README разойдутся с примерами каталога | Два места с похожим кодом | Фрагменты — выжимки; полный пример — по ссылке; оба проверяются компилятором |
| Ссылки в README на crates.io | Относительные пути ведут на GitHub только при поле `repository` на github.com | Поле задано; ссылки на файлы репозитория — абсолютные |
| Шаг CI «Publish (dry run)» до публикации `taskcraft` 0.1.0 | Пакеты Kafka и хранилища не проходят dry-run | Временный пропуск с понятным сообщением; снимается после публикации |

---

## Open Questions

Нет.

---

## Тестирование

| Критерий заявки | Проверка |
|-----------------|----------|
| №1 | `cargo test --workspace` выполняет doc-тесты `readme-check` |
| №2 | Проверка ссылок: абсолютные URL; относительные — только на файлы, которые есть в репозитории |
| №3 | Раздел «Acknowledgements» |
| №4 | `cargo package --list` трёх пакетов содержит README; `cargo publish --dry-run -p taskcraft` |
| №5 | `cargo doc` — вводные трёх пакетов |
| №6 | Публикация по команде |
