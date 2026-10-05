# Tasks: Каркас репозитория и инфраструктура качества

> **Change**: [change.md](change.md)
>
> **Design**: [design.md](design.md)
>
> **Статус**: В реализации
>
> **Автор**: Андрей Серохвостов
>
> **Дата**: 2026-10-05

---

## 1. Рабочее пространство

- [x] 1.1 Корневой `Cargo.toml`: виртуальное рабочее пространство, `members = ["crates/*"]`, `resolver = "3"`, `[workspace.package]` (версия 0.0.0, редакция 2024, MSRV 1.99, MIT, авторы, репозиторий)
- [x] 1.2 `[workspace.lints.rust]` и `[workspace.lints.clippy]` по design.md; без `type_complexity = "allow"`
- [x] 1.3 `.clippy.toml` с `msrv = "1.99"`
- [x] 1.4 `crates/taskcraft/Cargo.toml`: метаданные из рабочего пространства, `description`, `keywords`, `categories`, `readme`, `lints.workspace = true`
- [x] 1.5 `crates/taskcraft/src/lib.rs`: документация пакета — назначение, статус «резерв имени», ссылка на спецификацию
- [x] 1.6 `LICENSE` (MIT, sebkd, 2026), `README.md`, `/target/` в `.gitignore`
- [x] 1.7 Локально: `cargo fmt --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo test --workspace`, `cargo doc --no-deps` с `-D warnings`, `cargo publish --dry-run -p taskcraft` проходят; `Cargo.lock` создан

## 2. Проверка зависимостей

- [x] 2.1 `deny.toml`: advisories, licenses (список из design.md), bans (`multiple-versions = "warn"`), sources (только crates.io)
- [x] 2.2 `supply-chain/` через `cargo vet init`; импорт аудитов Mozilla, Google, Bytecode Alliance, ZcashFoundation

## 3. CI

- [x] 3.1 `.github/workflows/ci.yml`: push в любую ветку и `workflow_dispatch`; задание `check` (fmt, clippy, test, doc, publish --dry-run) и задание `msrv` (1.99, `cargo check --workspace --all-features`)
- [x] 3.2 В `check` — шаг, проверяющий `lints.workspace = true` в каждом `crates/*/Cargo.toml`
- [x] 3.3 `.github/workflows/supply-chain.yml`: cargo-deny и `cargo vet --locked`; push при изменении манифестов, lock-файла, `deny.toml`, `supply-chain/**`; ежедневное расписание
- [x] 3.4 Push ветки, проверить на GitHub: оба workflow зелёные

## 4. Финализация

- [x] 4.1 Проверки отрицательных сценариев в отдельных временных ветках: `unsafe` → CI красный; зависимость с GPL → cargo-deny красный; ветки удалены
- [x] 4.2 **Вручную, владелец репозитория:** `cargo publish -p taskcraft` своим токеном crates.io; проверить, что `taskcraft 0.0.0` виден на crates.io и ведёт на GitHub
- [x] 4.3 Отметить строку `bootstrap-workspace` в таблице «Статус реализации» спецификации при `/opsx-apply`

---

## Чеклист верификации

### Основной сценарий

- [ ] Push в любую ветку зеркала → CI выполняет сборку, тесты, линтер без предупреждений, проверку форматирования (критерий заявки №1)
- [ ] CI выполняет проверочную публикацию каждого пакета → проверка проходит, версии внутренних зависимостей совпадают с версиями пакетов (№4)
- [ ] Каркас влит → `taskcraft` занят на crates.io версией-заглушкой; в манифестах MIT и `https://github.com/Sebkd/taskcraft` (№5)
- [ ] Ядро собрано без возможностей → в дереве зависимостей нет клиентов Kafka, драйверов СУБД, библиотеки metrics (спецификация, критерий №33)

### Альтернативные сценарии

- [ ] Задание `msrv` на 1.99 зелёное

### Ошибочные сценарии

- [ ] Зависимость с уязвимостью или несовместимой лицензией → CI падает на проверке зависимостей (№2)
- [ ] Небезопасный код в любом пакете → сборка падает (№3)
- [ ] Пакет без `lints.workspace = true` → CI падает

### Нефункциональные требования

- [ ] `unsafe_code = "forbid"` действует во всех пакетах (спецификация 4.1 п. 1, 2.3.23 п. 3)
- [ ] Ежедневный запуск supply-chain настроен (спецификация 4.1 п. 5)

---

## Статус выполнения

| Задача | Статус | Комментарий |
|--------|--------|-------------|
| 1.1–1.7 | — | |
| 2.1–2.2 | — | |
| 3.1–3.4 | — | |
| 4.1 | — | |
| 4.2 | — | Ручной шаг владельца |
| 4.3 | — | При `/opsx-apply` |

**✓** — выполнено  
**○** — в процессе  
**—** — не начато

---

## Лог изменений

| Дата | Автор | Изменение |
|------|-------|----------|
| 2026-10-05 | Андрей Серохвостов | Design и план задач |
