"""``profile: production`` tightens memory-storage and run-id policies."""

from __future__ import annotations

import warnings

import pytest

from mycelium import (
    PROFILE_DEVELOPMENT,
    PROFILE_PRODUCTION,
    ConfigError,
    MissingRunIdentityError,
    SideEffectClass,
    TransitionScope,
    execution_scope,
    get_ledger,
    load_config_from_string,
)
from mycelium.loop_guard import reset_missing_run_identity_warnings


@pytest.fixture(autouse=True)
def _reset_identity_warnings() -> None:
    reset_missing_run_identity_warnings()
    yield
    reset_missing_run_identity_warnings()


def _prod_sqlite(tmp_path, extra: str = "") -> str:
    return f"""
profile: production
transition:
  agent_id: prod-agent
  policy_version: "2026.08.1"
action_ledger:
  storage: sqlite
  path: {tmp_path / "ledger.db"}
  tools: [charge]
tools:
  charge:
    side_effect_class: non_idempotent_mutate
{extra}
"""


def test_omitted_profile_is_development() -> None:
    cfg = load_config_from_string("tools: {}")
    assert cfg.profile == PROFILE_DEVELOPMENT


def test_explicit_development_profile_loads() -> None:
    cfg = load_config_from_string("profile: development\ntools: {}")
    assert cfg.profile == PROFILE_DEVELOPMENT


def test_invalid_profile_raises() -> None:
    with pytest.raises(
        ConfigError,
        match=r"'profile' must be 'development' or 'production', got 'staging'",
    ):
        load_config_from_string("profile: staging\ntools: {}")


def test_production_side_effecting_memory_raises() -> None:
    with pytest.raises(ConfigError, match="charge.*memory storage") as exc:
        load_config_from_string(
            """
profile: production
transition:
  agent_id: a
  policy_version: p
action_ledger:
  storage: memory
  tools: [charge]
tools:
  charge:
    side_effect_class: non_idempotent_mutate
"""
        )
    assert "memory_storage_policy" in str(exc.value)


def test_production_read_may_use_memory() -> None:
    cfg = load_config_from_string(
        """
profile: production
transition:
  agent_id: a
  policy_version: p
action_ledger:
  storage: memory
  tools: [search]
tools:
  search:
    side_effect_class: read
"""
    )
    assert cfg.profile == PROFILE_PRODUCTION
    assert cfg.tools["search"].side_effect_class == SideEffectClass.READ


@pytest.mark.parametrize(
    "storage_yaml",
    [
        "storage: sqlite\n  path: {path}",
        "storage: redis\n  url: redis://localhost:6379/0",
        "storage: postgres\n  dsn_env: MYCELIUM_POSTGRES_DSN",
    ],
)
def test_production_durable_storage_loads(tmp_path, storage_yaml: str) -> None:
    block = storage_yaml.format(path=tmp_path / "ledger.db")
    cfg = load_config_from_string(
        f"""
profile: production
transition:
  agent_id: a
  policy_version: p
action_ledger:
  {block}
  tools: [charge]
tools:
  charge:
    side_effect_class: non_idempotent_mutate
"""
    )
    assert cfg.profile == PROFILE_PRODUCTION


def test_production_enabled_guard_missing_run_id_fails_before_ledger_and_tool(
    tmp_path,
) -> None:
    cfg = load_config_from_string(
        _prod_sqlite(
            tmp_path,
            extra="""
loop_guard:
  storage: memory
scope_guard:
  storage: memory
  allowed_tools: [charge]
""",
        )
    )
    calls = {"n": 0}

    def charge() -> str:
        calls["n"] += 1
        return "paid"

    wrapped = cfg.apply_tool("charge", charge)
    with pytest.raises(MissingRunIdentityError) as exc:
        wrapped(tool_call_id="c0")
    assert calls["n"] == 0
    assert exc.value.guard == "ScopeGuard"
    ledger = get_ledger(wrapped)
    assert ledger is not None
    assert ledger._storage.list_all() == []


def test_production_thread_only_identity_fails(tmp_path) -> None:
    cfg = load_config_from_string(
        _prod_sqlite(
            tmp_path,
            extra="""
loop_guard:
  storage: memory
""",
        )
    )
    calls = {"n": 0}

    def charge() -> str:
        calls["n"] += 1
        return "paid"

    wrapped = cfg.apply_tool("charge", charge)
    with execution_scope(TransitionScope(thread_id="thread-1", run_id="")):
        with pytest.raises(MissingRunIdentityError):
            wrapped(tool_call_id="c0")
    assert calls["n"] == 0


def test_production_stable_run_id_succeeds(tmp_path) -> None:
    cfg = load_config_from_string(
        _prod_sqlite(
            tmp_path,
            extra="""
loop_guard:
  storage: memory
  consecutive_soft:
    non_idempotent_mutate: 5
""",
        )
    )
    calls = {"n": 0}

    def charge() -> str:
        calls["n"] += 1
        return "paid"

    wrapped = cfg.apply_tool("charge", charge)
    with execution_scope(TransitionScope(thread_id="t1", run_id="run-123")):
        assert wrapped(tool_call_id="c0") == "paid"
        assert wrapped(tool_call_id="c0") == "paid"
    assert calls["n"] == 1


def test_production_disabled_guards_do_not_require_run_id(tmp_path) -> None:
    cfg = load_config_from_string(_prod_sqlite(tmp_path))
    assert cfg.build_loop_guard() is None
    assert cfg.build_scope_guard() is None
    calls = {"n": 0}

    def charge() -> str:
        calls["n"] += 1
        return "ok"

    wrapped = cfg.apply_tool("charge", charge)
    assert wrapped(tool_call_id="c0") == "ok"
    assert wrapped(tool_call_id="c0") == "ok"
    assert calls["n"] == 1


def test_development_retains_memory_storage_warning() -> None:
    yaml_text = """
profile: development
transition:
  agent_id: a
  policy_version: p
action_ledger:
  storage: memory
  tools: [charge]
tools:
  charge:
    side_effect_class: non_idempotent_mutate
"""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", UserWarning)
        cfg = load_config_from_string(yaml_text)
    assert cfg.profile == PROFILE_DEVELOPMENT
    matching = [
        w
        for w in caught
        if issubclass(w.category, UserWarning) and "memory storage" in str(w.message)
    ]
    assert len(matching) == 1


def test_development_missing_run_id_still_warns_and_skips() -> None:
    cfg = load_config_from_string(
        """
profile: development
loop_guard:
  storage: memory
tools:
  search:
    side_effect_class: read
"""
    )
    calls = {"n": 0}

    def search(**_kwargs: object) -> str:
        calls["n"] += 1
        return "ok"

    wrapped = cfg.apply_tool("search", search)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        for i in range(4):
            wrapped(tool_call_id=f"c{i}")
    assert calls["n"] == 4
    skipped = [w for w in caught if "skipped:" in str(w.message)]
    assert len(skipped) == 1


def test_production_rejects_weaker_memory_storage_policy() -> None:
    with pytest.raises(
        ConfigError,
        match=(
            r"profile is 'production' but "
            r"'action_ledger.memory_storage_policy' is 'warn'"
        ),
    ):
        load_config_from_string(
            """
profile: production
action_ledger:
  storage: sqlite
  path: ./ledger.db
  memory_storage_policy: warn
  tools: [charge]
tools:
  charge:
    side_effect_class: non_idempotent_mutate
"""
        )


def test_production_rejects_weaker_missing_run_id_policy() -> None:
    with pytest.raises(
        ConfigError,
        match=(
            r"profile is 'production' but "
            r"'loop_guard.missing_run_id_policy' is 'warn'"
        ),
    ):
        load_config_from_string(
            """
profile: production
loop_guard:
  storage: memory
  missing_run_id_policy: warn
tools:
  search:
    side_effect_class: read
"""
        )
    with pytest.raises(
        ConfigError,
        match="'scope_guard.missing_run_id_policy' is 'warn'",
    ):
        load_config_from_string(
            """
profile: production
scope_guard:
  storage: memory
  allowed_tools: [search]
  missing_run_id_policy: warn
tools:
  search:
    side_effect_class: read
"""
        )


def test_production_accepts_explicit_error_policies(tmp_path) -> None:
    cfg = load_config_from_string(
        f"""
profile: production
action_ledger:
  storage: sqlite
  path: {tmp_path / "ledger.db"}
  memory_storage_policy: error
  tools: [charge]
loop_guard:
  storage: memory
  missing_run_id_policy: error
tools:
  charge:
    side_effect_class: non_idempotent_mutate
"""
    )
    loop = cfg.build_loop_guard()
    assert loop is not None
    assert loop.missing_run_id_policy == "error"


def test_production_applies_error_when_policies_omitted(tmp_path) -> None:
    cfg = load_config_from_string(
        _prod_sqlite(
            tmp_path,
            extra="""
loop_guard:
  storage: memory
scope_guard:
  storage: memory
  allowed_tools: [charge]
""",
        )
    )
    loop = cfg.build_loop_guard()
    scope = cfg.build_scope_guard()
    assert loop is not None
    assert scope is not None
    assert loop.missing_run_id_policy == "error"
    assert scope.missing_run_id_policy == "error"


def test_existing_configs_without_profile_still_load() -> None:
    cfg = load_config_from_string(
        """
tools:
  search:
    side_effect_class: read
"""
    )
    assert cfg.profile == PROFILE_DEVELOPMENT
    assert cfg.build_loop_guard() is None
