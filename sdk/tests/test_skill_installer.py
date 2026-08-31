"""Tests for the bundled coding-agent setup skill."""

from __future__ import annotations

from pathlib import Path

from mycelium.__main__ import main

SKILL_FILES = {
    Path("SKILL.md"),
    Path("agents/openai.yaml"),
    Path("references/dynamic-destination-authority.md"),
    Path("references/provider-reconciliation.md"),
    Path("references/tool-classification.md"),
}


def _files(root: Path) -> dict[Path, bytes]:
    return {
        path.relative_to(root): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }


def test_bundled_skill_matches_repository_source() -> None:
    repository_skill = Path(__file__).parents[2] / ".agents/skills/mycelium-setup"
    bundled_skill = Path(__file__).parents[1] / "mycelium/skills/mycelium-setup"

    assert _files(bundled_skill) == _files(repository_skill)
    assert set(_files(bundled_skill)) == SKILL_FILES


def test_skills_install_is_offline_and_idempotent(tmp_path: Path, capsys) -> None:
    catalog = tmp_path / "catalog"

    assert main(["skills", "install", "--target", str(catalog)]) == 0
    destination = catalog / "mycelium-setup"
    assert set(_files(destination)) == SKILL_FILES
    assert "Installed mycelium-setup skill" in capsys.readouterr().out

    assert main(["skills", "install", "--target", str(catalog)]) == 0
    assert "already current" in capsys.readouterr().out


def test_skills_install_refuses_different_existing_skill_without_force(
    tmp_path: Path,
    capsys,
) -> None:
    catalog = tmp_path / "catalog"
    destination = catalog / "mycelium-setup"
    destination.mkdir(parents=True)
    skill_file = destination / "SKILL.md"
    skill_file.write_text("locally customized", encoding="utf-8")

    assert main(["skills", "install", "--target", str(catalog)]) == 1
    assert skill_file.read_text(encoding="utf-8") == "locally customized"
    assert "use --force to replace it" in capsys.readouterr().err


def test_skills_install_force_replaces_different_existing_skill(
    tmp_path: Path,
    capsys,
) -> None:
    catalog = tmp_path / "catalog"
    destination = catalog / "mycelium-setup"
    destination.mkdir(parents=True)
    (destination / "SKILL.md").write_text("locally customized", encoding="utf-8")
    (destination / "extra.txt").write_text("stale", encoding="utf-8")

    assert main(["skills", "install", "--target", str(catalog), "--force"]) == 0
    assert set(_files(destination)) == SKILL_FILES
    assert "Installed mycelium-setup skill" in capsys.readouterr().out


def test_skills_install_uses_project_catalog_by_default(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    assert main(["skills", "install"]) == 0
    assert (tmp_path / ".agents/skills/mycelium-setup/SKILL.md").is_file()
