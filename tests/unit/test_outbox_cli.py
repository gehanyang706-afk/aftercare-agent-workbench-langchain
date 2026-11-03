"""CLI flags are validated together before launching any server or opening PostgreSQL."""

from aftercare.cli import main

TOKEN = "cli-local-warehouse-test-token-123456789"


def test_command_line_cannot_enable_both_delivery_authorities(monkeypatch, capsys):
    monkeypatch.setenv("WAREHOUSE_TOKEN", TOKEN)
    assert main(["serve", "--async-warehouse", "--simulate-warehouse"]) == 2
    output = capsys.readouterr().out
    assert '"code": "CONFIG_INVALID"' in output
    assert TOKEN not in output


def test_async_cli_flag_does_not_bypass_missing_credentials(monkeypatch, capsys):
    monkeypatch.setenv("WAREHOUSE_TOKEN", "")
    assert main(["serve", "--async-warehouse"]) == 2
    assert "CONFIG_INVALID" in capsys.readouterr().out


def test_mock_warehouse_does_not_need_product_settings_or_database(monkeypatch, capsys, tmp_path):
    monkeypatch.setenv("DATABASE_URL", "invalid-product-database-url")
    monkeypatch.delenv("MOCK_WAREHOUSE_TOKEN", raising=False)
    assert main(["mock-warehouse", "--state-path", str(tmp_path / "outside.sqlite3")]) == 2
    assert "MOCK_WAREHOUSE_CONFIG_INVALID" in capsys.readouterr().out
    assert not (tmp_path / "outside.sqlite3").exists()
