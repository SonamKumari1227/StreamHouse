"""Unit tests for the `python -m generator` CLI.

Argument parsing and validation only — no database, no broker. The modes themselves are
exercised end to end by the integration suites for each component.
"""

from __future__ import annotations

import pytest

from generator.__main__ import build_parser, main
from generator.config import DEFAULT_RNG_SEED
from generator.state_machine import DEFAULT_SPEED

# --------------------------------------------------------------------------- defaults


def test_bare_invocation_has_working_defaults() -> None:
    args = build_parser().parse_args([])
    assert args.mode == "both"
    assert args.speed == DEFAULT_SPEED
    assert args.seed == DEFAULT_RNG_SEED
    assert args.chaos == ""
    assert args.reset is False
    assert args.orders is None
    assert args.duration is None


def test_the_documented_invocation_parses() -> None:
    """The exact line from the CLI docstring."""
    args = build_parser().parse_args(
        ["--mode", "both", "--chaos", "duplicates,out-of-order", "--speed", "20.0"]
    )
    assert args.mode == "both"
    assert args.chaos == "duplicates,out-of-order"
    assert args.speed == 20.0


@pytest.mark.parametrize("mode", ["seed", "oltp", "gps", "both"])
def test_every_mode_is_accepted(mode: str) -> None:
    assert build_parser().parse_args(["--mode", mode]).mode == mode


def test_an_unknown_mode_is_rejected() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--mode", "nonsense"])


def test_stopping_conditions_parse() -> None:
    args = build_parser().parse_args(["--orders", "500", "--duration", "90"])
    assert args.orders == 500
    assert args.duration == 90.0


def test_connection_defaults_point_at_the_local_stack() -> None:
    args = build_parser().parse_args([])
    assert "dbname=streamhouse" in args.dsn
    assert args.bootstrap == "localhost:9092"
    assert args.topic == "gps.pings"


def test_help_lists_the_available_chaos_scenarios() -> None:
    help_text = build_parser().format_help()
    for name in ("duplicates", "out-of-order", "late-events"):
        assert name in help_text


# --------------------------------------------------------------------------- validation


def test_an_unknown_chaos_scenario_exits_with_a_usage_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["--chaos", "nonsense"]) == 2
    assert "unknown chaos scenario" in capsys.readouterr().err


def test_an_unbuilt_chaos_scenario_names_its_phase(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["--chaos", "late-events"]) == 2
    assert "Phase 3" in capsys.readouterr().err


@pytest.mark.parametrize("speed", ["0", "-5"])
def test_a_non_positive_speed_is_rejected(speed: str, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--speed", speed]) == 2
    assert "--speed must be > 0" in capsys.readouterr().err


def test_reset_is_refused_outside_seed_mode(capsys: pytest.CaptureFixture[str]) -> None:
    """--reset deletes every row; it must not fire from a mode that only reads."""
    assert main(["--mode", "oltp", "--reset"]) == 2
    err = capsys.readouterr().err
    assert "destructive" in err
    assert "--mode seed" in err


def test_reset_is_allowed_in_seed_mode() -> None:
    args = build_parser().parse_args(["--mode", "seed", "--reset"])
    assert args.reset is True


def test_validation_happens_before_any_connection_is_attempted() -> None:
    """A bad flag must fail fast, not after a connection timeout."""
    assert main(["--chaos", "nonsense", "--dsn", "host=192.0.2.1 connect_timeout=30"]) == 2


# --------------------------------------------------------------------------- seed volumes


def test_seed_volumes_can_be_overridden() -> None:
    args = build_parser().parse_args(
        ["--mode", "seed", "--customers", "10", "--restaurants", "3", "--riders", "4"]
    )
    assert (args.customers, args.restaurants, args.riders) == (10, 3, 4)


def test_seed_volume_defaults_match_the_config() -> None:
    from generator.config import SeedVolumes

    args = build_parser().parse_args([])
    defaults = SeedVolumes()
    assert args.customers == defaults.customers
    assert args.restaurants == defaults.restaurants
    assert args.riders == defaults.riders


# --------------------------------------------------------------------------- Stopper


def test_stopper_starts_unstopped() -> None:
    from generator.__main__ import Stopper

    assert Stopper().stopped is False


def test_the_first_signal_requests_a_stop_rather_than_raising(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Ctrl-C should end the run cleanly so the report still prints."""
    import signal

    from generator.__main__ import Stopper

    stopper = Stopper()
    previous = signal.getsignal(signal.SIGINT)
    try:
        stopper.install()
        handler = signal.getsignal(signal.SIGINT)
        assert callable(handler)
        handler(signal.SIGINT, None)
        assert stopper.stopped is True
        assert "stopping after the current event" in capsys.readouterr().err
    finally:
        signal.signal(signal.SIGINT, previous)


def test_a_second_signal_forces_the_issue() -> None:
    """Someone pressing Ctrl-C twice is no longer asking politely."""
    import signal

    from generator.__main__ import Stopper

    stopper = Stopper()
    previous = signal.getsignal(signal.SIGINT)
    try:
        stopper.install()
        handler = signal.getsignal(signal.SIGINT)
        assert callable(handler)
        handler(signal.SIGINT, None)
        with pytest.raises(KeyboardInterrupt):
            handler(signal.SIGINT, None)
    finally:
        signal.signal(signal.SIGINT, previous)


def test_an_interrupted_run_exits_with_the_conventional_code(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """130 is the shell convention for SIGINT; a traceback is not."""
    import generator.__main__ as cli

    def boom(*_args: object, **_kwargs: object) -> int:
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "run_seed", boom)
    assert cli.main(["--mode", "seed"]) == 130
    assert "interrupted" in capsys.readouterr().err
