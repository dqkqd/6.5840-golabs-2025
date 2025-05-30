import concurrent.futures
import enum
import os
import shutil
import subprocess
from pathlib import Path
from signal import SIGINT
from typing import Annotated
from uuid import uuid4

import typer
from tqdm import tqdm

app = typer.Typer()


class ExecutionError(Exception): ...


@enum.unique
class RaftCommand(enum.StrEnum):
    Test3A = "3A"
    Test3B = "3B"
    Test3C = "3C"
    Test3D = "3D"
    TestBackup3B = "TestBackup3B"
    TestBasicAgree3B = "TestBasicAgree3B"
    TestFigure8Unreliable3C = "TestFigure8Unreliable3C"
    TestInitialElection3A = "TestInitialElection3A"
    TestManyElections3A = "TestManyElections3A"
    TestReElection3A = "TestReElection3A"
    TestSnapshotBasic3D = "TestSnapshotBasic3D"


@enum.unique
class RSMCommand(enum.StrEnum):
    Test4A = "4A"
    TestRestartReplay4A = "TestRestartReplay4A"
    TestRestartSubmit4A = "TestRestartSubmit4A"


@enum.unique
class KvRaftCommand(enum.StrEnum):
    Test4B = "4B"
    Test4C = "4C"
    TestSnapshotRecover4C = "TestSnapshotRecover4C"
    TestBasic4B = "TestBasic4B"
    TestConcurrent4B = "TestConcurrent4B"
    TestManyPartitionsManyClients4B = "TestManyPartitionsManyClients4B"
    TestManyPartitionsOneClient4B = "TestManyPartitionsOneClient4B"
    TestOnePartition4B = "TestOnePartition4B"
    TestSnapshotRecoverManyClients4C = "TestSnapshotRecoverManyClients4C"
    TestSnapshotUnreliable4C = "TestSnapshotUnreliable4C"
    TestSpeed4B = "TestSpeed4B"
    TestUnreliable4B = "TestUnreliable4B"


@enum.unique
class ShardKvCommand(enum.StrEnum):
    TestJoinLeaveBasic5A = "TestJoinLeaveBasic5A"


@app.command()
def raft(
    case: Annotated[RaftCommand, typer.Option(case_sensitive=False)] | None = None,
    iterations: int = 5,
    verbose: bool = False,
    race: bool = False,
    timeout: int = 600,
    parallel: bool = False,
):
    if verbose:
        os.environ["DEBUG"] = "1"

    execute(
        cwd=Path("src/raft1"),
        case=case,
        race=race,
        sequence=not parallel,
        iterations=iterations,
        timeout=timeout,
    )


@app.command()
def rsm(
    case: Annotated[RSMCommand, typer.Option(case_sensitive=False)] | None = None,
    iterations: int = 5,
    verbose: bool = False,
    race: bool = False,
    timeout: int = 600,
    parallel: bool = False,
):
    if verbose:
        os.environ["DEBUG"] = "1"
        os.environ["RSM_DEBUG"] = "1"

    execute(
        cwd=Path("src/kvraft1/rsm"),
        case=case,
        race=race,
        sequence=not parallel,
        iterations=iterations,
        timeout=timeout,
    )


@app.command()
def kvraft(
    case: Annotated[KvRaftCommand, typer.Option(case_sensitive=False)] | None = None,
    iterations: int = 5,
    verbose: bool = False,
    race: bool = False,
    timeout: int = 60,
    parallel: bool = False,
):
    if verbose:
        os.environ["DEBUG"] = "1"
        os.environ["RSM_DEBUG"] = "1"
        os.environ["KVRAFT_DEBUG"] = "1"

    execute(
        cwd=Path("src/kvraft1"),
        case=case,
        race=race,
        sequence=not parallel,
        iterations=iterations,
        timeout=timeout,
    )


@app.command()
def shardkv(
    case: Annotated[ShardKvCommand, typer.Option(case_sensitive=False)] | None = None,
    iterations: int = 5,
    verbose: bool = False,
    race: bool = False,
    timeout: int = 60,
    parallel: bool = False,
):
    if verbose:
        os.environ["DEBUG"] = "1"
        os.environ["RSM_DEBUG"] = "1"
        os.environ["KVRAFT_DEBUG"] = "1"
        os.environ["SHARDKV_DEBUG"] = "1"

    execute(
        cwd=Path("src/shardkv1"),
        case=case,
        race=race,
        sequence=not parallel,
        iterations=iterations,
        timeout=timeout,
    )


@app.command()
def run_all(
    iterations: int = 5,
    verbose: bool = False,
    race: bool = False,
    timeout: int = 600,
    parallel: bool = False,
):
    if verbose:
        os.environ["DEBUG"] = "1"
        os.environ["RSM_DEBUG"] = "1"
        os.environ["KVRAFT_DEBUG"] = "1"

    for cwd in [Path("src/raft1"), Path("src/kvraft1/rsm"), Path("src/kvraft1")]:
        execute(
            cwd=cwd,
            case=None,
            race=race,
            sequence=not parallel,
            iterations=iterations,
            timeout=timeout,
        )


@app.command()
def extract_log(path: Path, pattern: str):
    new_output_file = path.with_suffix(".out")
    new_output_file.touch()

    with path.open("r") as src, new_output_file.open("w") as dst:
        for line in src:
            if pattern in line:
                _ = dst.write(line)


def execute(
    cwd: Path,
    case: str | None,
    race: bool,
    sequence: bool,
    iterations: int,
    timeout: int = 100,
):
    shutil.rmtree(output_dir())

    command = "go test -v"
    if case is not None:
        command = f"{command} -run {case}"
    if race:
        command += " --race"

    print(
        f"Running `{command}` in {cwd} ({"sequence" if sequence else "parallel"}, iterations={iterations}, timeout={timeout}s)"
    )

    if sequence:
        for _ in tqdm(range(iterations)):
            run_command(cwd=cwd, command=command, timeout=timeout)
    else:
        with concurrent.futures.ProcessPoolExecutor() as executor:
            futures = concurrent.futures.wait(
                (
                    executor.submit(run_command, cwd, command, timeout)
                    for _ in range(iterations)
                ),
                return_when=concurrent.futures.FIRST_EXCEPTION,
            )
            for f in futures.done:
                ex = f.exception(timeout=0.0)
                if ex is not None:
                    if isinstance(ex, ExecutionError):
                        print(ex)
                        return
                    else:
                        raise ex


def run_command(cwd: Path, command: str, timeout: int):
    output_file = output_dir() / uuid4().hex
    with subprocess.Popen(
        command.split(" "),
        stdout=output_file.open("w"),
        start_new_session=True,
        cwd=cwd,
    ) as process:
        try:
            _ = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            # kill the old running process
            os.killpg(os.getpgid(process.pid), SIGINT)
            _ = process.wait()
            print(f"Command {command}, timeout after {timeout}s")

    failed_keys = ["FAIL", "DATA RACE", "warning:", "Fatal", "panic:"]
    with output_file.open() as f:
        for line in f:
            for k in failed_keys:
                if k in line:
                    raise ExecutionError(f"error, has: `{k}`:\n{output_file}")

    # remove those success
    output_file.unlink()


def output_dir() -> Path:
    out = Path("output")
    out.mkdir(exist_ok=True)
    return out


if __name__ == "__main__":
    app()
