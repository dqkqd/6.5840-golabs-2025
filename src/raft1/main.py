import concurrent.futures
import enum
import os
import shutil
import subprocess
from pathlib import Path
from signal import SIGINT
from typing import Annotated, LiteralString
from uuid import uuid4

import typer
from tqdm import tqdm

app = typer.Typer()


class Result(enum.Enum):
    Failed = enum.auto()
    Passed = enum.auto()


class CommandError(Exception): ...


def output_dir() -> Path:
    out = Path("output")
    out.mkdir(exist_ok=True)
    return out


class Command(str, enum.Enum):
    TestInitialElection3A = "TestInitialElection3A"
    TestReElection3A = "TestReElection3A"
    TestManyElections3A = "TestManyElections3A"
    TestBasicAgree3B = "TestBasicAgree3B"
    TestBackup3B = "TestBackup3B"
    TestFigure8Unreliable3C = "TestFigure8Unreliable3C"
    Test3A = "3A"
    Test3B = "3B"
    Test3C = "3C"

    def prepared_command(self, race: bool) -> LiteralString:
        command = f"go test -run {self.value}"
        if race:
            command += " --race"
        return command


def failed(s: str) -> bool:
    s = s.strip()
    if "FAIL" in s or "DATA RACE" in s or "warning:" in s:
        return True
    return False


def check_future(future: concurrent.futures.Future[None]):
    ex = future.exception(timeout=0.0)
    if ex is not None:
        if isinstance(ex, CommandError):
            print(ex)
            return
        else:
            raise ex


def run_command_fn(command: LiteralString) -> None:
    output_file = output_dir() / uuid4().hex
    _ = subprocess.run(command.split(" "), stdout=output_file.open("w"))

    with output_file.open() as f:
        has_error = any(map(failed, f))

    if has_error:
        raise CommandError(output_file.read_text())


def run_command_sequence(command: LiteralString) -> None:
    output = subprocess.run(command.split(" "))
    if output.returncode != 0:
        raise CommandError(f"failed {command}")


def run_command_sequence_timeout(command: LiteralString, timeout: int):
    output_file = output_dir() / uuid4().hex
    print(f"Output file: {output_file}")

    with subprocess.Popen(
        command.split(" "), stdout=output_file.open("w"), start_new_session=True
    ) as process:
        try:
            _ = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            # kill the old running process
            os.killpg(os.getpgid(process.pid), SIGINT)
            _ = process.wait()
            print(f"Command {command}, timeout after {timeout}s")

    with output_file.open() as f:
        has_error = any(map(failed, f))

    if has_error:
        # remove unused files
        files = list(output_dir().iterdir())
        for file in files:
            if file == output_file:
                continue
            file.unlink()
        raise CommandError(f"error:\n{output_file}")


@app.command()
def parallel(
    case: Annotated[Command, typer.Option(case_sensitive=False)],
    iterations: int = 5,
    verbose: bool = False,
    race: bool = False,
):
    shutil.rmtree(output_dir())
    if verbose:
        os.environ["DEBUG"] = "1"

    command = case.prepared_command(race)
    print(f"Running `{command}` with {iterations} iterations")
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = concurrent.futures.wait(
            (executor.submit(run_command_fn, command) for _ in range(iterations)),
            return_when=concurrent.futures.FIRST_EXCEPTION,
        )
        for f in futures.done:
            check_future(f)

    print("PASS")


@app.command()
def sequence(
    case: Annotated[Command, typer.Option(case_sensitive=False)],
    iterations: int = 5,
    verbose: bool = False,
    race: bool = False,
    timeout: int | None = None,
):
    shutil.rmtree(output_dir())
    if verbose:
        os.environ["DEBUG"] = "1"

    command = case.prepared_command(race)
    print(f"Running `{command}` with {iterations} iterations")

    for _ in tqdm(range(iterations)):
        if timeout is not None:
            run_command_sequence_timeout(command=command, timeout=timeout)
        else:
            run_command_sequence(command=command)

    print("PASS")


@app.command()
def extract_log(path: Path, pattern: str):
    new_output_file = path.with_suffix(".out")
    new_output_file.touch()

    with path.open("r") as src, new_output_file.open("w") as dst:
        for line in src:
            if pattern in line:
                _ = dst.write(line)


if __name__ == "__main__":
    app()
