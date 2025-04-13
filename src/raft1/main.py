import concurrent.futures
import subprocess

import typer

app = typer.Typer()


def failed(s: str) -> bool:
    for x in reversed(s.splitlines()):
        if x.strip() == "FAIL":
            return True
    return False


def run_command(command: str) -> None:
    output = subprocess.run(command.split(" "), capture_output=True)
    stdout = output.stdout.decode()
    if failed(stdout):
        raise ValueError(stdout)


@app.command()
def TestBasicAgree3B(loop: int):
    command = "go test -run TestBasicAgree3B --race"
    print(f"Running `{command}` with {loop} iterations")

    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = concurrent.futures.wait(
            (executor.submit(run_command, command) for _ in range(loop)),
            return_when=concurrent.futures.FIRST_EXCEPTION,
        )
        for f in futures.done:
            ex = f.exception(timeout=0.0)
            if ex is not None and isinstance(ex, ValueError):
                print(ex)
                return

    print("PASS")


if __name__ == "__main__":
    app()
