from typing import Dict, Tuple, TypeVar, Optional
import re
import json
import textwrap
from pathlib import Path
from cryptography.hazmat.primitives import serialization
from snowflake.connector import DictCursor

from rich.live import Live
from rich.markup import escape
from rich.console import Console
from rich.prompt import Prompt, Confirm
from rich.panel import Panel
from rich.syntax import Syntax
from readchar import readkey, key
from .connectors.snowflake_connection import SnowflakeConn


def generate_config_interactive(config_path: Path) -> bool:
    """ Returns True if the config was generated successfully. """
    console = Console()

    console.print("Welcome to interactive config setup! This script will walk you through setting up your data buddy config file.")

    db_type = multiple_choice('What type of database do you want to connect?', {
        'snowflake': ('Snowflake', ''),
        'postgres': ('Postgres', ''),
    })

    if db_type == 'snowflake':
        account = Prompt.ask("[bold]Snowflake Account[/bold]")
        user = Prompt.ask("[bold]Snowflake User[/bold]")
        warehouse = Prompt.ask("[bold]Snowflake Warehouse[/bold] (optional)") or ''
        method = multiple_choice('Which login method do you use?', {
            'key': ('Key Pair', '(recommended)'),
            'pw': ('Password', ''),
        })
        pw = None
        private_key_file = None
        private_key_file_pwd = None

        if method == 'pw':
            pw = Prompt.ask("[bold]Password[/bold]", password=True)
        elif method == 'key':
            while True:
                private_key_file = Prompt.ask("[bold]Private Key File[/bold]", default="~/sf_private_key.p8")
                path = Path(private_key_file)
                if not path.is_file():
                    console.print(f"[red]{private_key_file} does not exist.[/red]")
                    continue

                # try validating with no password
                if not validate_key(path):
                    while True:
                        private_key_file_pwd = Prompt.ask("[bold]Private Key Passphrase[/bold]", password=True)
                        if not validate_key(path, password=private_key_file_pwd):
                            console.print(f"[red]Failed to decrypt key.[/red]")
                            continue
                        break
                break

        # Test Connection
        with console.status('Testing Snowflake Connection...') as s:
            conn = SnowflakeConn.get_conn(
                console,
                {
                    'private_key_file': str(private_key_file),
                    'private_key_file_pwd': private_key_file_pwd,
                    'password': pw or None,
                    'account': account,
                    'user': user,
                    'warehouse': warehouse or None,
                },
            )
            assert conn
            try:
                with conn.cursor(DictCursor) as cur:
                    cur.execute("SHOW DATABASES")
                    databases = [
                        row['name']
                        for row in cur.fetchall()
                        if row['name'] != 'SNOWFLAKE'
                    ]
            finally:
                conn.close()
            console.print('Connected Successfully!')

        which = multiple_choice('Which databases should be analyzed?', {
            'all': ('All', f'Analyze all databases: ({escape(", ".join(databases))})'),
            'select': ('Manual', 'Select which databases to analyze'),
        })
        if which == 'all':
            pass
        elif which == 'select':
            databases = [
                d
                for d in databases
                if Confirm.ask(f"Include [green]{escape(d)}[/green]?", default=True)
            ]

        config_yaml = re.sub(r'\n+', '\n', textwrap.dedent(f"""\
            per_run_timeout_seconds: 1800
            metrics_config:
              max_threads: 10
              per_query_timeout_seconds: 60
              per_column_timeout_seconds: 90
              per_database_timeout_seconds: 600
              exclude_expensive_queries: false
              exclude_complex_queries: false
            connections:
              default:
                type: snowflake
                account: {json.dumps(account)}
                user: {json.dumps(user)}
                {f'password: {json.dumps(pw)}' if pw else ''}
                {f'private_key_file: {json.dumps(private_key_file)}' if private_key_file else ''}
                {f'private_key_file_pwd: {json.dumps(private_key_file_pwd)}' if private_key_file_pwd else ''}
                {f'warehouse: {json.dumps(warehouse)}' if warehouse else ''}
                databases:
        """)).strip()
        for d in databases:
            config_yaml += (
                '\n'
                f'      {json.dumps(d)}:\n'
                f'        database: {json.dumps(d)}'
            )

        with open(config_path, 'w') as f:
            f.write(config_yaml)
        console.print(Panel(
            Syntax(config_yaml, 'yaml', line_numbers=False),
            title=f"Generated {str(config_path)} as:"
        ))
        console.print(f'Setup complete! If you need to make further changes, please modify {config_path} directly.')
        return True

    elif db_type == 'postgres':
        console.print("[red]Sorry, interactive Postgres setup isn't supported yet.[/red]")
        return False

    else:
        raise ValueError(f"{db_type} unsupported")

    return False


T = TypeVar('T')
def multiple_choice[T](prompt: str, choices: Dict[T, Tuple[str, str]]) -> T:
    options = [k for k in choices.keys()]
    if not options:
        raise ValueError('No choices provided.')
    selected_index = 0

    def pretty_options():
        out = [
            f'[bold]{prompt}[/bold]  [dim](Arrows to select, Enter to continue)[/dim]'
        ]
        for k, (short, detail) in choices.items():
            if k == options[selected_index]:
                out.append(f'❯   [blue underline]{escape(short)}[/blue underline]  [dim]{escape(detail)}[/dim]')
            else:
                out.append(f'    {escape(short)}')
        return "\n\r".join(out)

    with Live(pretty_options(), auto_refresh=False) as live:
        while True:
            live.update(pretty_options(), refresh=True)
            pressed = readkey()
            if pressed == key.ENTER:
                live.update(f'[bold]{prompt}[/bold]  {escape(choices[options[selected_index]][0])}', refresh=True)
                return options[selected_index]
            if pressed in (key.UP, key.LEFT, 'w', 'a'):
                selected_index = (selected_index - 1) % len(options)
            if pressed in (key.DOWN, key.RIGHT, 's', 'd'):
                selected_index = (selected_index + 1) % len(options)

def validate_key(key_path: Path, password: Optional[str] = None) -> bool:
    with key_path.open("rb") as f:
        key_data = f.read()
    try:
        # Try to load
        serialization.load_pem_private_key(
            key_data,
            password=password.encode() if password is not None else None,
        )
        return True
    except (TypeError, ValueError):
        # TypeError is raised if a password is required
        return False
    return False
